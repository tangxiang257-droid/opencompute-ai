"""Fresh-only local attempts: stage 0 by default, opt-in serial stage 1.

No public execution gate.

Trusted single-loop assembly and synchronous revocation-fenced contexts required.
SQLite calls block the loop (existing busy timeout 15s). No reopen/recovery/GC.
"""
import hashlib
import json
import math
import struct
from contextlib import contextmanager
from pydantic import TypeAdapter
from .contracts import Task,Session,State,Identifier,Stage
from .v2_journal import V2Journal
from .store import Conflict,canonical
from .tile_contract_v2 import TileV2,validate_prefill_plan,decode_input_payload
from .stage_ownership import StageBinding,StageOwner

MAX_BYTES=65536
MAX_RESERVED=262144

@contextmanager
def _non_suppressing(context):
    """An authorization provider cannot turn a failed body into success."""
    failure=None
    with context as principal:
        try:
            yield principal
        except BaseException as exc:
            failure=exc
            raise
    # Reached only when the provider suppressed the body's exception.
    if failure is not None:
        raise failure

class LocalAttemptJournal(V2Journal):
    @classmethod
    def create(cls,path,task,session,stages,*,authorize,execution_authorize,enable_stage1=False):
        if type(enable_stage1) is not bool:raise TypeError('explicit boolean required')
        if not callable(execution_authorize):raise TypeError('execution fence required')
        if not callable(authorize):raise TypeError('owner fence required')
        def owner_fence():return _non_suppressing(authorize())
        obj=super().create(path,task,session,stages,authorize=owner_fence)
        obj._execution=execution_authorize
        obj._stage1_enabled=enable_stage1
        try:
            with _non_suppressing(obj._authorized()),obj._store._tx() as c:
                c.execute('''CREATE TABLE p1_local_attempts(
                    tile INTEGER PRIMARY KEY, plan TEXT NOT NULL, instance TEXT NOT NULL,
                    input BLOB NOT NULL, output BLOB, reserved INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('ADMITTED','OUTPUT_COMMITTED')),
                    CHECK(typeof(input)='blob' AND length(input) BETWEEN 1 AND 65536),
                    CHECK(output IS NULL OR (typeof(output)='blob' AND length(output) BETWEEN 1 AND 65536)),
                    CHECK((state='ADMITTED' AND output IS NULL) OR (state='OUTPUT_COMMITTED' AND output IS NOT NULL)))''')
                if enable_stage1:
                    sql=c.execute("SELECT sql FROM sqlite_master WHERE name='p1_local_attempts'").fetchone()[0]
                    c.execute(sql.replace('p1_local_attempts','p1_local_stage1_attempts',1))
        except BaseException:
            obj.close();raise
        return obj

    def _plan(self,c,owner,*,active,stage_id=0):
        t=self._store._load(c,'p1_tasks',self._task_id,Task)
        s=self._store._load(c,'p1_sessions',self._session_id,Session)
        if t.owner_id!=owner or s.owner_id!=owner:raise PermissionError('owner mismatch')
        if active and (t.state!=State.PREPARING or s.state!=State.PREPARING):raise Conflict('inactive')
        r=c.execute('SELECT body,digest FROM p1_v2_plans WHERE session_id=? AND epoch=?',(s.session_id,s.epoch)).fetchone()
        if r is None:raise Conflict('missing current plan')
        p=json.loads(r[0])
        if hashlib.sha256(canonical(['register_v2',p]).encode()).hexdigest()!=r[1]:raise Conflict('plan digest')
        tiles=tuple(TileV2.model_validate_json(canonical(x)) for x in p['tiles'])
        stages=tuple(Stage.model_validate_json(x[0]) for x in c.execute('SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(s.session_id,)))
        validate_prefill_plan(t,s,stages,tiles,hidden_size=p['hidden_size'],vocab_size=p['vocab_size'])
        return s,r[1],tuple(x for x in tiles if x.stage_id==stage_id),p['vocab_size']

    def _used(self,c):
        used=c.execute('SELECT coalesce(sum(reserved),0) FROM p1_local_attempts').fetchone()[0]
        if self._stage1_enabled:
            used+=c.execute('SELECT coalesce(sum(reserved),0) FROM p1_local_stage1_attempts').fetchone()[0]
        return used

    def _sources(self,c,owner):
        session,plan,upstream,vocab=self._plan(c,owner,active=True)
        _,down_plan,downstream,_=self._plan(c,owner,active=True,stage_id=1)
        if plan!=down_plan or len(upstream)!=len(downstream):raise Conflict('source plan mismatch')
        rows=c.execute('SELECT tile,plan,output,state FROM p1_local_attempts ORDER BY tile').fetchall()
        if len(rows)!=len(upstream):raise Conflict('complete upstream required')
        values=[]
        for row,src,dst in zip(rows,upstream,downstream):
            tid,payload_plan,value,state=row
            if (tid!=src.tile_id or payload_plan!=plan or state!='OUTPUT_COMMITTED' or
                src.output!=dst.input or src.token_start!=dst.token_start or src.token_count!=dst.token_count):
                raise Conflict('upstream mismatch')
            decode_input_payload(dst,value,layout_version=2,vocab_size=vocab)
            expected=canonical(['local_output',plan,tid,hashlib.sha256(value).hexdigest()])
            event=c.execute('SELECT payload FROM p1_events WHERE id=?',('local:output:'+str(tid),)).fetchone()
            if event!=(expected,):raise Conflict('source output event mismatch')
            values.append(value)
        return session,plan,downstream,tuple(values)

    def make_stage1_owner(self,operation,*,instance_id):
        if not self._stage1_enabled:raise Conflict('stage 1 disabled')
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            session,plan,tiles,inputs=self._sources(c,owner)
        return self._make_owner(operation,inputs,instance_id=instance_id,stage_id=1,
                                source_snapshot=(session.epoch,plan,inputs))

    def make_owner(self,operation,inputs,*,instance_id):
        return self._make_owner(operation,inputs,instance_id=instance_id,stage_id=0)

    def _make_owner(self,operation,inputs,*,instance_id,stage_id,source_snapshot=None):
        table='p1_local_attempts' if stage_id==0 else 'p1_local_stage1_attempts'
        event_prefix='local:' if stage_id==0 else 'local1:'
        TypeAdapter(Identifier).validate_python(instance_id,strict=True)
        if type(inputs) is not tuple or not 1<=len(inputs)<=64:raise Conflict('bounded fixed inputs required')
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            s,plan,tiles,vocab=self._plan(c,owner,active=True,stage_id=stage_id)
        if len(inputs)!=len(tiles):raise Conflict('complete stage inputs required')
        reserved=0
        for tile,value in zip(tiles,inputs):
            decode_input_payload(tile,value,layout_version=2,vocab_size=vocab)
            size=4
            for d in tile.output.shape:
                if size>MAX_BYTES//d:raise Conflict('output budget')
                size*=d
            reserved+=len(value)+size
        if reserved>MAX_RESERVED:raise Conflict('retained bytes budget')
        binding=StageBinding(s.task_id,s.session_id,s.epoch,stage_id,s.model_hash,s.layout_hash)
        fixed={x.token_start:(x,value) for x,value in zip(tiles,inputs)}
        def check(spec):
            item=fixed.get(spec.token_start)
            if item is None:raise Conflict('unknown range')
            tile,value=item
            if (spec.binding!=binding or spec.token_count!=tile.token_count or
                spec.input_digest!=hashlib.sha256(value).hexdigest()):raise Conflict('fixed input mismatch')
            return tile,value
        @contextmanager
        def transaction(spec):
            with _non_suppressing(self._authorized()) as owner,_non_suppressing(self._execution(spec)) as executor,self._store._tx() as c:
                if executor!=owner or type(executor) is not str:raise PermissionError('execution principal')
                current,digest,unused,unused_vocab=self._plan(c,owner,active=True)
                if digest!=plan or current.epoch!=binding.epoch:raise Conflict('stale plan')
                if stage_id==1:
                    current,source_plan,source_tiles,values=self._sources(c,owner)
                    if (current.epoch,source_plan,values)!=source_snapshot:
                        raise Conflict('source changed after binding')
                yield c
        def admit(spec):
            tile,value=check(spec)
            with transaction(spec) as c:
                if c.execute(f'SELECT 1 FROM {table} WHERE tile=?',(tile.tile_id,)).fetchone():
                    raise Conflict('attempt already exists; query only')
                if c.execute(f'SELECT 1 FROM {table} WHERE instance<>? LIMIT 1',(instance_id,)).fetchone():
                    raise Conflict('different instance')
                count=c.execute(f"SELECT count(*) FROM {table} WHERE state='OUTPUT_COMMITTED'").fetchone()[0]
                if count!=tile.tile_id:raise Conflict('prefix not committed')
                size=4*math.prod(tile.output.shape)
                used=self._used(c)
                if used+len(value)+size>MAX_RESERVED:raise Conflict('retained bytes budget')
                c.execute(f"INSERT INTO {table} VALUES(?,?,?,?,NULL,?,'ADMITTED')",(tile.tile_id,plan,instance_id,value,len(value)+size))
                self._record_event(c,event_prefix+'admit:'+str(tile.tile_id),canonical(['local_admit',plan,instance_id,tile.tile_id,spec.input_digest]),{'execution_allowed':False})
            return True
        def commit(spec,result):
            tile,value=check(spec)
            if type(result) is not bytes or len(result)!=4*math.prod(tile.output.shape) or len(result)>MAX_BYTES:
                raise Conflict('output bytes shape')
            if any(not math.isfinite(x[0]) for x in struct.iter_unpack('<f',result)):raise Conflict('nonfinite output')
            with transaction(spec) as c:
                row=c.execute(f'SELECT plan,instance,input,state FROM {table} WHERE tile=?',(tile.tile_id,)).fetchone()
                if row!=(plan,instance_id,value,'ADMITTED'):raise Conflict('attempt mismatch')
                c.execute(f"UPDATE {table} SET output=?,state='OUTPUT_COMMITTED' WHERE tile=?",(result,tile.tile_id))
                self._record_event(c,event_prefix+'output:'+str(tile.tile_id),canonical(['local_output',plan,tile.tile_id,hashlib.sha256(result).hexdigest()]),{'execution_allowed':False})
        return StageOwner(binding,operation,authorize=admit,commit=commit)

    def query(self,tile_id,*,stage_id=0):
        if type(stage_id) is not int or stage_id not in (0,1):raise Conflict('stage id')
        if stage_id==1 and not self._stage1_enabled:raise Conflict('stage 1 disabled')
        table='p1_local_attempts' if stage_id==0 else 'p1_local_stage1_attempts'
        if type(tile_id) is not int or not 0<=tile_id<64:raise Conflict('tile id')
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            # Historical query remains authorized but does not re-grant execution.
            t=self._store._load(c,'p1_tasks',self._task_id,Task)
            if owner!=t.owner_id:raise PermissionError('owner mismatch')
            row=c.execute(f'SELECT state,output FROM {table} WHERE tile=?',(tile_id,)).fetchone()
            return None if row is None else {'state':row[0],'output':row[1],'execution_allowed':False,'retry_safe':False}
