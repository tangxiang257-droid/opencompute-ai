"""Fresh-only local stage-0 attempt experiment; no public execution gate.

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
    def create(cls,path,task,session,stages,*,authorize,execution_authorize):
        if not callable(execution_authorize):raise TypeError('execution fence required')
        if not callable(authorize):raise TypeError('owner fence required')
        def owner_fence():return _non_suppressing(authorize())
        obj=super().create(path,task,session,stages,authorize=owner_fence)
        obj._execution=execution_authorize
        try:
            with _non_suppressing(obj._authorized()),obj._store._tx() as c:
                c.execute('''CREATE TABLE p1_local_attempts(
                    tile INTEGER PRIMARY KEY, plan TEXT NOT NULL, instance TEXT NOT NULL,
                    input BLOB NOT NULL, output BLOB, reserved INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('ADMITTED','OUTPUT_COMMITTED')),
                    CHECK(typeof(input)='blob' AND length(input) BETWEEN 1 AND 65536),
                    CHECK(output IS NULL OR (typeof(output)='blob' AND length(output) BETWEEN 1 AND 65536)),
                    CHECK((state='ADMITTED' AND output IS NULL) OR (state='OUTPUT_COMMITTED' AND output IS NOT NULL)))''')
        except BaseException:
            obj.close();raise
        return obj

    def _plan(self,c,owner,*,active):
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
        return s,r[1],tuple(x for x in tiles if x.stage_id==0),p['vocab_size']

    def make_owner(self,operation,inputs,*,instance_id):
        TypeAdapter(Identifier).validate_python(instance_id,strict=True)
        if type(inputs) is not tuple or not 1<=len(inputs)<=64:raise Conflict('bounded fixed inputs required')
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            s,plan,tiles,vocab=self._plan(c,owner,active=True)
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
        binding=StageBinding(s.task_id,s.session_id,s.epoch,0,s.model_hash,s.layout_hash)
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
                yield c
        def admit(spec):
            tile,value=check(spec)
            with transaction(spec) as c:
                if c.execute('SELECT 1 FROM p1_local_attempts WHERE tile=?',(tile.tile_id,)).fetchone():
                    raise Conflict('attempt already exists; query only')
                if c.execute('SELECT 1 FROM p1_local_attempts WHERE instance<>? LIMIT 1',(instance_id,)).fetchone():
                    raise Conflict('different instance')
                count=c.execute("SELECT count(*) FROM p1_local_attempts WHERE state='OUTPUT_COMMITTED'").fetchone()[0]
                if count!=tile.tile_id:raise Conflict('prefix not committed')
                size=4*math.prod(tile.output.shape)
                used=c.execute('SELECT coalesce(sum(reserved),0) FROM p1_local_attempts').fetchone()[0]
                if used+len(value)+size>MAX_RESERVED:raise Conflict('retained bytes budget')
                c.execute("INSERT INTO p1_local_attempts VALUES(?,?,?,?,NULL,?,'ADMITTED')",(tile.tile_id,plan,instance_id,value,len(value)+size))
                self._record_event(c,'local:admit:'+str(tile.tile_id),canonical(['local_admit',plan,instance_id,tile.tile_id,spec.input_digest]),{'execution_allowed':False})
            return True
        def commit(spec,result):
            tile,value=check(spec)
            if type(result) is not bytes or len(result)!=4*math.prod(tile.output.shape) or len(result)>MAX_BYTES:
                raise Conflict('output bytes shape')
            if any(not math.isfinite(x[0]) for x in struct.iter_unpack('<f',result)):raise Conflict('nonfinite output')
            with transaction(spec) as c:
                row=c.execute('SELECT plan,instance,input,state FROM p1_local_attempts WHERE tile=?',(tile.tile_id,)).fetchone()
                if row!=(plan,instance_id,value,'ADMITTED'):raise Conflict('attempt mismatch')
                c.execute("UPDATE p1_local_attempts SET output=?,state='OUTPUT_COMMITTED' WHERE tile=?",(result,tile.tile_id))
                self._record_event(c,'local:output:'+str(tile.tile_id),canonical(['local_output',plan,tile.tile_id,hashlib.sha256(result).hexdigest()]),{'execution_allowed':False})
        return StageOwner(binding,operation,authorize=admit,commit=commit)

    def query(self,tile_id):
        if type(tile_id) is not int or not 0<=tile_id<64:raise Conflict('tile id')
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            # Historical query remains authorized but does not re-grant execution.
            t=self._store._load(c,'p1_tasks',self._task_id,Task)
            if owner!=t.owner_id:raise PermissionError('owner mismatch')
            row=c.execute('SELECT state,output FROM p1_local_attempts WHERE tile=?',(tile_id,)).fetchone()
            return None if row is None else {'state':row[0],'output':row[1],'execution_allowed':False,'retry_safe':False}
