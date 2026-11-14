"""Explicit fresh-only local ordinary-byte prefill waves; no model or public entrypoint.

Single owning loop, trusted same-UID storage and external revocation fences.
Known-token causal prefill only, not autoregressive decode. This adds
conservative diagonal-wave overlap, not work-conserving scheduling.
"""
import asyncio
import hashlib
import math
import struct
from contextlib import contextmanager
from pydantic import TypeAdapter
from .contracts import Identifier
from .local_attempt import LocalAttemptJournal, _non_suppressing, MAX_BYTES, MAX_RESERVED
from .stage_ownership import StageBinding, StageOwner
from .store import Conflict, canonical
from .tile_contract_v2 import decode_input_payload

class SourceNotReady(Conflict):
    """No execution/admission attempted; scheduler may wait for upstream commit."""

class WaveAttemptJournal(LocalAttemptJournal):
    @classmethod
    def create(cls,path,task,session,stages,*,authorize,execution_authorize,enable_wave):
        if enable_wave is not True:raise ValueError('explicit wave opt-in required')
        return super().create(path,task,session,stages,authorize=authorize,
                              execution_authorize=execution_authorize,enable_stage1=True)

    def _source(self,c,owner,tile):
        session,plan,tiles,vocab=self._plan(c,owner,active=True,stage_id=1)
        if tiles[tile.tile_id]!=tile:raise Conflict('source plan changed')
        # Earlier KV dependencies must have committed, not merely been admitted.
        prefix=c.execute('SELECT tile,plan,state FROM p1_local_attempts WHERE tile<=? ORDER BY tile',
                         (tile.tile_id,)).fetchall()
        if len(prefix)!=tile.tile_id+1 or any(r!=(k,plan,'OUTPUT_COMMITTED') for k,r in enumerate(prefix)):
            raise SourceNotReady('upstream prefix not committed')
        value=c.execute('SELECT output FROM p1_local_attempts WHERE tile=?',(tile.tile_id,)).fetchone()[0]
        decode_input_payload(tile,value,layout_version=2,vocab_size=vocab)
        expected=canonical(['local_output',plan,tile.tile_id,hashlib.sha256(value).hexdigest()])
        if c.execute('SELECT payload FROM p1_events WHERE id=?',('local:output:'+str(tile.tile_id),)).fetchone()!=(expected,):
            raise Conflict('source event mismatch')
        return session,plan,value

    def make_wave_stage1_owner(self,operation,*,instance_id):
        self.preflight_wave_budget()
        return _WaveStage1(self,operation,instance_id)

    def _payload_budget(self,c,owner,wire_layout_version):
        from .contracts import Task, Stage
        from .payload_budget import assess_payload_budget
        session,_,a,vocab=self._plan(c,owner,active=True)
        _,_,b,_=self._plan(c,owner,active=True,stage_id=1)
        task=self._store._load(c,'p1_tasks',self._task_id,Task)
        stages=tuple(Stage.model_validate_json(x[0]) for x in c.execute(
            'SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',
            (session.session_id,)))
        tiles=tuple(t for pair in zip(a,b) for t in pair)
        report=assess_payload_budget(task,session,stages,tiles,
            hidden_size=a[0].output.shape[2],vocab_size=vocab,
            wire_layout_version=wire_layout_version)
        return a,report

    def preflight_wave_budget(self):
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            a,report=self._payload_budget(c,owner,2)
            report.require_local(max_bytes=MAX_BYTES,max_reserved=MAX_RESERVED)
            return a

    def preflight_handoff_budget(self,*,wire_layout_version):
        """Read-only current-plan check; not permission, credit or reservation."""
        with _non_suppressing(self._authorized()) as owner,self._store._tx() as c:
            _,report=self._payload_budget(c,owner,wire_layout_version)
            report.require_local(max_bytes=MAX_BYTES,max_reserved=MAX_RESERVED)
            report.require_handoff()
            return report

class _WaveStage1:
    def __init__(self,journal,operation,instance_id):
        TypeAdapter(Identifier).validate_python(instance_id,strict=True)
        self._j=journal;self._current=None;self._busy=False
        with _non_suppressing(journal._authorized()) as owner,journal._store._tx() as c:
            session,plan,tiles,_=journal._plan(c,owner,active=True,stage_id=1)
        self._plan=plan;self._tiles=tiles
        binding=StageBinding(session.task_id,session.session_id,session.epoch,1,session.model_hash,session.layout_hash)
        def fixed(spec):
            if self._current is None:raise Conflict('no fixed source')
            tile,value=self._current
            if (spec.binding!=binding or spec.token_start!=tile.token_start or spec.token_count!=tile.token_count
                or spec.input_digest!=hashlib.sha256(value).hexdigest()):raise Conflict('fixed source mismatch')
            return tile,value
        @contextmanager
        def transaction(spec):
            tile,value=fixed(spec)
            with _non_suppressing(journal._authorized()) as owner,_non_suppressing(journal._execution(spec)) as executor,journal._store._tx() as c:
                if type(executor)is not str or executor!=owner:raise PermissionError('execution principal')
                current,digest,actual=journal._source(c,owner,tile)
                if current.epoch!=binding.epoch or digest!=plan or actual!=value:raise Conflict('source binding changed')
                yield c,tile,value
        def admit(spec):
            with transaction(spec) as (c,tile,value):
                if c.execute('SELECT 1 FROM p1_local_stage1_attempts WHERE tile=?',(tile.tile_id,)).fetchone():
                    raise Conflict('attempt already exists; no retry')
                if c.execute('SELECT 1 FROM p1_local_stage1_attempts WHERE instance<>? LIMIT 1',(instance_id,)).fetchone():
                    raise Conflict('different instance')
                count=c.execute("SELECT count(*) FROM p1_local_stage1_attempts WHERE state='OUTPUT_COMMITTED'").fetchone()[0]
                if count!=tile.tile_id:raise Conflict('downstream prefix not committed')
                reserved=len(value)+4*math.prod(tile.output.shape)
                if journal._used(c)+reserved>MAX_RESERVED:raise Conflict('shared retained budget')
                c.execute("INSERT INTO p1_local_stage1_attempts VALUES(?,?,?,?,NULL,?,'ADMITTED')",
                          (tile.tile_id,plan,instance_id,value,reserved))
                journal._record_event(c,'local1:admit:'+str(tile.tile_id),canonical(['local_admit',plan,instance_id,tile.tile_id,spec.input_digest]),{'execution_allowed':False})
            return True
        def commit(spec,result):
            tile,value=fixed(spec)
            if type(result)is not bytes or len(result)!=4*math.prod(tile.output.shape) or len(result)>MAX_BYTES:
                raise Conflict('output bytes shape')
            if any(not math.isfinite(x[0]) for x in struct.iter_unpack('<f',result)):raise Conflict('nonfinite output')
            with transaction(spec) as (c,tile,value):
                row=c.execute('SELECT plan,instance,input,state FROM p1_local_stage1_attempts WHERE tile=?',(tile.tile_id,)).fetchone()
                if row!=(plan,instance_id,value,'ADMITTED'):raise Conflict('attempt mismatch')
                c.execute("UPDATE p1_local_stage1_attempts SET output=?,state='OUTPUT_COMMITTED' WHERE tile=?",(result,tile.tile_id))
                journal._record_event(c,'local1:output:'+str(tile.tile_id),canonical(['local_output',plan,tile.tile_id,hashlib.sha256(result).hexdigest()]),{'execution_allowed':False})
        self._owner=StageOwner(binding,operation,authorize=admit,commit=commit)

    @property
    def state(self):return self._owner.state

    async def call_next(self):
        self._owner._loop_check()
        if self._busy or self.state.status!='idle':raise Conflict('wave owner unavailable')
        self._busy=True
        try:
            tile=next((t for t in self._tiles if t.token_start==self.state.next_token),None)
            if tile is None:raise Conflict('wave complete')
            try:
                with _non_suppressing(self._j._authorized()) as owner,self._j._store._tx() as c:
                    session,plan,value=self._j._source(c,owner,tile)
                    if session.epoch!=tile.epoch or plan!=self._plan:raise Conflict('stale wave plan')
            except SourceNotReady:
                raise
            except BaseException:
                await self._owner.close();raise
            self._current=(tile,value)
            return await self._owner.call(value,token_start=tile.token_start,token_count=tile.token_count)
        finally:
            self._current=None;self._busy=False

    async def close(self):await self._owner.close()

async def _close_all(owners):
    pending=asyncio.gather(*(o.close() for o in owners),return_exceptions=True)
    interrupted=None
    while not pending.done():
        try:await asyncio.shield(pending)
        except asyncio.CancelledError as exc:interrupted=exc
    failures=[x for x in pending.result() if isinstance(x,BaseException)]
    if interrupted is not None:failures.insert(0,interrupted)
    if failures:raise BaseExceptionGroup('wave close failures',failures)

async def run_waves(journal,stage0_operation,stage1_operation,inputs,*,stage0_instance,stage1_instance):
    """Known-token prefill only; errors cancel the wave and drain before return.

    Autoregressive decode depends on downstream-generated next tokens and must
    not use this schedule. At most two owned calls are active.

    Returns no copied output collection: query the authorized journal afterward.
    """
    if type(journal)is not WaveAttemptJournal:raise TypeError('explicit wave journal required')
    tiles=journal.preflight_wave_budget();owners=[];failure=None
    try:
        a=journal.make_owner(stage0_operation,inputs,instance_id=stage0_instance);owners.append(a)
        b=journal.make_wave_stage1_owner(stage1_operation,instance_id=stage1_instance);owners.append(b)
        async def upstream(k):
            t=tiles[k];await a.call(inputs[k],token_start=t.token_start,token_count=t.token_count)
        await upstream(0)
        for k in range(len(tiles)-1):
            async with asyncio.TaskGroup() as group:
                group.create_task(upstream(k+1));group.create_task(b.call_next())
        await b.call_next()
    except BaseException as exc:failure=exc
    try:await _close_all(owners)
    except BaseException as cleanup:
        if failure is not None:raise BaseExceptionGroup('wave and cleanup failures',[failure,cleanup])
        raise
    if failure is not None:raise failure
