"""Private one-shot trusted CPU prefill assembly; no import-time model work.

The real factory loads synchronously and cannot be interrupted mid-load. Loading
may retain full weight snapshots and both stages; byte-journal limits are NOT
weight/KV/RSS limits. This module introduces no public/default execution route.
"""
import asyncio
from dataclasses import dataclass,replace
from pydantic import TypeAdapter
from .contracts import Task,Stage,Digest,Identifier
from .local_attempt import _non_suppressing
from .local_wave import WaveAttemptJournal,run_waves
from .stage_ownership import _PlannedOperation,StageBinding
from .store import Conflict
from .tile_contract_v2 import decode_input_payload

@dataclass(frozen=True,slots=True)
class _Preparation:
    phase:str
    root:str
    weight_sha256:str
    config_sha256:str
    plan_digest:str
    task_id:str
    session_id:str
    epoch:int
    stage0_instance:str
    stage1_instance:str

@dataclass(frozen=True,slots=True)
class _Snapshot:
    task:object
    session:object
    stages:tuple
    tiles:tuple
    digest:str
    vocab:int

def _snapshot(journal):
    with _non_suppressing(journal._authorized()) as owner,journal._store._tx() as c:
        session,digest,a,vocab=journal._plan(c,owner,active=True)
        second,other,b,_=journal._plan(c,owner,active=True,stage_id=1)
        if second!=session or other!=digest:raise Conflict('inconsistent preparation plan')
        if journal._used(c)!=0 or any(c.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone()
            for table in ('p1_local_attempts','p1_local_stage1_attempts')):
            raise Conflict('existing attempts cannot resume')
        task=journal._store._load(c,'p1_tasks',journal._task_id,Task)
        stages=tuple(Stage.model_validate_json(x[0]) for x in c.execute(
            'SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(session.session_id,)))
        return _Snapshot(task,session,stages,tuple(sorted((*a,*b),key=lambda t:(t.tile_id,t.stage_id))),digest,vocab)

def _current(journal,snapshot):
    if _snapshot(journal)!=snapshot:raise Conflict('preparation plan changed')

def _cpu_factory(*args,**kwargs):
    # Deliberately lazy: importing the assembly does not import Torch/read weights.
    from compute.qwen2_tiled_stage import _owned_cpu_prefill_bytes
    return _owned_cpu_prefill_bytes(*args,**kwargs)

def _check_operation(operation,snapshot,stage):
    if type(operation)is not _PlannedOperation:raise TypeError('exact planned CPU operation required')
    s=snapshot.session
    binding=StageBinding(s.task_id,s.session_id,s.epoch,stage,s.model_hash,s.layout_hash)
    ranges=tuple((t.token_start,t.token_count) for t in snapshot.tiles if t.stage_id==stage)
    if operation._binding!=binding or operation._ranges!=ranges:raise Conflict('loaded operation plan mismatch')

async def _run_cpu_prefill_waves(journal,root,*,expected_weight_sha256,expected_config_sha256,
        token_inputs,stage0_instance,stage1_instance,prepare_authorize):
    """Trusted local entrypoint, requiring separate load/postload preparation authorization.

    prepare_authorize(spec) must synchronously authenticate and fence preparation
    until exit, yielding the owner principal. It does not replace each call's
    execution_authorize fence. No await/SQLite transaction spans synchronous load.
    Cancellation checkpoints occur BEFORE loading, BETWEEN stages and BEFORE
    dispatch, not inside a loader. Exceptions may retain private stage references.
    No immediate unload, reset, retry or recovery is promised.
    """
    if type(journal)is not WaveAttemptJournal:raise TypeError('explicit wave journal required')
    if type(root)is not str or not root or len(root)>4096:raise ValueError('trusted bounded root string required')
    if not callable(prepare_authorize):raise TypeError('preparation fence required')
    for pin in (expected_weight_sha256,expected_config_sha256):TypeAdapter(Digest).validate_python(pin,strict=True)
    for identity in (stage0_instance,stage1_instance):TypeAdapter(Identifier).validate_python(identity,strict=True)
    snapshot=_snapshot(journal)
    if expected_weight_sha256!=snapshot.task.model_hash or expected_weight_sha256!=snapshot.session.model_hash:
        raise Conflict('trusted weight pin differs from plan')
    upstream=tuple(t for t in snapshot.tiles if t.stage_id==0)
    if type(token_inputs)is not tuple or len(token_inputs)!=len(upstream):raise ValueError('complete immutable token inputs required')
    for tile,value in zip(upstream,token_inputs):decode_input_payload(tile,value,layout_version=2,vocab_size=snapshot.vocab)
    journal.preflight_wave_budget();_current(journal,snapshot)
    spec=_Preparation('load0',root,expected_weight_sha256,expected_config_sha256,snapshot.digest,
        snapshot.task.task_id,snapshot.session.session_id,snapshot.session.epoch,stage0_instance,stage1_instance)
    operations=[]
    # These checkpoints deliver pending cancellation before entering the next
    # synchronous action. They do not make cold load cancellable.
    await asyncio.sleep(0)
    for stage in (0,1):
        with _non_suppressing(prepare_authorize(replace(spec,phase='load'+str(stage)))) as principal:
            if type(principal)is not str or principal!=snapshot.task.owner_id:raise PermissionError('preparation principal')
            _current(journal,snapshot)
            operation=_cpu_factory(root,expected_sha256=expected_weight_sha256,
                expected_config_sha256=expected_config_sha256,task=snapshot.task,
                session=snapshot.session,stages=snapshot.stages,tiles=snapshot.tiles,stage_id=stage)
            _check_operation(operation,snapshot,stage);_current(journal,snapshot)
            operations.append(operation)
        await asyncio.sleep(0)
    with _non_suppressing(prepare_authorize(replace(spec,phase='postload'))) as principal:
        if type(principal)is not str or principal!=snapshot.task.owner_id:raise PermissionError('preparation principal')
        _current(journal,snapshot)
    await asyncio.sleep(0)
    # Preserve exact-type identity: no generic callable wrapping of these values.
    await run_waves(journal,operations[0],operations[1],token_inputs,
                    stage0_instance=stage0_instance,stage1_instance=stage1_instance)
