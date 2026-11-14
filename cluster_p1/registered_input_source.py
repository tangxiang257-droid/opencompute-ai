"""Current v12 registered-input provenance, NOT task execution authorization.

The real reader checks STORED metadata and actual snapshot bytes. Tokenizer/model
status remains metadata-only/unverified. This adapter never binds a v12 plan,
loads a model, supplies issuer authorization/resources, or changes execution gates.
No production LocalUserFence is provided: trusted bootstrap must use the SAME
USER verifier as ApplicationService. Trusted same-process Python is the boundary.
"""
import asyncio
import inspect
import threading
from input_declaration import InputDeclarationReader
from private_source_bridge import LocalUserFence
from .contracts import Task, Session, Stage
from .online_issuance import IssuanceAuthority, _make_declaration
from .store import canonical
from .local_attempt import _non_suppressing
import hashlib


class SourceRejected(ValueError):
    def __init__(self):super().__init__('registered input source unavailable')


def _need(value):
    if not value:raise SourceRejected()


class RegisteredInputSource:
    """One source-register attempt; check is valid only on its synchronous stack.

    Constructor does not authorize anything. The async method owns Authority's
    lock before entering the synchronous USER fence (A -> F -> issuer/SQLite).
    Source digest checks are consumed once; no portable proof or history cache.
    hidden_size/layer counts remain plan declarations, not inspected model facts.
    P1 payload_ref stays inert metadata; it is never read as a path or authority.
    """
    def __init__(self,reader,*,identity_fence=None):
        _need(type(reader)is InputDeclarationReader and isinstance(identity_fence,LocalUserFence))
        _need(reader._service is not None and reader._service._verifier is identity_fence)
        self._reader=reader;self._service=reader._service;self._authority=reader._authority
        self._provider=identity_fence;self._loop=asyncio.get_running_loop()
        self._pins=(reader._model,reader._model_record_digest,reader._preprocessing_version,reader._contract_sha256)
        self._active=None;self._spent=False
    def _current(self):
        _need(asyncio.get_running_loop() is self._loop)
        _need(self._reader._service is self._service and self._reader._authority is self._authority
              and self._service._authority is self._authority and self._service._verifier is self._provider)
        _need((self._reader._model,self._reader._model_record_digest,self._reader._preprocessing_version,self._reader._contract_sha256)==self._pins)
        self._authority._check()
    def _owner(self,credential,expected):
        self._current();_need(self._service._download_owner(credential)==expected)
    def check(self,namespace,source_id,owner,task_id,declaration_digest,source_binding):
        """Issuer leaf callback; no lock acquisition, awaits, permit or side store."""
        self._current();active=self._active
        _need(active is not None and active['task'] is asyncio.current_task()
              and active['thread']==threading.get_ident() and self._authority._lock.locked()
              and not active['consumed'])
        _need((namespace,source_id,owner,task_id,declaration_digest)==active['expected'])
        _need(type(source_binding)is dict and canonical(source_binding)==active['binding'])
        self._owner(active['credential'],owner)
        _need(self._reader._read_locked(active['credential'],task_id,owner)==active['source'])
        active['consumed']=True
        return None
    async def register(self,issuer,credential,task,session,stages,tiles,*,hidden_size,vocab_size,config_sha256):
        _need(type(issuer)is IssuanceAuthority and issuer.source_provider is self)
        _need(not self._spent and self._active is None);self._current();issuer.ready()
        # Freeze/validate the complete proposed plan before the only lock await.
        d=_make_declaration(task,session,stages,tiles,hidden_size,vocab_size,config_sha256)
        owner=d['task']['owner_id'];task_id=d['task']['task_id'];self._owner(credential,owner)
        attempted=False
        try:
            async with self._authority._lock:
                _need(not self._spent and self._active is None and issuer.source_provider is self);self._owner(credential,owner)
                context=self._provider.hold(credential)
                if inspect.isawaitable(context):
                    if inspect.iscoroutine(context) and inspect.getcoroutinestate(context)==inspect.CORO_CREATED:context.close()
                    raise SourceRejected()
                with _non_suppressing(context):
                    self._owner(credential,owner)
                    source=self._reader._read_locked(credential,task_id,owner)
                    _need((source.task_id,source.owner_id,source.model.weights_sha256,source.model.config_sha256,
                           source.vocab_size,source.token_count)==
                          (task_id,owner,d['task']['model_hash'],config_sha256,vocab_size,d['session']['expected_tokens']))
                    _need(source.execution_allowed is False and source.computation_verified is False
                          and source.tokenizer_status=='internal_metadata_only' and source.model_status=='unverified_declared')
                    binding=dict(input_snapshot=source.snapshot_digest,token_digest=source.token_blob_sha256,preprocessing=source.contract_sha256)
                    self._active=dict(task=asyncio.current_task(),thread=threading.get_ident(),credential=credential,
                        expected=(issuer.namespace,task_id,owner,task_id,hashlib.sha256(canonical(d).encode()).hexdigest()),
                        binding=canonical(binding),source=source,consumed=False)
                    self._spent=True;attempted=True
                    try:
                        result=issuer.register(Task.model_validate_json(canonical(d['task'])),
                            Session.model_validate_json(canonical(d['session'])),
                            tuple(Stage.model_validate_json(canonical(x)) for x in d['stages']),d['plan']['tiles'],
                            hidden_size=hidden_size,vocab_size=vocab_size,config_sha256=config_sha256,
                            source_id=task_id,source_binding=binding)
                        _need(self._active['consumed']);self._owner(credential,owner)
                        _need(self._reader._read_locked(credential,task_id,owner)==source)
                    finally:self._active=None
                # Scope exit can revoke, expire or fail after the durable insert.
                self._owner(credential,owner)
                _need(self._reader._read_locked(credential,task_id,owner)==source)
                return result
        except BaseException:
            if attempted:issuer.retired=True
            raise
        finally:self._active=None
