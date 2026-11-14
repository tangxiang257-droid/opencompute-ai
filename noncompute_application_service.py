"""Unmounted non-computing application facade over ONE controlled authority.

Default user authentication and local plan provision both deny. No HTTP app,
listener, model/runtime imports or environment credential discovery. A trusted
local plan provider is an explicit substitute for a real planning/admission system.
"""
import types
import inspect
from dataclasses import dataclass, replace

import controlled_delivery_authority as control
import task_input_api as identity


class ServiceRejected(ValueError):
    def __init__(self):super().__init__('operation unavailable')


class InputConflict(ServiceRejected):
    pass

class DraftConflict(ServiceRejected):pass
class DraftMissing(ServiceRejected):pass


def _safe(method):
    # Discard private operation traceback before exposing a fixed public error.
    async def call(self,*args,**kwargs):
        operation=None;conflict=None
        try:operation=method(self,*args,**kwargs)
        except Exception:pass
        args=kwargs=None
        if operation is not None:
            try:return await operation
            except InputConflict:conflict=InputConflict
            except DraftConflict:conflict=DraftConflict
            except DraftMissing:conflict=DraftMissing
            except Exception:pass
        operation=None
        if conflict:raise conflict()
        raise ServiceRejected()
    return call


def _task(value):
    if type(value) is not str or len(value)!=32 or any(c not in '0123456789abcdef' for c in value):raise ServiceRejected()


def _idem(value):
    if type(value) is not str or not 1<=len(value)<=128 or not identity._ID.fullmatch(value):raise ServiceRejected()


def _digests(values):
    if type(values) is not tuple or not 1<=len(values)<=64:raise ServiceRejected()
    for value in values:
        if type(value) is not str or len(value)!=64 or not identity._HASH.fullmatch(value):raise ServiceRejected()


@dataclass(frozen=True, slots=True)
class InputRegistration:
    task_id: str
    state: str = 'input_registered'
    execution_allowed: bool = False


class RejectPlans:
    def plan_for(self, task, verified_input):
        raise ServiceRejected()


@dataclass(frozen=True, slots=True)
class TaskPage:
    items: tuple
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class TaskDetail:
    task_id: str
    registration_state: str
    result_type: str | None
    result_availability: str
    physical_stop: str = 'unconfirmed'
    execution_allowed: bool = False
    computation_verified: bool = False


def _detail(record):
    _task(record.task_id)
    states={'STORED':'input_registered', 'CANCELLED':'cancellation_registered',
            'STOP_PENDING':'stop_pending', 'RESULT_STORED':'result_recorded'}
    if record.task_state not in states:raise ServiceRejected()
    # Current ledger accepts ONLY prefill_logits_v1. This is a declaration of
    # recorded type, not a file integrity/availability check or generated text.
    result=record.task_state=='RESULT_STORED'
    return TaskDetail(record.task_id,states[record.task_state],
                      'prefill_logits_v1' if result else None,
                      'not_checked' if result else 'not_recorded')


class ApplicationService:
    def __init__(self):raise ServiceRejected()

    def _owner(self, credential):
        # No caller-specified owner/principal/header or admin/Worker fallback.
        if type(credential) is not str or not 1<=len(credential)<=4096 or not all(33<=ord(c)<=126 for c in credential):raise ServiceRejected()
        return identity._owner(self._verifier.verify(credential))

    @_safe
    async def create_input(self, credential, idem, raw):
        conflict=False
        try:
            owner=self._owner(credential);_idem(idem)
            if type(raw) is not bytes or len(raw)>identity._MAX_BODY:raise ServiceRejected()
            binding,tokens=identity._payload(raw,owner)
            task=await self._authority.create(owner,idem,binding,tokens)
            return InputRegistration(task.task_id,task.state)
        except control.RegistrationConflict:conflict=True
        except Exception:pass
        del credential,raw
        if conflict:raise InputConflict()
        raise ServiceRejected()

    @_safe
    async def bind_plan(self, credential, task_id):
        try:
            owner=self._owner(credential);_task(task_id)
            task,stored=await self._authority.registered_input(owner,task_id)
            # Provider receives actual verified snapshot metadata/bytes. Its result
            # still goes through canonical parser, identity checks and DB binding.
            raw=self._plans.plan_for(task,stored)
            return await self._authority.bind_plan(owner,task_id,raw)
        except Exception:pass
        del credential
        raise ServiceRejected()

    @_safe
    async def accept_result(self, credential, task_id, idem, digests, reader, writer):
        try:
            _idem(idem);_digests(digests)
            owner=self._owner(credential);_task(task_id)
            await self._authority.require_owner(owner,task_id)
            key=await self._authority.register_result_set(owner,task_id,reader,writer,self._tls,idem,digests)
            self._connections[key]=(owner,task_id)
            return key
        except Exception:
            writer.close()
        except BaseException:
            writer.close();raise
        del credential
        raise ServiceRejected()

    def _connection_owner(self, credential, key):
        owner=self._owner(credential)
        if type(key) is not object:raise ServiceRejected()
        row=self._connections.get(key)
        if row is None or row[0]!=owner:raise ServiceRejected()
        return owner

    @_safe
    async def result_contexts(self, credential, key):
        try:return await self._authority.result_set_contexts(self._connection_owner(credential,key),key)
        except Exception:pass
        del credential
        raise ServiceRejected()

    @_safe
    async def receive_result(self, credential, key):
        try:return await self._authority.receive_framed_result(self._connection_owner(credential,key),key)
        except Exception:pass
        del credential
        raise ServiceRejected()

    def _download_owner(self, credential):
        # Download-only synchronous verifier contract. Never execute an awaitable.
        if type(credential) is not str or not 1<=len(credential)<=4096 or not all(33<=ord(c)<=126 for c in credential):raise ServiceRejected()
        principal=self._verifier.verify(credential)
        if type(principal) is types.CoroutineType:
            if inspect.getcoroutinestate(principal)==inspect.CORO_CREATED:principal.close()
            raise ServiceRejected()
        return identity._owner(principal)

    def _download_route(self, task_id):
        route=getattr(self,'_c_result_routes',{}).get(task_id)
        if route is not None:
            from private_c_result_route import LocalCResultRoute
            if type(route) is not LocalCResultRoute or route.service is not self or route.task!=task_id:raise ServiceRejected()
        return route

    def _download_final(self, credential, task_id, owner, digest, route):
        if self._download_owner(credential)!=owner or self._download_route(task_id) is not route:raise ServiceRejected()
        if route is not None:route.final(credential,digest)

    @_safe
    async def download_result(self, credential, task_id, sink, *, max_bytes, metadata):
        owner=self._download_owner(credential);_task(task_id)
        if not callable(sink) or not callable(metadata):raise ServiceRejected()
        if type(max_bytes) is not int or not 0<max_bytes<=4194304:raise ServiceRejected()
        route=self._download_route(task_id)
        if route is not None:return await route.fetch(credential,sink,max_bytes=max_bytes,metadata=metadata)
        def authorize():return self._download_owner(credential)
        return await self._authority._download_result(owner,task_id,sink,
            _authorize=authorize,max_bytes=max_bytes,metadata=metadata)

    @_safe
    async def read_result(self, credential, task_id, idem, sink):
        try:
            _idem(idem)
            if not callable(sink):raise ServiceRejected()
            owner=self._owner(credential);_task(task_id)
            await self._authority.require_owner(owner,task_id)
            return await self._authority.read_result(owner,task_id,idem,sink)
        except Exception:pass
        del credential
        raise ServiceRejected()

    @_safe
    async def result_reference(self, credential, task_id, idem):
        try:
            _idem(idem)
            owner=self._owner(credential);_task(task_id)
            await self._authority.require_owner(owner,task_id)
            return await self._authority.result_reference(owner,task_id,idem)
        except Exception:pass
        del credential
        raise ServiceRejected()

    @_safe
    async def cancel(self, credential, task_id):
        try:
            owner=self._owner(credential);_task(task_id)
            await self._authority.require_owner(owner,task_id)
            return await self._authority.cancel(owner,task_id)
        except Exception:pass
        del credential
        raise ServiceRejected()

    @_safe
    async def task_list(self, credential, *, after=None, limit=10):
        owner=self._owner(credential)
        if type(limit) is not int or not 1<=limit<=20:raise ServiceRejected()
        if after is not None:_task(after)
        records,cursor=await self._authority.task_records(owner,after=after,limit=limit)
        return TaskPage(tuple(_detail(record) for record in records),cursor)

    @_safe
    async def task_detail(self, credential, task_id):
        owner=self._owner(credential);_task(task_id)
        return _detail(await self._authority.task_record(owner,task_id))

    @_safe
    async def cancel_detail(self, credential, task_id):
        # Project the actual cancellation transaction receipt, not a later read.
        # A result committed first therefore remains result_recorded.
        return _detail(await self.cancel(credential,task_id))

    async def close(self):
        await self._authority.close()

    @_safe
    async def draft(self, credential, action, **values):
        owner=self._owner(credential)
        failure=None
        try:return await self._authority.draft(owner,action,**values)
        except control.DraftVersionConflict:failure=DraftConflict
        except control.DraftUnavailable:failure=DraftMissing
        except Exception:pass
        credential=values=None
        if failure:raise failure()
        raise ServiceRejected()


def open_service(location,input_fd,result_fd,limits,*,tls,recipient,
                 deployment=None,verifier=None,local_plans=None,initialize=False,enable_drafts=False,enable_unified=False,enable_middle_loopback=False,middle_certificate_pins=None):
    """Trusted local configuration only. No production registration or routing."""
    if type(enable_middle_loopback) is not bool or (enable_middle_loopback and not enable_unified):raise ServiceRejected()
    if type(tls.peers) is not dict or len(tls.peers)!=1:raise ServiceRejected()
    pins=dict(tls.peers)
    if middle_certificate_pins is not None:
        if not enable_unified or type(middle_certificate_pins) is not dict:raise ServiceRejected()
        for name,pin in middle_certificate_pins.items():
            if name in pins and pins[name]!=pin:raise ServiceRejected()
            pins[name]=pin
    service=object.__new__(ApplicationService)
    service._tls=replace(tls,peers=dict(tls.peers))
    service._verifier=identity.RejectAllUsers() if verifier is None else verifier
    service._plans=RejectPlans() if local_plans is None else local_plans
    service._connections={}
    service._authority=control.open_authority(location,input_fd,result_fd,limits,
        deployment=deployment,recipient=recipient,certificate_pins=pins,
        initialize=initialize,delivery_mode='framed_result_set_v1',enable_drafts=enable_drafts,
        enable_unified=enable_unified,enable_middle_loopback=enable_middle_loopback)
    return service
