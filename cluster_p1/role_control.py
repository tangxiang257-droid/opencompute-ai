"""Private split role registration and coordinator client, no remote bootstrap.

The descriptor is inert data, not a permit. Real identity, authorization,
existing resource promises and device completion providers are all external.
Tests use invented metadata/providers; no physical completion is inferred here.
The existing ControlPair remains unchanged for compatibility.
"""
from dataclasses import dataclass
import asyncio,hashlib,inspect,uuid
from .control_exchange import ControlClient,ControlService,_json,_parse,_nothing,result_digest
from .control_journal import _binding,_identifier
from .local_attempt import _non_suppressing
from .projection_driver import Spec
from .delivery_receipt import _with_cleanup

def _need(ok):
    if not ok:raise ValueError('private role registration rejected')

def _ids(binding):
    command=hashlib.sha256(b'control-command-v1\0'+_json(binding)).hexdigest()
    return command,hashlib.sha256(b'control-reservation-v1\0'+command.encode()).hexdigest()

@dataclass(frozen=True,slots=True)
class RoleDescriptor:
    """Canonical ordinary data installed by trusted out-of-band bootstrap.

    peer inside binding identifies the controller as seen by the role, NOT the
    reverse Channel identity. The client independently verifies role_node against
    its own authenticated channel. Certificates/generations require real bootstrap.
    """
    role_node:str
    command_id:str
    reservation_id:str
    binding_json:bytes
    charge:int
    def __post_init__(self):
        _identifier(self.role_node)
        binding=_parse(self.binding_json);_binding(binding)
        _need((self.command_id,self.reservation_id)==_ids(binding))
        _need(type(self.charge)is int and 0<self.charge<2**63)
    @property
    def binding(self):return _parse(self.binding_json)

class RegisteredRole:
    """Local operation owner. Only the role bootstrap supplies service/body.

    completion.check(binding), begin(binding,scope), finish(binding,scope) and
    quarantine(binding,scope) are synchronous, yield no permit, and return None.
    Real completion must own ALL work of operation(scope), including partial
    begin, copies and other streams. Method names/None are not physical evidence.
    No scope/fence is held as a context across operation or network awaits.
    """
    __slots__=('_service','_operation','_completion','_scope','_quarantined','_descriptor')
    @property
    def descriptor(self):return self._descriptor
    @classmethod
    def register(cls,service,spec,operation,*,declaration_digest,charge,completion=None):
        _need(cls is RegisteredRole and type(service)is ControlService and type(spec)is Spec)
        _need(callable(operation) and service._operation is None)
        _need(completion is not None and all(callable(getattr(completion,n,None)) for n in ('check','begin','finish','quarantine')))
        service.check(spec,declaration_digest)
        binding=service.binding(spec);command,reservation=_ids(binding)
        descriptor=RoleDescriptor(service.node,command,reservation,_json(binding),charge)
        # Existing promises only. Registration creates no new external resource.
        with service.fence(binding,'register'):
            with _non_suppressing(service.journal.resources.hold(reservation,descriptor.binding,charge)) as value:
                _nothing(value);_nothing(completion.check(descriptor.binding))
        obj=object.__new__(cls)
        obj._service=service;obj._operation=operation;obj._completion=completion
        obj._scope=None;obj._quarantined=False;obj._descriptor=descriptor
        service.bind(spec,obj._invoke,command,reservation,charge)
        return obj
    def _quarantine(self):
        if self._scope is not None and not self._quarantined:
            # If quarantine raises it remains retryable, but never enables work.
            _nothing(self._completion.quarantine(self.descriptor.binding,self._scope))
            self._quarantined=True
    async def _invoke(self):
        _need(self._scope is None)
        self._scope=uuid.uuid4().hex
        try:
            _nothing(self._completion.begin(self.descriptor.binding,self._scope))
            value=self._operation(self._scope)
            if inspect.isawaitable(value):value=await value
            result_digest(value) # Reject object/reference/nonordinary outputs before finish.
            _nothing(self._completion.finish(self.descriptor.binding,self._scope))
            return value
        except BaseException as error:
            try:self._quarantine()
            except BaseException as cleanup:raise _with_cleanup(error,cleanup)
            raise
    async def serve_once(self):
        """Called by a role-owned task; coordinator cannot schedule this method."""
        try:
            await self._service.serve_once()
            if self._scope is not None:
                receipt=self._service.journal.query(self.descriptor.command_id)
                if receipt is None or receipt['state']!='COMMITTED' or receipt['retired']:
                    self._quarantine()
        except BaseException as error:
            primary=error
            # Conservatively retire an existing intent, including cancellation
            # during credit release before _invoke could register a device scope.
            # This local cleanup grants no execution and releases no reservation.
            try:
                if self._service.journal.query(self.descriptor.command_id) is not None:
                    self._service.journal.fail(self.descriptor.command_id,self.descriptor.reservation_id,self.descriptor.binding)
            except BaseException as cleanup:primary=_with_cleanup(primary,cleanup)
            try:self._quarantine()
            except BaseException as cleanup:primary=_with_cleanup(primary,cleanup)
            raise primary

class CoordinatorControl:
    """Client+fixed data only. Never accepts a body or starts a service task.

    Separate trusted role bootstrap must run the role's event loop. This module
    does not connect hosts, deploy a node, grant execution or recover a session.
    """
    __slots__=('_client','_descriptor','_started','_busy','_closed')
    def __init__(self,client,descriptor):
        _need(type(client)is ControlClient and type(descriptor)is RoleDescriptor)
        descriptor.__post_init__();client.current()
        _need(client._identity.node_id==descriptor.role_node)
        _need(not getattr(client.channel,'_role_control_claimed',False))
        # One coordinator wrapper per live connection. Reconstructing a wrapper
        # after an uncertain prepare cannot restore first-dispatch eligibility.
        # This local marker is not global replay prevention or a reconnect grant.
        client.channel._role_control_claimed=True
        self._client=client;self._descriptor=descriptor
        self._started=False;self._busy=False;self._closed=False
    @property
    def descriptor(self):return self._descriptor
    async def _request(self,action):
        d=self._descriptor;self._client.current()
        _need(self._client._identity.node_id==d.role_node)
        return await self._client.request(action,d.command_id,d.reservation_id,d.binding,d.charge)
    async def dispatch(self):
        _need(not self._busy and not self._closed);self._busy=True
        try:
            if self._started:return await self._request('query')
            self._started=True # Set before I/O: an unknown response never repeats execute.
            receipt=await self._request('prepare')
            if receipt['state']!='RESERVED' or receipt['retired']:return receipt
            return await self._request('execute')
        except BaseException as error:
            try:await self.close()
            except BaseException as cleanup:raise _with_cleanup(error,cleanup)
            raise
        finally:self._busy=False
    async def query(self):
        _need(self._started and not self._busy and not self._closed);self._busy=True
        try:return await self._request('query')
        except BaseException as error:
            try:await self.close()
            except BaseException as cleanup:raise _with_cleanup(error,cleanup)
            raise
        finally:self._busy=False
    async def close(self):
        self._closed=True
        # Only this client's shutdown task is owned here, never a role task.
        async def close_transport():
            channel=self._client.channel
            try:await channel.close()
            finally:
                # Channel._abort may fail in writer.close before cancelling its
                # reader. Settle that owned reader even on writer failure.
                if not channel._task.done():channel._task.cancel()
                await asyncio.gather(channel._task,return_exceptions=True)
        task=asyncio.create_task(close_transport());cancelled=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as error:cancelled=error
            except BaseException:break
        try:task.result()
        except BaseException as error:
            if cancelled is not None:raise _with_cleanup(cancelled,error)
            raise
        if cancelled is not None:raise cancelled
