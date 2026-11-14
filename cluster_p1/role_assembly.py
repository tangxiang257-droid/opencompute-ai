"""Private installed-role composition, no listener, grant or automatic execution.

Caller supplies the actual installed startup and local A/B operation. This owns
that startup, operation and control channel after the one-time claim. External
identity/task/resource/device services remain necessary; method presence is not
trust. No source registration or offline submission implies execution consent.
"""
import asyncio
from .role_startup import RoleStartup
from .channel import Channel
from .role_resources import RoleResources
from .local_stage_operations import StageAOperation,StageBOperation
from .control_journal import ControlJournal
from .control_exchange import ControlService,_need
from .role_control import RegisteredRole,_ids
from .delivery_receipt import _with_cleanup

class InstalledStageRole:
    def __init__(self):raise TypeError('use await attach')
    @classmethod
    async def attach(cls,startup,operation,channel,path,*,coordinator_node,charge,
                     control_budget,identity=None,authorize=None):
        _need(cls is InstalledStageRole and type(startup)is RoleStartup)
        _need(not getattr(startup,'_installed_role_claimed',False))
        startup._installed_role_claimed=True # Never clear after an uncertain step.
        self=object.__new__(cls);self.startup=startup;self.operation=operation
        self.channel=channel;self.journal=None;self.role=None;self._closed=False
        self._active=None;self._close_task=None;self._runtime=None;self._owns_operation=False;self._owns_channel=False
        try:
            self._runtime=dict(startup.runtime)
            _need(type(channel)is Channel and channel is not startup.channel)
            _need(startup.prepared is None or channel is not startup.prepared.channel)
            _need(not any(getattr(channel,k,False) for k in ('_online_issuance_claimed','_receipt_state','_installed_role_control_claimed')))
            channel._installed_role_control_claimed=True;self._owns_channel=True
            self._check();self._owns_operation=True
            _need(callable(getattr(identity,'hold',None)) and callable(getattr(authorize,'hold',None)))
            d=startup.descriptor;r=startup.runtime['resources'];t=startup.runtime['ticket']
            self.journal=ControlJournal(path,control_budget,resources=r)
            service=ControlService(channel,self.journal,stage=startup.stage,node=d['node'],
                coordinator_node=coordinator_node,device=d['device'],instance=d['instance'],
                identity=identity,authorize=authorize)
            service.check(operation.spec,d['declaration_digest']);binding=service.binding(operation.spec)
            _,rid=_ids(binding)
            # Authenticate actual control connection/task permission before binding.
            with service.fence(binding,'register'):
                r.bind_control(t.reservation_id,rid,binding,charge)
            self.role=RegisteredRole.register(service,operation.spec,operation,
                declaration_digest=d['declaration_digest'],charge=charge,completion=operation.completion)
            self._check();return self
        except BaseException as error:
            try:await self.close()
            except BaseException as cleanup:raise _with_cleanup(error,cleanup)
            raise
    def _check(self):
        _need(not self._closed);s=self.startup;s._preflight();s.client.live_guard()
        _need(set(s.runtime)==set(self._runtime) and all(s.runtime[k] is v for k,v in self._runtime.items()))
        r=s.runtime;o=self.operation
        _need(type(o)is (StageAOperation if s.stage==0 else StageBOperation))
        _need(type(r['resources'])is RoleResources and r['resources'] is s.store.resources)
        _need((o.sender if s.stage==0 else o.receiver) is s.journal)
        _need(o.instance==r['instance'] and o.layer_range==r['layer_range'])
        for k in ('authority','resources','ticket','device','loader'):_need(getattr(o,k) is r[k])
        from .private_one_token import _factories
        _need(o.factory is (r['factories'] or _factories))
        s.store.require_issued(s.journal,s.stage);o.ready()
        if s.stage==1:o.received_bytes() # Actual committed B input, not coordinator claims.
    @property
    def descriptor(self):
        self._check();_need(self.role is not None);return self.role.descriptor
    async def serve_once(self):
        _need(self._active is None)
        self._active=asyncio.current_task()
        try:
            self._check();_need(self.role is not None)
            await self.role.serve_once();self._check()
        except BaseException:
            # Caller closes after this task exits; never revive restored providers.
            self._closed=True
            raise
        finally:self._active=None
    async def close(self):
        if self._close_task is None:
            self._closed=True
            async def cleanup():
                errors=[];task=self._active
                if task is not None and task is not asyncio.current_task():
                    task.cancel();results=await asyncio.gather(task,return_exceptions=True)
                    errors.extend(e for e in results if isinstance(e,BaseException) and not isinstance(e,asyncio.CancelledError))
                # Existing operation owns its backend/scope; no refunds or GPU-stop claim.
                if self._owns_operation:
                    try:self.operation.quarantine()
                    except BaseException as e:errors.append(e)
                    if type(self.operation)is StageBOperation:
                        try:self.operation.close()
                        except BaseException as e:errors.append(e)
                if self._owns_channel:
                    try:await self.channel.close()
                    except BaseException as e:errors.append(e)
                    if hasattr(self.channel,'_task'):
                        if not self.channel._task.done():self.channel._task.cancel()
                        await asyncio.gather(self.channel._task,return_exceptions=True)
                if self.journal is not None:
                    try:self.journal.close()
                    except BaseException as e:errors.append(e)
                try:await self.startup.close()
                except BaseException as e:errors.append(e)
                if errors:raise BaseExceptionGroup('installed role cleanup uncertain',errors)
            self._close_task=asyncio.create_task(cleanup())
        cancelled=None
        while not self._close_task.done():
            try:await asyncio.shield(self._close_task)
            except asyncio.CancelledError as e:cancelled=e
            except BaseException:break
        try:self._close_task.result()
        except BaseException as e:
            if cancelled is not None:raise _with_cleanup(cancelled,e)
            raise
        if cancelled is not None:raise cancelled
