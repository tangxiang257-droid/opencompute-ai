"""Private role-local startup over supplied authenticated streams.

No listener/launcher/model invocation. Providers are trusted external dependencies;
method checks do not authenticate them. Unstarted means no H advertisement, not
no reader task. Original issuance connection remains owned for role lifetime.
Caller owns and must cancel/join any task invoking install; close retires transports
and journals, including a journal awaiting the B preparation barrier.
"""
import asyncio
from .channel import _accept_unstarted
from .online_issuance import RoleInstallStore
from .online_issuance_transport import IssuanceClient, _copy
from .prepared_receipt import PreparedReceiptConnection, _retire_failed
from .control_exchange import _need
from .delivery_receipt import _with_cleanup


def _methods(provider,names):
    _need(provider is not None and all(callable(getattr(provider,n,None)) for n in names))

async def _close_channel(channel):
    try:await channel.close()
    finally:
        if not channel._task.done():channel._task.cancel()
        await asyncio.gather(channel._task,return_exceptions=True)

class RoleStartup:
    def __init__(self):raise TypeError('use await RoleStartup.create')

    @classmethod
    async def create(cls,reader,writer,config,grants,store,descriptor,*,
                     issuer_node,issuer_certificate,identity=None,authorize=None,
                     resources=None,completion=None,runtime=None):
        """Take ownership of supplied writer even on rejected construction."""
        obj=object.__new__(cls);obj.client=None;obj.channel=None;obj.journal=None
        obj.prepared=None;obj.descriptor=None;obj._close_task=None;delegated=False
        obj._attempted=False;obj._closed=False;obj.store=store;obj.runtime=None
        obj.stage=descriptor.get('stage') if type(descriptor)is dict else None
        async def close_writer():
            try:writer.close()
            finally:await asyncio.wait_for(writer.wait_closed(),10)
        try:
            _need(cls is RoleStartup and type(store)is RoleInstallStore and obj.stage in (0,1))
            _need(type(runtime)is dict);obj.runtime=dict(runtime)
            obj.descriptor=_copy(descriptor)
            obj._preflight()
            for p in (identity,authorize,resources):_methods(p,('hold',))
            _methods(completion,('check','begin','finish','quarantine'))
            delegated=True
            obj.channel=await _accept_unstarted(reader,writer,config,grants,capacity_bytes=32768,capacity_tiles=1)
            obj.client=IssuanceClient(obj.channel,store,descriptor,issuer_node=issuer_node,
                issuer_certificate=issuer_certificate,identity=identity,authorize=authorize,
                resources=resources,completion=completion)
            return obj
        except BaseException as error:
            if delegated and obj.channel is None:raise
            await _retire_failed(error,(lambda:_close_channel(obj.channel)) if obj.channel is not None else close_writer)

    def _preflight(self):
        _need(not self._closed);self.store.ready();_methods(self.store.resources,('hold_control',))
        r=self.runtime
        d=self.descriptor;stage=d['declaration']['stages'][self.stage]
        _need(r.get('instance')==d['instance'] and r.get('layer_range')==(stage['layer_start'],stage['layer_end']))
        _need(set(r)=={'instance','layer_range','authority','resources','ticket','device','loader','factories'})
        _need(r['factories'] is None or callable(r['factories']))
        _need(type(r['instance'])is str and type(r['layer_range'])is tuple)
        _need(r['ticket'] is not None and type(r['ticket'])is not bool)
        _methods(r['resources'],('verify_stage','quarantine'))
        _methods(r['device'],('hold_load','open_scope','require_open','materialize','finish_scope','require_finished','quarantine'))
        _methods(r['loader'],('load','quarantine'))
        _methods(r['authority'],('hold_load',)+(('hold_stage0',) if self.stage==0 else ('hold_call','finalize')))

    async def install(self,path,*,execution_authorize=None,prepared=None,prepare_fence=None):
        _need(not self._attempted and not self._closed);self._attempted=True
        try:
            self._preflight()
            if self.stage==0:_need(callable(execution_authorize) and prepared is None and prepare_fence is None)
            else:
                _need(type(prepared)is PreparedReceiptConnection and execution_authorize is None)
                self.prepared=prepared
            handle=await self.client.install()
            self._preflight();self.client.live_guard()
            d=handle.pending.request;stage=d['declaration']['stages'][self.stage]
            _need(self.runtime['instance']==d['instance'] and self.runtime['layer_range']==(stage['layer_start'],stage['layer_end']))
            if self.stage==0:self.journal=self.store.create_sender(handle,path,execution_authorize=execution_authorize)
            else:self.journal=await prepared.prepare_online(self.store,handle,path,prepare_fence=prepare_fence)
            self._preflight();self.client.live_guard()
            return self.journal
        except BaseException as error:
            self._closed=True
            await _retire_failed(error,self.close)

    async def close(self):
        """Drain owned readers even when caller cancellation repeats."""
        self._closed=True
        async def cleanup():
            try:
                if self.journal is not None:self.journal.close()
                elif self.prepared is not None and self.prepared.journal is not None:self.prepared.journal.close()
            finally:
                try:
                    if self.prepared is not None:await _close_channel(self.prepared.channel)
                finally:await self.client.close()
        if self._close_task is None:self._close_task=asyncio.create_task(cleanup())
        cancellation=None
        while not self._close_task.done():
            try:await asyncio.shield(self._close_task)
            except asyncio.CancelledError as error:cancellation=error
            except BaseException:break
        try:self._close_task.result()
        except BaseException as error:
            if cancellation is not None:raise _with_cleanup(cancellation,error)
            raise
        if cancellation is not None:raise cancellation
