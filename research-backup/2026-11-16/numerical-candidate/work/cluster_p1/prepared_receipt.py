"""Private opt-in receipt-ready assembly, not permission to execute.

Trusted same-loop Python only. No TLS construction, remote permits, workers,
recovery or default-path changes. The optional awaitable fence is only a local
preparation barrier; its return value never establishes readiness/authorization.
"""
import asyncio
import json
from contextlib import contextmanager
from .channel import Channel,ChannelError
from .bound_connection import BoundReceiptConnection
from .nonce_coordinator import NonceCoordinator
from .local_attempt import _non_suppressing
from .delivery_receipt import _with_cleanup
from .store import Conflict
from .handoff import MAX_FRAME_BYTES

@contextmanager
def _deny_execution(*args):
    raise PermissionError('receipt readiness does not authorize execution')
    yield  # contextmanager shape; unreachable

async def _retire_failed(primary,closer):
    # Cancellation must not detach cleanup, including a second cancellation.
    task=asyncio.create_task(closer());cancellation=None
    while not task.done():
        try:await asyncio.shield(task)
        except asyncio.CancelledError as exc:cancellation=exc
        except BaseException:break
    try:task.result()
    except BaseException as exc:primary=_with_cleanup(primary,exc)
    if cancellation is not None:raise _with_cleanup(cancellation,primary)
    raise primary

class PreparedReceiptConnection:
    def __init__(self,*args,**kwargs):
        raise TypeError('use await PreparedReceiptConnection.create(...)')

    @classmethod
    async def create(cls,reader,writer,config,grants,**channel_options):
        if cls is not PreparedReceiptConnection:raise TypeError('exact assembly required')
        obj=object.__new__(cls);obj.channel=None;obj.journal=None;obj._attempted=False
        async def close_writer():
            writer.close()
            await asyncio.wait_for(writer.wait_closed(),channel_options.get('timeout',10))
        try:
            if '_gate_receipt' in channel_options:raise TypeError('assembly owns receipt gate')
            channel_options.setdefault('capacity_bytes',MAX_FRAME_BYTES)
            channel_options.setdefault('capacity_tiles',1)
            if (type(channel_options['capacity_bytes'])is not int
                    or not 1<=channel_options['capacity_bytes']<=MAX_FRAME_BYTES
                    or type(channel_options['capacity_tiles'])is not int
                    or channel_options['capacity_tiles']!=1):
                raise ValueError('receipt assembly requires <=64KiB payload and one tile')
            obj.channel=Channel(reader,writer,config,grants,_gate_receipt=True,**channel_options)
            obj.bound=BoundReceiptConnection(obj.channel)
            return obj
        except BaseException as primary:
            await _retire_failed(primary,obj.channel.close if obj.channel is not None else close_writer)

    @property
    def state(self):return self.channel._receipt_state

    async def prepare(self,coordinator,reservation,path,*,prepare_fence=None):
        self.channel._identity_loop_check()
        if self._attempted:raise ValueError('preparation already attempted')
        self._attempted=True
        try:
            if self.state!='unprepared':raise ChannelError('connection unavailable')
            self.channel._receipt_state='preparing'
            if type(coordinator)is not NonceCoordinator:raise TypeError('real nonce coordinator required')
            # No await until the real factory has burned its slot and installed
            # the fresh fixed-context ledger and complete V2 plan.
            j=coordinator.create_receiver(reservation,self.bound,path,execution_authorize=_deny_execution)
            self.journal=j
            expected=json.loads(reservation.body)['plan_digest']
            if prepare_fence is not None:await prepare_fence()
            # Revalidate after the await, in established journal->peer->SQLite
            # order. Barrier return values and arbitrary objects confer nothing.
            self.bound._journal(j)
            with _non_suppressing(j._authorized()) as owner, _non_suppressing(j._receipt_fence()), j._store._tx() as c:
                j._context(c)
                _,plan,_,_=j._plan(c,owner,active=True,stage_id=1)
                if plan!=expected or j._receipt_edge.plan_digest!=expected:
                    raise Conflict('prepared receipt plan changed')
                for table in ('p1_local_attempts','p1_local_stage1_attempts','p1_v2_receipts','p1_received_frames'):
                    if c.execute('SELECT count(*) FROM '+table).fetchone()[0]:
                        raise Conflict('fresh receipt ledger required')
            if self.state!='preparing':raise ChannelError('preparation retired')
            self.channel._receipt_state='ready'
            await self.channel.start()
            with self.bound.peer_authorize(self.bound.expected_peer):pass
            return j
        except BaseException as primary:
            if self.journal is not None:
                try:self.journal.close()
                except BaseException as cleanup:primary=_with_cleanup(primary,cleanup)
            await self.bound._close_failed(primary)

    async def prepare_online(self,store,handle,path,*,prepare_fence=None):
        """Explicit fresh online-local factory; H credit still follows all checks.

        JSON responses/history cannot be passed as an install qualification.
        Receipt readiness remains default-denied for actual execution.
        """
        from .online_issuance import RoleInstallStore
        self.channel._identity_loop_check()
        if self._attempted:raise ValueError('preparation already attempted')
        self._attempted=True
        try:
            if type(store)is not RoleInstallStore:raise TypeError('exact online local store required')
            if self.state!='unprepared':raise ChannelError('connection unavailable')
            self.channel._receipt_state='preparing'
            j=store.create_receiver(handle,self.bound,path,execution_authorize=_deny_execution)
            self.journal=j
            expected=json.loads(handle.body)['plan_digest']
            if prepare_fence is not None:await prepare_fence()
            store.require_issued(j,1);self.bound._journal(j)
            with _non_suppressing(j._authorized()) as owner,_non_suppressing(j._receipt_fence()),j._store._tx() as c:
                j._context(c)
                _,plan,_,_=j._plan(c,owner,active=True,stage_id=1)
                if plan!=expected or j._receipt_edge.plan_digest!=expected:raise Conflict('prepared online receipt plan changed')
                for table in ('p1_local_attempts','p1_local_stage1_attempts','p1_v2_receipts','p1_received_frames'):
                    if c.execute('SELECT count(*) FROM '+table).fetchone()[0]:raise Conflict('fresh receipt ledger required')
            if self.state!='preparing':raise ChannelError('preparation retired')
            self.channel._receipt_state='ready'
            await self.channel.start()
            store.require_issued(j,1)
            return j
        except BaseException as primary:
            if self.journal is not None:
                try:self.journal.close()
                except BaseException as cleanup:primary=_with_cleanup(primary,cleanup)
            await self.bound._close_failed(primary)
