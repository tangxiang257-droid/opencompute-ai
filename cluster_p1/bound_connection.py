"""Private connection-to-receipt assembly; no socket creation or default gate.

Real TLS identity is established by Channel construction, not by this adapter.
Trusted Python assembly, single owning loop; no global nonce registry/recovery.
"""
import asyncio
from contextlib import contextmanager
from .channel import Channel,Delivery
from .receipt_attempt import ReceiptAttemptJournal,PeerIdentity
from .delivery_receipt import DeliveryAttempt,_with_cleanup

class BoundReceiptConnection:
    def __init__(self,channel):
        if type(channel)is not Channel:raise TypeError('real Channel required')
        if getattr(channel,'_receipt_adapter_bound',False):raise ValueError('connection already bound')
        self._channel=channel
        self._identity=channel.authenticated_identity
        identity=self._identity
        self.expected_peer=PeerIdentity(identity.node_id,identity.certificate_sha256,identity.connection_generation)
        expected=self.expected_peer
        @contextmanager
        def peer_authorize(requested):
            if type(requested)is not PeerIdentity or requested!=expected:
                raise PermissionError('fixed connection peer required')
            with channel.identity_fence(identity):yield expected
        self.peer_authorize=peer_authorize
        with self.peer_authorize(expected):pass
        channel._receipt_adapter_bound=True
        self._journal_attempted=False
        self._busy=False

    def create_journal(self,path,task,session,stages,*,authorize,execution_authorize,
                       receipt_binding,local_node,expected_config_sha256):
        if self._journal_attempted:raise ValueError('journal creation already attempted')
        self._journal_attempted=True
        return ReceiptAttemptJournal.create(path,task,session,stages,authorize=authorize,
            execution_authorize=execution_authorize,receipt_binding=receipt_binding,
            expected_peer=self.expected_peer,peer_authorize=self.peer_authorize,
            local_node=local_node,expected_config_sha256=expected_config_sha256,
            enable_receipt_stage1=True)

    def _journal(self,journal):
        if (type(journal)is not ReceiptAttemptJournal
                or journal._receipt_peer!=self.expected_peer
                or journal._receipt_peer_provider is not self.peer_authorize):
            raise ValueError('journal belongs to a different connection binding')

    async def _close_failed(self,primary):
        # Unknown receive/ownership outcome retires the stream. Do not resend,
        # regrant credits, or silently leave a detached close task on cancellation.
        task=asyncio.create_task(self._channel.close())
        cancellation=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as exc:cancellation=exc
            except BaseException:break
        try:task.result()
        except BaseException as exc:primary=_with_cleanup(primary,exc)
        if cancellation is not None:raise _with_cleanup(cancellation,primary)
        raise primary

    async def receive_once(self,journal,*,commit_fence=None):
        self._journal(journal)
        if self._busy:raise ValueError('one bound receive at a time')
        with self.peer_authorize(self.expected_peer):pass
        self._busy=True
        try:
            receiver=journal.make_receiver() if commit_fence is None else journal.make_receiver(commit_fence=commit_fence)
            try:
                delivery=await self._channel.receive()
                if type(delivery)is not Delivery or delivery._channel is not self._channel:
                    raise ValueError('delivery belongs to another connection')
                # Construction and ownership claim contain no await; the real
                # receiver rechecks live peer state before recording queued data.
                attempt=DeliveryAttempt(receiver,delivery)
                return await attempt.run()
            except BaseException as exc:
                await self._close_failed(exc)
        finally:self._busy=False
