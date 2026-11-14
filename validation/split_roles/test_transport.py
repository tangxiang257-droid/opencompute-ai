"""Ordinary bytes, real Channels, invented SSL; no remote runtime proof."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import patch
import hashlib
import struct
import unittest
from validation.role_bridge import test_transport as fixtures
from validation.role_bridge.test_transport import streams, config
from cluster_p1.channel import Channel, Grant
from cluster_p1.byte_spool import Binding, Plan, Spool
from cluster_p1.envelope import Envelope
from cluster_p1.split_role_transport import HiddenSender, HiddenReceiver, ResultSender, ResultIngress, transport_scope

class Identity:
    @contextmanager
    def hold(self,identity):yield identity.node_id
class Authority:
    revoked=False
    active=0
    @contextmanager
    def hold(self,principal,binding,action):
        if self.revoked:raise PermissionError('revoked')
        self.active+=1
        try:yield None
        finally:self.active-=1

class RecipientAuthority:
    """Explicit synthetic C policy, not B's runtime/transport permission."""
    revoked=False
    active=0
    @contextmanager
    def hold_receiver(self,context,action):
        if self.revoked or context['recipient_node']!='c' or context['purpose']!='result-store':raise PermissionError('C permission absent')
        if context['receive_binding']['scope']['peer_node']!='b':raise PermissionError('wrong result sender')
        self.active+=1
        try:yield None
        finally:self.active-=1

class Tests(unittest.IsolatedAsyncioTestCase):
    make_receiver=fixtures.Tests.make_receiver
    fence=fixtures.Tests.fence
    async def asyncSetUp(self):
        self.channels=[];self.bridge=None
        await fixtures.Tests.asyncSetUp(self)
        self.identity=Identity();self.authority=Authority()
        self.kw=dict(identity=self.identity,authorize=self.authority)
        self.hs=HiddenSender(self.hidden_send,scope=transport_scope(self.spec),**self.kw)
        self.hr=HiddenReceiver(self.bound,self.receiver,scope=transport_scope(self.spec),**self.kw)
        a,aw,b,bw=streams('b','c')
        grant=Grant(self.spec.request,self.spec.session,1,1,(0,1),self.spec.model,2)
        self.ms=Channel(a,aw,config('c'),{'c':{grant}},capacity_bytes=32768,capacity_tiles=1)
        self.mr=Channel(b,bw,config('b'),{'b':{grant}},capacity_bytes=32768,capacity_tiles=1)
        self.channels.extend((self.ms,self.mr))
        self.rs=ResultSender(self.result_send,self.ms,self.binding,**self.kw)
        self.recipient=RecipientAuthority()
        self.ri=ResultIngress(self.result_reader,self.result_writer,config('b'),self.mr,self.binding,recipient_node='c',receive_authorize=self.recipient,**self.kw)
    async def asyncTearDown(self):
        await asyncio.gather(*(x.close() for x in (self.hs,self.hr,self.rs,self.ri)),return_exceptions=True)
        await fixtures.Tests.asyncTearDown(self)
    def spool(self):
        s=self.spec
        b=Binding(s.owner,s.request,s.session,'single-token',1,self.rs.generation(),s.model,s.layout,s.input_snapshot,s.token_digest,s.preprocessing,s.plan_digest)
        p=Plan(b,(1,),s.vocab)
        self.source.reserve('result-key',p,'source');self.destination.reserve('result-key',p,'destination')
        spool=Spool(self.root/'split-spool',self.source,self.destination,'result-key',p)
        spool.produce(lambda write:[write(0,c,struct.pack('<f',1.)*min(8192,s.vocab-c)) for c in range(0,s.vocab,8192)])
        return spool,hashlib.sha256(struct.pack('<f',1.)*s.vocab).hexdigest()
    async def test_full_hidden_and_result(self):
        s=self.spec
        raw=Envelope(s.request,s.session,1,0,0,(0,1),s.model,2,(1,1,s.hidden),'float32',struct.pack('<f',1.)*s.hidden).encode()
        sent,received=await asyncio.gather(self.hs.send(raw),self.hr.receive())
        self.assertEqual(sent,received);self.assertFalse(sent['execution_allowed'])
        spool,digest=self.spool()
        ack,manifest=await asyncio.gather(self.rs.send(s,spool,digest,lambda:None),self.ri.receive(s,self.artifacts,lambda:None,self.fence))
        self.assertEqual(ack['manifest_digest'],manifest)
        chunks=[];self.artifacts.fetch(s.owner,s.request,lambda tile,b:chunks.append(b))
        self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*151936)
        self.assertEqual(sum(f[:1]==b'D' for f in self.result_send.w.frames),19)
        self.assertFalse(any(hasattr(self.rs,n) for n in ('_receiver','_ledger','_journal')))
    async def test_default_deny(self):
        with self.assertRaises(ValueError):HiddenSender(self.hidden_send,scope=transport_scope(self.spec))

    async def test_wrong_generation_denied_before_destination_reserve(self):
        spool,digest=self.spool();self.result_writer.unique=b'other-data-incarnation'
        results=await asyncio.gather(self.rs.send(self.spec,spool,digest,lambda:None),self.ri.receive(self.spec,self.artifacts,lambda:None,self.fence),return_exceptions=True)
        self.assertTrue(all(isinstance(x,BaseException) for x in results))
        self.assertEqual(sum(f[:1]==b'H' for f in self.result_writer.frames),0)
    async def test_corrupt_result_frame_retains_local_promises(self):
        spool,digest=self.spool()
        from cluster_p1 import split_role_transport as module
        original=module.payload
        def corrupt(*args):
            data=original(*args);return data[:4]+b'X'+data[5:]
        with patch.object(module,'payload',side_effect=corrupt):
            result=await asyncio.gather(self.rs.send(self.spec,spool,digest,lambda:None),self.ri.receive(self.spec,self.artifacts,lambda:None,self.fence),return_exceptions=True)
        self.assertTrue(all(isinstance(x,BaseException) for x in result))
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))
        with self.assertRaises(Exception):await self.rs.send(self.spec,spool,digest,lambda:None)
    async def test_cancel_credit_barrier_joins_local_readers(self):
        spool,digest=self.spool();self.result_writer.block_kind=b'C'
        sender=asyncio.create_task(self.rs.send(self.spec,spool,digest,lambda:None))
        receiver=asyncio.create_task(self.ri.receive(self.spec,self.artifacts,lambda:None,self.fence))
        await asyncio.wait_for(self.result_writer.entered.wait(),3)
        receiver.cancel()
        result=await asyncio.gather(sender,receiver,return_exceptions=True)
        self.assertTrue(all(isinstance(x,BaseException) for x in result))
        self.assertTrue(self.result_send._task.done());self.assertTrue(self.ri._receiver._channel._task.done())
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))
    async def test_authorization_revoked_during_metadata_credit(self):
        spool,digest=self.spool();self.mr.w.block_kind=b'C'
        sender=asyncio.create_task(self.rs.send(self.spec,spool,digest,lambda:None))
        receiver=asyncio.create_task(self.ri.receive(self.spec,self.artifacts,lambda:None,self.fence))
        await asyncio.wait_for(self.mr.w.entered.wait(),3)
        self.authority.revoked=True;self.mr.w.resume.set()
        result=await asyncio.gather(sender,receiver,return_exceptions=True)
        self.assertTrue(all(isinstance(x,BaseException) for x in result))
        self.assertIsNone(self.ri._receiver)
        self.assertEqual(sum(f[:1]==b'H' for f in self.result_writer.frames),0)
    async def test_distinct_metadata_principal_denied(self):
        class Wrong:
            @contextmanager
            def hold(inner,who):yield who.connection_generation
        self.rs._identity=Wrong();spool,digest=self.spool()
        with self.assertRaises(ValueError):await self.rs.send(self.spec,spool,digest,lambda:None)
        self.assertEqual(sum(f[:1]==b'D' for f in self.ms.w.frames),0)

    async def test_actual_input_mismatch_rejected_before_data_H(self):
        spool,digest=self.spool()
        wrong=replace(self.spec,inputs=('e'*64,))
        result=await asyncio.gather(self.rs.send(self.spec,spool,digest,lambda:None),self.ri.receive(wrong,self.artifacts,lambda:None,self.fence),return_exceptions=True)
        self.assertTrue(all(isinstance(x,BaseException) for x in result))
        self.assertIsNone(self.ri._receiver)
    async def test_repeated_cancel_close_joins_metadata(self):
        entered=asyncio.Event();resume=asyncio.Event()
        async def wait_closed():entered.set();await resume.wait()
        self.result_writer.wait_closed=wait_closed
        task=asyncio.create_task(self.ri.close())
        await entered.wait();task.cancel()
        # An event-loop callback supplies the second cancellation; no sleep.
        loop=asyncio.get_running_loop();loop.call_soon(task.cancel);loop.call_soon(resume.set)
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(self.mr._task.done());self.assertTrue(self.mr.w.closed)

    async def test_commit_succeeded_response_lost_is_not_retried(self):
        spool,digest=self.spool();original=self.artifacts._commit_published
        def lost(*a,**kw):original(*a,**kw);raise RuntimeError('commit response lost')
        with patch.object(self.artifacts,'_commit_published',side_effect=lost):
            result=await asyncio.gather(self.rs.send(self.spec,spool,digest,lambda:None),self.ri.receive(self.spec,self.artifacts,lambda:None,self.fence),return_exceptions=True)
        self.assertTrue(all(isinstance(x,BaseException) for x in result))
        chunks=[];self.artifacts.fetch(self.spec.owner,self.spec.request,lambda tile,b:chunks.append(b))
        self.assertEqual(len(b''.join(chunks)),607744)
        before=len(self.result_send.w.frames)
        with self.assertRaises(Exception):await self.rs.send(self.spec,spool,digest,lambda:None)
        self.assertEqual(len(self.result_send.w.frames),before)
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))

    async def test_fences_cover_sync_checks_and_never_network_waits(self):
        spool,digest=self.spool()
        from validation.role_bridge.test_transport import Writer
        original=Writer.drain
        async def drain(writer):
            self.assertEqual(self.authority.active,0)
            await original(writer)
        def check():self.assertGreater(self.authority.active,0)
        with patch.object(Writer,'drain',drain):
            await asyncio.gather(self.rs.send(self.spec,spool,digest,check),self.ri.receive(self.spec,self.artifacts,check,self.fence))
        self.assertEqual(self.authority.active,0)

if __name__=='__main__':unittest.main()
