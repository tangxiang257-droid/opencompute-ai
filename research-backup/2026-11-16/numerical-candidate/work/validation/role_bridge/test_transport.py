"""Real channels/journals/artifact files over memory streams and invented SSL.

No sockets, TLS handshake, models, device work or actual execution authority.
"""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import os
import sqlite3
import ssl
import struct
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from validation.one_token import test_assembly as fixtures
from cluster_p1.channel import Channel, Grant, TLSConfig, ChannelError
from cluster_p1.bound_connection import BoundReceiptConnection
from cluster_p1.prepared_receipt import PreparedReceiptConnection
from cluster_p1.contracts import Task, Session, Stage
from cluster_p1.envelope import Envelope
from cluster_p1.byte_spool import Binding, Plan, Spool
from isolated_tls_delivery import ReceiveBinding
from local_delivery_gate import Scope
try:
    from cluster_p1.async_role_bridge import AsyncRoleBridge
except ImportError:
    AsyncRoleBridge = None


class Writer:
    def __init__(self, peer, cert, server):
        self.peer=peer;self.cert=cert;self.server_side=server
        self.context=SimpleNamespace(verify_mode=ssl.CERT_REQUIRED,check_hostname=not server)
        self.closed=False;self.frames=[];self.block_kind=None
        self.entered=asyncio.Event();self.resume=asyncio.Event();self.last_kind=None
        self.unique=b'fake-memory-channel-binding'
    def get_extra_info(self,name):return self if name=='ssl_object' else None
    def getpeercert(self,binary_form=False):return self.cert
    def get_channel_binding(self,name):return self.unique
    def version(self):return 'TLSv1.2'
    def is_closing(self):return self.closed
    def write(self,data):
        if self.closed:raise ConnectionError('memory writer closed')
        self.frames.append(data);self.last_kind=data[:1];self.peer.feed_data(data)
    async def drain(self):
        if self.last_kind==self.block_kind:
            self.entered.set();await self.resume.wait()
    def close(self):
        if not self.closed:self.closed=True;self.peer.feed_eof()
        self.resume.set()
    async def wait_closed(self):pass


def streams(sender,receiver):
    a,b=asyncio.StreamReader(),asyncio.StreamReader()
    return a,Writer(b,receiver.encode(),False),b,Writer(a,sender.encode(),True)


def config(peer):
    return TLSConfig('unused','unused','unused',{peer:hashlib.sha256(peer.encode()).hexdigest()})


class Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(AsyncRoleBridge,'async role bridge missing')
        self.channels=[];self.bridge=None
    async def asyncSetUp(self):
        await fixtures.Tests.asyncSetUp(self)
        spec=self.spec;self.bound_journal=self.receiver
        self.bound=self.coordinator._issued_journals[id(self.receiver)][-1]
        sr,sw,rr,rw=streams('b','c');self.result_reader,self.result_writer=rr,rw
        grant1=Grant(spec.request,spec.session,1,1,(0,1),spec.model,2)
        self.result_send=Channel(sr,sw,config('c'),{'c':{grant1}},capacity_bytes=32816,capacity_tiles=1)
        self.channels.append(self.result_send)
        scope=Scope(spec.request,1,spec.plan_digest,spec.layout,'out','logits',spec.session,
                    'single-token','b',hashlib.sha256(b'b').hexdigest(),'untrusted-placeholder')
        self.binding=ReceiveBinding(scope,1,spec.model,2,'float32',((0,1,(1,1,spec.vocab)),))
        self.bridge=AsyncRoleBridge(hidden_sender=self.hidden_send,hidden_receiver=self.bound,
            receipt_journal=self.bound_journal,result_sender=self.result_send,result_reader=rr,
            result_writer=rw,result_config=config('b'),result_binding=self.binding)
        await self.hidden_send.start()
    async def make_receiver(self,reservation):
        grant0=Grant(self.task.task_id,'1'*32,1,0,(0,1),'a'*64,2)
        ar,aw,br,bw=streams('a','b')
        self.hidden_send=Channel(ar,aw,config('b'),{'b':{grant0}},capacity_bytes=65536,capacity_tiles=1)
        self.prepared=await PreparedReceiptConnection.create(br,bw,config('a'),{'a':{grant0}})
        self.hidden_recv=self.prepared.channel;self.channel=self.hidden_recv
        self.channels.extend((self.hidden_send,self.hidden_recv))
        receiver=await self.prepared.prepare(self.coordinator,reservation,self.root/'receiver.db')
        self.assertEqual(self.prepared.state,'ready')
        return receiver
    async def asyncTearDown(self):
        if self.bridge is not None:await self.bridge.close()
        await asyncio.gather(*(c.close() for c in self.channels))
    async def send_hidden(self):
        raw=Envelope(self.spec.request,self.spec.session,1,0,0,(0,1),self.spec.model,2,
                     (1,1,self.spec.hidden),'float32',struct.pack('<f',1.)*self.spec.hidden).encode()
        self.bridge.require_current(self.spec)
        self.assertEqual(await self.bridge.exchange_hidden(raw),raw)
        return raw
    def spool(self,**binding_changes):
        spec=self.spec
        binding=Binding(spec.owner,spec.request,spec.session,'single-token',1,
            self.bridge.reservation_generation(spec),spec.model,spec.layout,spec.input_snapshot,
            spec.token_digest,spec.preprocessing,spec.plan_digest)
        binding=replace(binding,**binding_changes)
        plan=Plan(binding,(1,),spec.vocab)
        self.source.reserve('result-key',plan,'source');self.destination.reserve('result-key',plan,'destination')
        spool=Spool(self.root/'result-spool',self.source,self.destination,'result-key',plan)
        def body(write):
            for col in range(0,spec.vocab,8192):write(0,col,struct.pack('<f',1.)*min(8192,spec.vocab-col))
        spool.produce(body)
        return spool,hashlib.sha256(struct.pack('<f',1.)*spec.vocab).hexdigest()
    @contextmanager
    def fence(self):yield None
    async def publish(self,spool,digest,check=lambda:None):
        return await self.bridge.publish_result(self.spec,spool,self.artifacts,'result-key',digest,check,self.fence)
    async def test_real_hidden_receipt_and_large_result_persistence(self):
        raw=await self.send_hidden()
        with sqlite3.connect(self.root/'receiver.db') as c:
            self.assertEqual(c.execute('select frame from p1_received_frames').fetchone(),(raw,))
        self.assertEqual(self.hidden_recv._used_tiles,0)
        spool,digest=self.spool();result=await self.publish(spool,digest)
        chunks=[];self.assertEqual(self.artifacts.fetch('owner',self.spec.request,lambda tile,b:chunks.append(b)),result)
        self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*151936)
        self.assertEqual(sum(f[:1]==b'D' for f in self.result_send.w.frames),19)
        self.assertEqual(sum(f[:1]==b'C' for f in self.result_writer.frames),18)
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))
    async def test_credit_not_returned_before_bounded_write(self):
        await self.send_hidden();spool,digest=self.spool();observed=[]
        original=os.write
        def write(fd,data):
            # First target write occurs after exactly one data frame was admitted.
            observed.append((len(data),sum(f[:1]==b'C' for f in self.result_writer.frames)))
            return original(fd,data)
        with patch('cluster_p1.async_role_bridge.os.write',side_effect=write):await self.publish(spool,digest)
        self.assertEqual(observed[0],(32768,0));self.assertTrue(all(n<=32768 for n,_ in observed))
    async def test_cancel_credit_release_joins_and_retains_quota(self):
        await self.send_hidden();spool,digest=self.spool();self.result_writer.block_kind=b'C'
        task=asyncio.create_task(self.publish(spool,digest))
        await asyncio.wait_for(self.result_writer.entered.wait(),3);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(self.result_writer.closed);self.assertTrue(self.result_send._closed)
        self.assertEqual(self.db.execute('select state from artifact_tickets').fetchone(),('WRITING',))
        self.assertEqual(self.source.used(),640512)
    async def test_bad_hidden_frame_closes_without_receipt(self):
        self.bridge.require_current(self.spec)
        with self.assertRaises(ValueError):await self.bridge.exchange_hidden(b'bad')
        with sqlite3.connect(self.root/'receiver.db') as c:
            self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone(),(0,))
    async def test_generation_change_and_wrong_journal_rejected(self):
        first=self.bridge.reservation_generation(self.spec)
        self.assertNotEqual(first,self.bound.expected_peer.connection_generation)
        with self.assertRaises(ValueError):self.bridge.validate_journal(self.sender)
        self.result_writer.unique=b'replaced-binding'
        with self.assertRaises(ValueError):self.bridge.require_current(self.spec)
    async def test_final_fence_failure_keeps_result_unpublished(self):
        await self.send_hidden();spool,digest=self.spool()
        @contextmanager
        def denied():raise PermissionError('revoked before commit');yield
        with self.assertRaises(PermissionError):
            await self.bridge.publish_result(self.spec,spool,self.artifacts,'result-key',digest,lambda:None,denied)
        self.assertEqual(self.artifacts._metadata('owner',self.spec.request)[0].state,'STORED')
        self.assertEqual(self.db.execute('select state from artifact_tickets').fetchone(),('WRITING',))

    async def test_result_start_failure_cancels_and_joins_other_start(self):
        await self.send_hidden();spool,digest=self.spool()
        entered=asyncio.Event();release=asyncio.Event();cancelled=asyncio.Event()
        async def blocked_drain():
            entered.set()
            try:await release.wait()
            except asyncio.CancelledError:cancelled.set();raise
        async def fail_start():
            await entered.wait();raise RuntimeError('start failed')
        self.result_writer.drain=blocked_drain
        try:
            with patch.object(self.result_send,'start',side_effect=fail_start):
                with self.assertRaises(RuntimeError):await self.publish(spool,digest)
            self.assertTrue(cancelled.is_set(),'the other start task must be cancelled and joined')
        finally:release.set()

    async def test_uncertain_file_close_never_retries_same_descriptor(self):
        await self.send_hidden();spool,digest=self.spool();closed=[];original=os.close
        def uncertain(fd):
            try:path=os.readlink('/proc/self/fd/'+str(fd))
            except OSError:path=''
            if path.endswith('/.pending-0.bin') or fd in closed:
                closed.append(fd)
                if len(closed)==1:original(fd);raise OSError('close outcome uncertain')
            return original(fd)
        with patch('cluster_p1.async_role_bridge.os.close',side_effect=uncertain):
            with self.assertRaises(OSError):await self.publish(spool,digest)
        self.assertEqual(len(closed),1)

    async def test_wrong_spool_token_binding_rejected_before_artifact_reserve(self):
        await self.send_hidden();spool,digest=self.spool(token_digest='0'*64)
        with self.assertRaises(ValueError):await self.publish(spool,digest)
        self.assertEqual(self.db.execute('select count(*) from artifact_tickets').fetchone(),(0,))

    async def test_capacity_mutation_and_unprepared_ingress_rejected(self):
        self.hidden_recv.capacity_tiles=2
        with self.assertRaises(ValueError):self.bridge.require_current(self.spec)
        self.hidden_recv.capacity_tiles=1
        self.result_send.capacity_bytes=32768+49
        with self.assertRaises(ValueError):self.bridge.require_current(self.spec)
        self.result_send.capacity_bytes=32768+48
        self.hidden_recv._receipt_state='preparing'
        with self.assertRaises(ValueError):self.bridge.require_current(self.spec)

    async def test_artifact_commit_lost_response_keeps_history_and_no_resend(self):
        await self.send_hidden();spool,digest=self.spool();original=self.artifacts._commit_published
        def lost(*args,**kwargs):
            original(*args,**kwargs);raise OSError('artifact commit response lost')
        with patch.object(self.artifacts,'_commit_published',side_effect=lost):
            with self.assertRaises(OSError):await self.publish(spool,digest)
        self.assertEqual(self.artifacts._metadata('owner',self.spec.request)[0].state,'RESULT_STORED')
        count=len(self.result_send.w.frames)
        with self.assertRaises(ValueError):await self.publish(spool,digest)
        self.assertEqual(len(self.result_send.w.frames),count)
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))
        self.assertEqual((self.source.record('result-key')['retired'],self.destination.record('result-key')['retired']),(1,1))

    async def test_raw_writer_close_failure_still_joins_other_connections(self):
        original=self.result_writer.close;calls=[]
        def bad_close():
            calls.append(1)
            if len(calls)==1:raise OSError('raw writer close unknown')
            return original()
        with patch.object(self.result_writer,'close',side_effect=bad_close):
            with self.assertRaises(BaseExceptionGroup):await self.bridge.close()
        self.assertTrue(all(c._closed and c._task.done() for c in self.channels))
        self.assertEqual(self.bridge._tasks,set())

    async def test_corrupted_result_wire_is_never_published(self):
        await self.send_hidden();spool,digest=self.spool();original=self.result_send.w.write
        def corrupted(raw):
            if raw[:1]==b'D':raw=raw[:-1]+bytes([raw[-1]^1])
            original(raw)
        with patch.object(self.result_send.w,'write',side_effect=corrupted):
            with self.assertRaises((ChannelError,PermissionError,BaseExceptionGroup)) as raised:
                await self.publish(spool,digest)
        def leaves(error):
            if isinstance(error,BaseExceptionGroup):
                return [leaf for item in error.exceptions for leaf in leaves(item)]
            return [error]
        self.assertTrue(all(isinstance(e,(ChannelError,PermissionError)) for e in leaves(raised.exception)))
        self.assertEqual(self.artifacts._metadata('owner',self.spec.request)[0].state,'STORED')
        self.assertEqual(self.db.execute('select state from artifact_tickets').fetchone(),('WRITING',))
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))

    def factory(self,*args):return fixtures.Tests.factory(self,*args)

    async def test_main_one_token_real_async_role_branch(self):
        self.policy=replace(self.policy,host_capacity=524288)
        result=await fixtures.Tests.execute(self,conduit=self.bridge)
        chunks=[]
        self.assertEqual(self.artifacts.fetch('owner',self.spec.request,lambda tile,b:chunks.append(b)),result['manifest_digest'])
        self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*151936)
        self.assertEqual(self.sender.query(0)['state'],'OUTPUT_COMMITTED')
        self.assertFalse(result['execution_allowed'])
        self.assertEqual(sum(f[:1]==b'D' for f in self.result_send.w.frames),19)

    async def test_transport_buffer_accounting_and_old_host_budget_denial(self):
        self.assertEqual(self.bridge.transport_buffer_bytes(self.spec),65536*2+(32768+48)*2)
        self.assertEqual(self.policy.host_capacity,262144)
        with self.assertRaises(fixtures.Denied):
            await fixtures.Tests.execute(self,conduit=self.bridge)
        self.assertEqual(self.loader.calls,[]);self.assertEqual(self.calls,[])
        self.assertEqual(self.resources.requests,[])
        self.assertEqual((self.source.used(),self.destination.used()),(0,0))


if __name__=='__main__':unittest.main()
