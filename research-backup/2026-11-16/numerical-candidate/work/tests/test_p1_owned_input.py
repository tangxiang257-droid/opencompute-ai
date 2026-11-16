import asyncio
import threading
import unittest
from dataclasses import replace
import test_p1_delivery_receipt as base
from cluster_p1.delivery_receipt import DeliveryAttempt
from cluster_p1.stage_ownership import StageOwner,StageBinding
from cluster_p1.channel import Delivery
from cluster_p1.envelope import decode

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from types import SimpleNamespace
        import test_p1_handoff as hf
        f=hf.Tests('test_receive_and_duplicate');f.setUp()
        self.addCleanup(f.doCleanups)
        channel=base.Channel()
        self.fixture=SimpleNamespace(fixture=f,channel=channel,
            delivery=Delivery(channel,decode(f.frame()),192))
        self.calls=[];self.commits=[]
        def operation(value,**kw):self.calls.append((value,kw));return b'out'
        self.operation=operation
    def owner(self,operation=None,binding=None):
        b=binding or StageBinding('t','s',1,1,'a'*64,'a'*64)
        o=StageOwner(b,operation or self.operation,authorize=lambda spec:True,commit=lambda spec,value:self.commits.append(spec))
        self.addAsyncCleanup(o.close);return o
    def attempt(self,owner,delivery=None):
        return DeliveryAttempt(self.fixture.fixture.receiver,delivery or self.fixture.delivery,consumer=owner)
    async def test_bound_bytes_not_replacement_and_release_after_read(self):
        o=self.owner();a=self.attempt(o);original=self.fixture.delivery.envelope.payload
        self.fixture.delivery.envelope=replace(self.fixture.delivery.envelope,payload=b'X'*192)
        self.fixture.channel.finish.set();await a.run()
        self.assertIs(self.calls[0][0],original)
        self.assertEqual(self.commits[0].input_digest,__import__('hashlib').sha256(original).hexdigest())
        self.assertEqual(a.state.consumption,'completed');self.assertEqual(self.fixture.channel.calls,1)
        self.assertIsNone(a._frame);self.assertIsNone(a._payload);self.assertIsNone(a._delivery)
    async def test_mismatched_owner_refused_before_claim(self):
        for field,value in [('epoch',2),('stage_id',0),('task_id','other'),('model_hash','b'*64),('layout_hash','b'*64)]:
            o=self.owner(binding=replace(StageBinding('t','s',1,1,'a'*64,'a'*64),**{field:value}))
            with self.subTest(field=field),self.assertRaises(ValueError):self.attempt(o)
        self.assertEqual(self.fixture.channel.calls,0)
    async def test_duplicate_does_not_compute_again(self):
        self.fixture.channel.finish.set();await self.attempt(self.owner()).run()
        d=Delivery(self.fixture.channel,decode(self.fixture.fixture.frame()),192)
        a=self.attempt(self.owner(),d);r=await a.run()
        self.assertTrue(r['duplicate']);self.assertEqual(len(self.calls),1)
        self.assertEqual(a.state.consumption,'skipped_duplicate');self.assertEqual(self.fixture.channel.calls,2)
    async def test_cancel_holds_credit_until_operation_returns(self):
        loop=asyncio.get_running_loop();entered=asyncio.Event();finish=threading.Event()
        def op(value,**kw):
            self.calls.append(value);loop.call_soon_threadsafe(entered.set)
            if not finish.wait(3):raise RuntimeError('barrier timeout')
            return b'late'
        o=self.owner(op);a=self.attempt(o);t=asyncio.create_task(a.run());await entered.wait()
        try:
            t.cancel();await asyncio.sleep(0);t.cancel();await asyncio.sleep(0)
            self.assertFalse(t.done());self.assertEqual(self.fixture.channel.calls,0)
        finally:finish.set();self.fixture.channel.finish.set()
        with self.assertRaises(asyncio.CancelledError):await t
        self.assertEqual(self.commits,[]);self.assertEqual(self.fixture.channel.calls,1)
        self.assertEqual(a.state.consumption,'unconfirmed');self.assertEqual(o.state.status,'retired')
    async def test_commit_failure_receipt_does_not_authorize_retry(self):
        o=self.owner()
        def fail(*args):raise RuntimeError('commit')
        o._commit=fail;self.fixture.channel.finish.set();a=self.attempt(o)
        with self.assertRaises(RuntimeError):await a.run()
        self.assertEqual(a.state.registration,'recorded');self.assertEqual(a.state.consumption,'unconfirmed')
        self.assertEqual(self.fixture.channel.calls,1)
        d=Delivery(self.fixture.channel,decode(self.fixture.fixture.frame()),192)
        r=await self.attempt(self.owner(),d).run()
        self.assertTrue(r['duplicate']);self.assertEqual(len(self.calls),1)
    async def test_cancel_after_receipt_before_consume_keeps_gap_explicit(self):
        from unittest.mock import patch
        receiver=self.fixture.fixture.receiver;original=receiver.receive_bytes
        def cancel_after(frame):
            result=original(frame);asyncio.current_task().cancel();return result
        o=self.owner();a=self.attempt(o);self.fixture.channel.finish.set()
        with patch.object(receiver,'receive_bytes',side_effect=cancel_after):
            with self.assertRaises(asyncio.CancelledError):await a.run()
        self.assertEqual(self.calls,[]);self.assertEqual(o.state.status,'retired')
        self.assertEqual(a.state.registration,'recorded');self.assertEqual(a.state.consumption,'unconfirmed')
        d=Delivery(self.fixture.channel,decode(self.fixture.fixture.frame()),192)
        later=self.attempt(self.owner(),d);await later.run()
        self.assertEqual(later.state.consumption,'skipped_duplicate');self.assertEqual(self.calls,[])
    async def test_release_does_not_revoke_external_immutable_reference(self):
        retained=self.fixture.delivery.envelope.payload
        self.fixture.channel.finish.set();a=self.attempt(self.owner());await a.run()
        self.assertEqual(retained,b'\0'*192)
        self.assertIsNone(a._payload)
    async def test_cancel_worker_error_then_release_error_preserves_chain(self):
        loop=asyncio.get_running_loop();entered=asyncio.Event();finish=threading.Event()
        worker=ValueError('worker');root=LookupError('worker original cause');worker.__cause__=root
        cleanup=RuntimeError('release')
        def op(value,**kw):
            loop.call_soon_threadsafe(entered.set)
            if not finish.wait(3):raise RuntimeError('barrier timeout')
            raise worker
        o=self.owner(op);a=self.attempt(o)
        self.fixture.channel.failure=cleanup
        t=asyncio.create_task(a.run());await entered.wait();t.cancel();await asyncio.sleep(0)
        finish.set();self.fixture.channel.finish.set()
        with self.assertRaises(asyncio.CancelledError) as caught:await t
        def includes(error,wanted,seen=None):
            seen=set() if seen is None else seen
            if error is wanted:return True
            if error is None or id(error) in seen:return False
            seen.add(id(error))
            return any(includes(x,wanted,seen) for x in (error.__cause__,error.__context__,*getattr(error,'exceptions',())))
        self.assertTrue(includes(caught.exception,worker))
        self.assertTrue(includes(caught.exception,cleanup))
        self.assertTrue(includes(caught.exception,root))
        self.assertIs(worker.__cause__,root)
        self.assertEqual(self.fixture.channel.calls,1);self.assertEqual(o.state.status,'retired')
