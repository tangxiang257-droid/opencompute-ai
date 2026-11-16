import asyncio
import unittest
from unittest.mock import patch
import test_p1_handoff as hf
from cluster_p1.channel import Delivery
from cluster_p1.envelope import decode
from cluster_p1.delivery_receipt import DeliveryAttempt

class Channel:
    def __init__(self):self.calls=0;self.enter=asyncio.Event();self.finish=asyncio.Event();self.failure=None
    async def _release(self,size):
        self.calls+=1;self.enter.set();await self.finish.wait()
        if self.failure:raise self.failure

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Only reuse fixture helpers, no channel/network/node construction.
        self.fixture=hf.Tests('test_receive_and_duplicate');self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.channel=Channel();self.delivery=Delivery(self.channel,decode(self.fixture.frame()),192)
        self.attempt=DeliveryAttempt(self.fixture.receiver,self.delivery)
    async def test_normal_once(self):
        self.channel.finish.set();r=await self.attempt.run()
        self.assertFalse(r['duplicate']);self.assertEqual(self.channel.calls,1)
        self.assertEqual(self.attempt.state.release,'completed')
        with self.assertRaises(RuntimeError):await self.attempt.run()
        self.assertEqual(self.channel.calls,1)
    async def test_registration_error_still_releases(self):
        self.channel.finish.set();e=ValueError('injected')
        with patch.object(self.fixture.receiver,'receive_bytes',side_effect=e):
            with self.assertRaises(ValueError) as caught:await self.attempt.run()
        self.assertIs(caught.exception,e);self.assertEqual(self.channel.calls,1)
        self.assertEqual(self.attempt.state.registration,'unconfirmed')
    async def test_cancel_waits_for_release_and_preserves_receipt(self):
        t=asyncio.create_task(self.attempt.run());await self.channel.enter.wait()
        t.cancel();await asyncio.sleep(0);t.cancel();await asyncio.sleep(0)
        self.assertFalse(t.done());self.channel.finish.set()
        with self.assertRaises(asyncio.CancelledError):await t
        self.assertEqual(self.channel.calls,1);self.assertEqual(self.attempt.state.registration,'recorded')
        self.assertEqual(self.attempt.state.release,'completed')
    async def test_release_failure_not_retried(self):
        self.channel.failure=RuntimeError('release failed');self.channel.finish.set()
        with self.assertRaises(RuntimeError):await self.attempt.run()
        self.assertEqual(self.channel.calls,1);self.assertEqual(self.attempt.state.release,'unconfirmed')
        with self.assertRaises(RuntimeError):await self.attempt.run()
    async def test_double_claim(self):
        second=DeliveryAttempt(self.fixture.receiver,self.delivery)
        t=asyncio.create_task(self.attempt.run());await self.channel.enter.wait()
        try:
            with self.assertRaises(RuntimeError):await second.run()
        finally:self.channel.finish.set();await t
        self.assertEqual(self.channel.calls,1)
    async def test_independent_duplicate_delivery_each_released(self):
        self.channel.finish.set();await self.attempt.run()
        d=Delivery(self.channel,decode(self.fixture.frame()),192)
        r=await DeliveryAttempt(self.fixture.receiver,d).run()
        self.assertTrue(r['duplicate']);self.assertEqual(self.channel.calls,2)
    async def test_snapshot_does_not_follow_replaced_envelope(self):
        from dataclasses import replace
        self.delivery.envelope=replace(self.delivery.envelope,task_id='other')
        self.channel.finish.set()
        self.assertFalse((await self.attempt.run())['duplicate'])
    async def test_mutable_fields_rejected_before_claim(self):
        from dataclasses import replace
        e=replace(self.delivery.envelope,tensor_shape=[1,2,24])
        d=Delivery(self.channel,e,192)
        with self.assertRaises(TypeError):DeliveryAttempt(self.fixture.receiver,d)
        self.assertFalse(d._released);self.assertFalse(hasattr(d,'_p1_receipt_claimed'))
    async def test_equivalent_noncanonical_frames_semantic_duplicate(self):
        import hashlib,json
        from cluster_p1.envelope import PREFIX
        canonical=self.fixture.frame();_,_,n,p=PREFIX.unpack(canonical[:PREFIX.size])
        header=json.dumps(json.loads(canonical[PREFIX.size:PREFIX.size+n]),indent=2).encode()
        raw=PREFIX.pack(b'TENV',1,len(header),p)+header+canonical[PREFIX.size+n:-32]
        wire=raw+hashlib.sha256(raw).digest()
        self.assertNotEqual(wire,canonical)
        self.channel.finish.set();await self.attempt.run()
        d=Delivery(self.channel,decode(wire),192)
        self.assertTrue((await DeliveryAttempt(self.fixture.receiver,d).run())['duplicate'])
    async def test_closed_journal_rejects_but_releases(self):
        self.fixture.j.close();self.channel.finish.set()
        with self.assertRaises(ValueError):await self.attempt.run()
        self.assertEqual(self.channel.calls,1);self.assertEqual(self.attempt.state.release,'completed')
    async def test_primary_and_cleanup_failure_are_both_preserved(self):
        primary=ValueError('registration');cleanup=RuntimeError('release')
        self.channel.failure=cleanup;self.channel.finish.set()
        with patch.object(self.fixture.receiver,'receive_bytes',side_effect=primary):
            with self.assertRaises(ValueError) as caught:await self.attempt.run()
        self.assertIs(caught.exception,primary);self.assertIs(caught.exception.__cause__,cleanup)
        self.assertEqual(self.attempt.state.registration,'unconfirmed')
        self.assertEqual(self.attempt.state.release,'unconfirmed')
    async def test_cancellation_preserves_primary_and_release_faults(self):
        primary=ValueError('registration');cleanup=RuntimeError('release')
        cause=LookupError('original cause');primary.__cause__=cause
        self.channel.failure=cleanup
        with patch.object(self.fixture.receiver,'receive_bytes',side_effect=primary):
            t=asyncio.create_task(self.attempt.run());await self.channel.enter.wait()
            t.cancel();await asyncio.sleep(0);self.channel.finish.set()
            with self.assertRaises(asyncio.CancelledError) as caught:await t
        def includes(root,wanted,seen=None):
            seen=set() if seen is None else seen
            if root is wanted:return True
            if root is None or id(root) in seen:return False
            seen.add(id(root))
            return any(includes(x,wanted,seen) for x in (root.__cause__,root.__context__,*getattr(root,'exceptions',())))
        self.assertTrue(includes(caught.exception,primary))
        self.assertTrue(includes(caught.exception,cleanup))
        self.assertIs(primary.__cause__,cause)
        self.assertEqual(self.channel.calls,1)
        self.assertEqual(self.attempt.state.registration,'unconfirmed')
        self.assertEqual(self.attempt.state.release,'unconfirmed')
