import asyncio
import unittest
from cluster_p1.channel import Channel, Grant
from cluster_p1.envelope import Envelope
from cluster_p1.handoff import MAX_FRAME_BYTES

class Tests(unittest.IsolatedAsyncioTestCase):
    def envelope(self,n,**kw):
        d=dict(task_id='t',session_id='s',execution_epoch=1,stage_id=0,tile_id=0,
               token_range=(0,1),model_hash='a'*64,layout_version=2,
               tensor_shape=(n,),dtype='uint8',payload=b'x'*n)
        d.update(kw);return Envelope(**d)
    def channel(self):
        # No sockets: exercise real Channel.send/reservation with a recording wire sink.
        c=object.__new__(Channel);c.grants=(Grant('t','s',1,0,(0,1),'a'*64,2),)
        c._condition=asyncio.Condition();c._closed=False;c._peer_capacity=(65536,1)
        c._credits_bytes=65536;c._credits_tiles=1;c.timeout=1
        c.frames=[]
        async def frame(kind,body):c.frames.append((kind,body))
        c._frame=frame
        return c
    async def test_65536_payload_refused_before_credit(self):
        c=self.channel(); e=self.envelope(65536)
        self.assertGreater(len(e.encode()),MAX_FRAME_BYTES)
        with self.assertRaises(ValueError):
            await c.send(e,max_frame_bytes=MAX_FRAME_BYTES)
        self.assertEqual((c._credits_bytes,c._credits_tiles,c.frames),(65536,1,[]))
    async def test_exact_frame_boundary_and_one_over(self):
        e=self.envelope(65000); n=65000+65536-len(e.encode());e=self.envelope(n)
        self.assertEqual(len(e.encode()),65536)
        c=self.channel();await c.send(e,max_frame_bytes=65536)
        self.assertEqual(c.frames,[(b'D',e.encode())])
        c=self.channel()
        with self.assertRaises(ValueError):await c.send(self.envelope(n+1),max_frame_bytes=65536)
        self.assertEqual(c.frames,[]);self.assertEqual(c._credits_tiles,1)
    async def test_header_growth_counts(self):
        e=self.envelope(65000);n=65000+65536-len(e.encode())
        c=self.channel()
        # Token/header numeric width and identifier bytes must count, not fixed overhead.
        e=self.envelope(n,task_id='t'*128)
        c.grants=(Grant('t'*128,'s',1,0,(0,1),'a'*64,2),)
        with self.assertRaises(ValueError):await c.send(e,max_frame_bytes=65536)
        self.assertEqual(c.frames,[]);self.assertEqual(c._credits_tiles,1)

    async def test_handoff_wrapper_uses_receiver_limit(self):
        from cluster_p1.handoff import send_handoff
        c=self.channel()
        with self.assertRaises(ValueError):await send_handoff(c,self.envelope(65536))
        self.assertEqual((c._credits_tiles,c.frames),(1,[]))
        e=self.envelope(4);await send_handoff(c,e)
        self.assertEqual(c.frames,[(b'D',e.encode())])
    async def test_bad_bounds_do_not_consume_credit(self):
        for bound in (True,0,-1,1.5,2**40):
            c=self.channel()
            with self.assertRaises(ValueError):await c.send(self.envelope(4),max_frame_bytes=bound)
            self.assertEqual((c._credits_tiles,c.frames),(1,[]))
    async def test_float32_largest_fitting_payload_not_fixed_subtraction(self):
        def fp(n):return self.envelope(n*4,tensor_shape=(1,1,n),dtype='float32')
        n=16384
        while len(fp(n).encode())>65536:n-=1
        c=self.channel();await c.send(fp(n),max_frame_bytes=65536)
        self.assertEqual(c.frames,[(b'D',fp(n).encode())])
        c=self.channel()
        with self.assertRaises(ValueError):await c.send(fp(n+1),max_frame_bytes=65536)
        self.assertEqual((c._credits_tiles,c.frames),(1,[]))
    async def test_serialized_bytes_frozen_before_credit_wait(self):
        c=self.channel();c._credits_tiles=0;e=self.envelope(4);original=e.encode()
        started=asyncio.Event();old=c._condition.wait
        async def wait():started.set();return await old()
        c._condition.wait=wait
        task=asyncio.create_task(c.send(e,max_frame_bytes=65536))
        await started.wait()
        object.__setattr__(e,'payload',b'y'*4)
        async with c._condition:
            c._credits_tiles=1;c._condition.notify_all()
        await task
        self.assertEqual(c.frames,[(b'D',original)])
    async def test_wrapper_rejects_non_channel(self):
        from cluster_p1.handoff import send_handoff
        with self.assertRaises(TypeError):await send_handoff(object(),self.envelope(4))
    async def test_dynamic_encode_cannot_bypass_grant(self):
        from cluster_p1.channel import ChannelError
        c=self.channel();e=self.envelope(4)
        rogue=self.envelope(4,task_id='unauthorized').encode()
        object.__setattr__(e,'encode',lambda:rogue)
        with self.assertRaises(ChannelError):await c.send(e,max_frame_bytes=65536)
        self.assertEqual((c._credits_tiles,c.frames),(1,[]))
    async def test_dynamic_encode_credit_counts_actual_wire(self):
        c=self.channel();e=self.envelope(4);wire=self.envelope(400).encode()
        object.__setattr__(e,'encode',lambda:wire)
        await c.send(e,max_frame_bytes=65536)
        self.assertEqual(c._credits_bytes,65536-400)
        self.assertEqual(c.frames,[(b'D',wire)])
    async def test_dynamic_encode_malformed_wire_refused(self):
        c=self.channel();e=self.envelope(4)
        object.__setattr__(e,'encode',lambda:b'bad')
        with self.assertRaises(ValueError):await c.send(e,max_frame_bytes=65536)
        self.assertEqual((c._credits_tiles,c.frames),(1,[]))
    async def test_subclass_encoded_identity_is_authority_not_instance(self):
        from cluster_p1.channel import ChannelError
        wire=self.envelope(4,session_id='wrong').encode()
        class Spoof(Envelope):
            def encode(self):return wire
        e=Spoof(**self.envelope(4).__dict__);c=self.channel()
        with self.assertRaises(ChannelError):await c.send(e)
        self.assertEqual((c._credits_tiles,c.frames),(1,[]))
    async def test_dynamic_encode_requires_immutable_bytes(self):
        for wire in (bytearray(self.envelope(4).encode()),memoryview(b'a'),'bad',None):
            c=self.channel();e=self.envelope(4)
            object.__setattr__(e,'encode',lambda:wire)
            with self.assertRaises(ValueError):await c.send(e)
            self.assertEqual((c._credits_tiles,c.frames),(1,[]))
    async def test_generic_default_keeps_larger_payload_policy(self):
        c=self.channel();e=self.envelope(65536)
        await c.send(e)
        self.assertEqual(c.frames,[(b'D',e.encode())])
        self.assertEqual((c._credits_bytes,c._credits_tiles),(0,0))
    async def test_wire_payload_exceeds_peer_capacity_before_debit(self):
        from cluster_p1.channel import ChannelError
        c=self.channel();c._peer_capacity=(8,1);c._credits_bytes=8;e=self.envelope(4)
        object.__setattr__(e,'encode',lambda:self.envelope(12).encode())
        with self.assertRaises(ChannelError):await c.send(e)
        self.assertEqual((c._credits_bytes,c._credits_tiles,c.frames),(8,1,[]))
