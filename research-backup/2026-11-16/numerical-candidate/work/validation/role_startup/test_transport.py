"""Unstarted means no local H; Channel still owns a running reader task.
All streams and TLS metadata here are ordinary memory doubles, never sockets.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from cluster_p1 import channel as transport
from validation.role_bridge.test_transport import streams, config

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.reader,self.writer,self.peer_reader,self.peer_writer=streams('a','b')
        self.channels=[]
    async def asyncTearDown(self):
        await asyncio.gather(*(c.close() for c in self.channels),return_exceptions=True)
        self.peer_writer.close();await self.peer_writer.wait_closed()
        self.writer.close();await self.writer.wait_closed()
    async def unstarted(self,**kw):
        c=await transport._accept_unstarted(self.reader,self.writer,config('b'),{},**kw)
        self.channels.append(c);return c
    async def test_supplied_stream_constructs_without_H(self):
        c=await self.unstarted()
        self.assertEqual(self.writer.frames,[]);self.assertFalse(c._start_attempted)
        self.assertFalse(c._task.done())
        await c.start();self.assertTrue(c._start_attempted)
        self.assertEqual([f[:1] for f in self.writer.frames],[b'H'])
    async def test_duplicate_start_denies_without_second_H_or_abort(self):
        c=await self.unstarted();await c.start()
        with self.assertRaises(transport.ChannelError):await c.start()
        self.assertFalse(c._closed);self.assertEqual(len(self.writer.frames),1)
    async def test_concurrent_start_claims_before_drain(self):
        c=await self.unstarted();self.writer.block_kind=b'H'
        first=asyncio.create_task(c.start());await self.writer.entered.wait()
        with self.assertRaises(transport.ChannelError):await c.start()
        self.assertFalse(first.done());self.writer.resume.set();await first
        self.assertEqual(len(self.writer.frames),1)
    async def test_cancel_drain_burns_start_and_aborts(self):
        c=await self.unstarted();self.writer.block_kind=b'H'
        first=asyncio.create_task(c.start());await self.writer.entered.wait();first.cancel()
        with self.assertRaises(asyncio.CancelledError):await first
        self.assertTrue(c._start_attempted);self.assertTrue(c._closed)
        with self.assertRaises(transport.ChannelError):await c.start()
        self.assertEqual(len(self.writer.frames),1)
    async def test_B_gate_denial_does_not_consume_future_ready_start(self):
        c=await self.unstarted(_gate_receipt=True)
        with self.assertRaises(transport.ChannelError):await c.start()
        self.assertFalse(c._start_attempted);self.assertFalse(c._receipt_h_started)
        self.assertEqual(self.writer.frames,[])
        # Explicit state substitute; integration must use real PreparedReceipt.
        c._receipt_state='ready';await c.start()
        self.assertTrue(c._receipt_h_started);self.assertTrue(c._start_attempted)
        with self.assertRaises(transport.ChannelError):await c.start()
        self.assertEqual(len(self.writer.frames),1)
    async def test_private_dial_is_unstarted_and_legacy_dial_started(self):
        opened=AsyncMock(return_value=(self.reader,self.writer));context=object()
        with patch.object(transport.asyncio,'open_connection',opened),patch.object(transport.TLSConfig,'context',return_value=context):
            c=await transport._connect_unstarted('not-used.invalid',443,config('b'),{},'not-used.invalid')
        self.channels.append(c)
        opened.assert_awaited_once_with('not-used.invalid',443,ssl=context,server_hostname='not-used.invalid')
        self.assertFalse(c._start_attempted);self.assertEqual(self.writer.frames,[])
        await c.close()
        r,w,pr,pw=streams('a','b')
        try:
            with patch.object(transport.asyncio,'open_connection',AsyncMock(return_value=(r,w))),patch.object(transport.TLSConfig,'context',return_value=context):
                legacy=await transport.connect('not-used.invalid',443,config('b'),{},'not-used.invalid')
            self.channels.append(legacy);self.assertTrue(legacy._start_attempted)
            self.assertEqual([f[:1] for f in w.frames],[b'H'])
        finally:pw.close();await pw.wait_closed()
    async def test_constructor_failure_closes_supplied_writer(self):
        with self.assertRaises(transport.ChannelError):await transport._accept_unstarted(self.reader,self.writer,config('other'),{})
        self.assertTrue(self.writer.closed)
    async def test_legacy_accept_still_sends_one_H(self):
        c=await transport.accept(self.reader,self.writer,config('b'),{})
        self.channels.append(c);self.assertTrue(c._start_attempted)
        self.assertEqual([f[:1] for f in self.writer.frames],[b'H'])

    async def test_failed_helper_joins_reader_even_if_writer_close_raises(self):
        c=await self.unstarted();original=self.writer.close;waited=[]
        def close():original();raise OSError('uncertain close')
        async def wait_closed():waited.append(True)
        self.writer.close=close;self.writer.wait_closed=wait_closed
        try:
            with self.assertRaises(BaseExceptionGroup):await transport._failed_owned_stream(RuntimeError('primary'),c,self.writer,1)
            self.assertTrue(c._task.done());self.assertEqual(waited,[True])
        finally:self.writer.close=original
    async def test_constructor_cleanup_survives_repeated_cancellation(self):
        entered=asyncio.Event();resume=asyncio.Event()
        async def wait_closed():entered.set();await resume.wait()
        self.writer.wait_closed=wait_closed
        task=asyncio.create_task(transport._accept_unstarted(self.reader,self.writer,config('wrong'),{}))
        await entered.wait();task.cancel()
        loop=asyncio.get_running_loop();loop.call_soon(task.cancel);loop.call_soon(resume.set)
        with self.assertRaises(BaseExceptionGroup):await task
        self.assertTrue(self.writer.closed)
    async def test_existing_receipt_advertisement_marker_is_not_ignored(self):
        c=await self.unstarted(_gate_receipt=True)
        c._receipt_state='ready';c._receipt_h_started=True
        with self.assertRaises(transport.ChannelError):await c.start()
        self.assertEqual(self.writer.frames,[])

if __name__=='__main__':unittest.main()
