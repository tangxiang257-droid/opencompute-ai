"""Independent negative startup tests; ordinary memory bytes and synthetic providers."""
import asyncio
from contextlib import contextmanager
import unittest
from unittest.mock import patch
from cluster_p1.role_startup import RoleStartup
from cluster_p1.online_issuance_transport import IssuanceClient
from validation.online_issuance import test_transport as fixture
from validation.role_bridge.test_transport import streams,config

class RuntimeDouble:
    def __init__(self):self.calls=[]
    def forbidden(self,*args):self.calls.append(args);raise AssertionError('runtime must not run')
    verify_stage=quarantine=hold_load=open_scope=require_open=materialize=finish_scope=require_finished=load=hold_stage0=forbidden

class Tests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=fixture.Tests.asyncSetUp
    cleanup=fixture.Tests.cleanup
    async def create(self,**changes):
        self.reader,self.writer,self.pr,self.pw=streams('a','issuer')
        self.addAsyncCleanup(self.close_peer)
        self.runtime_double=RuntimeDouble()
        self.store.resources.hold_control=self.synthetic_mapping
        runtime=dict(instance=self.descriptor['instance'],layer_range=(0,1),authority=self.runtime_double,
            resources=self.runtime_double,ticket=object(),device=self.runtime_double,loader=self.runtime_double,factories=None)
        kw=dict(self.kw,completion=self.completion,runtime=runtime);kw.update(changes)
        self.startup=await RoleStartup.create(self.reader,self.writer,config('issuer'),{},self.store,self.descriptor,**kw)
        async def cleanup_startup():await asyncio.gather(self.startup.close(),return_exceptions=True)
        self.addAsyncCleanup(cleanup_startup)
        return self.startup
    @contextmanager
    def synthetic_mapping(self,*args):yield None
    async def close_peer(self):self.pw.close();await self.pw.wait_closed()
    def assert_uninstalled(self):
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM installs').fetchone(),(0,))
        self.assertEqual(self.runtime_double.calls,[])
    def assert_rejected_before_H(self):
        self.assertTrue(self.writer.closed);self.assertEqual(self.writer.frames,[]);self.assert_uninstalled()
    async def test_missing_identity_closes_writer_zero_H(self):
        with self.assertRaises(Exception):await self.create(identity=None)
        self.assert_rejected_before_H()
    async def test_missing_resources_closes_writer_zero_H(self):
        with self.assertRaises(Exception):await self.create(resources=None)
        self.assert_rejected_before_H()
    async def test_missing_completion_closes_writer_zero_H(self):
        with self.assertRaises(Exception):await self.create(completion=None)
        self.assert_rejected_before_H()
    async def test_missing_runtime_closes_writer_zero_H(self):
        with self.assertRaises(Exception):await self.create(runtime=None)
        self.assert_rejected_before_H()
    async def test_malformed_runtime_closes_writer_zero_H(self):
        with self.assertRaises(Exception):await self.create(runtime={'instance':'wrong'})
        self.assert_rejected_before_H()
    async def test_missing_store_hold_control_closes_zero_H(self):
        with patch.object(self.store,'ready',side_effect=lambda:delattr(self.store.resources,'hold_control')):
            with self.assertRaises(Exception):await self.create()
        self.assert_rejected_before_H()
    async def test_create_retains_exact_original_connection_until_close(self):
        s=await self.create()
        self.assertIs(s.channel.r,self.reader);self.assertIs(s.channel.w,self.writer)
        self.assertIs(s.client.channel,s.channel);self.assertEqual(self.writer.frames,[])
        self.assertFalse(s.channel._task.done())
        await s.close();self.assertTrue(s.channel._task.done());self.assertTrue(self.writer.closed)
        self.assert_uninstalled()
    async def test_install_preflight_failure_closes_reader_no_H(self):
        s=await self.create();s.runtime['device']=None
        with self.assertRaises(Exception):await s.install(self.root/'never.db',execution_authorize=lambda:None)
        self.assertTrue(s.channel._task.done());self.assert_rejected_before_H()
    async def test_already_started_issuance_rejects_without_pending(self):
        s=await self.create();await s.channel.start()
        with self.assertRaises(Exception):await s.install(self.root/'never.db',execution_authorize=lambda:None)
        self.assertEqual(sum(x[:1]==b'H' for x in self.writer.frames),1)
        self.assertTrue(s.channel._task.done());self.assertTrue(self.writer.closed);self.assert_uninstalled()
    async def test_constructor_failure_after_channel_joins_reader(self):
        from cluster_p1 import role_startup as module
        original=module._accept_unstarted;seen=[]
        async def capture(*args,**kw):
            c=await original(*args,**kw);seen.append(c);return c
        with patch.object(module,'_accept_unstarted',capture):
            with self.assertRaises(Exception):await self.create(issuer_node='wrong')
        self.assertEqual(len(seen),1);self.assertTrue(seen[0]._task.done());self.assert_rejected_before_H()

    async def test_close_writer_error_still_joins_reader(self):
        s=await self.create();original=self.writer.close
        def close():original();raise OSError('synthetic writer close failure')
        self.writer.close=close
        try:
            with self.assertRaises(BaseException):await s.close()
            self.assertTrue(s.channel._task.done());self.assertTrue(self.writer.closed)
        finally:self.writer.close=original
    async def test_constructed_client_rejects_already_started_channel(self):
        s=await self.create();await s.channel.start()
        with self.assertRaises(Exception):
            IssuanceClient(s.channel,self.store,self.descriptor,completion=self.completion,**self.kw)
        self.assertEqual(sum(x[:1]==b'H' for x in self.writer.frames),1)
        self.assert_uninstalled()

if __name__=='__main__':unittest.main()
