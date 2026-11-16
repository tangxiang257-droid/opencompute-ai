"""Real SQLite + bounded memory Channels; fabricated providers are not authority."""
import asyncio
from contextlib import contextmanager
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from cluster_p1.channel import Channel, Grant
from cluster_p1.online_issuance import IssuanceAuthority, RoleInstallStore
from cluster_p1.online_issuance_transport import IssuanceClient, IssuanceService
from validation.role_bridge.test_transport import streams, config, Writer
from test_p1_tile_contract_v2 import fixture

class Holds:
    def __init__(self):self.active=0;self.revoked=False
    @contextmanager
    def hold(self,*args):
        if self.revoked:raise PermissionError('synthetic revocation')
        self.active+=1
        try:yield None
        finally:self.active-=1
class Identity(Holds):
    suffix=""
    @contextmanager
    def hold(self,who):
        with super().hold(who):yield who.node_id+self.suffix
class Source:
    def check(self,*args):return None
class Completion:
    def check(self,*args):return None
    def begin(self,*args):return None
    def finish(self,*args):return None
    def quarantine(self,*args):return None

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.holds=Holds();self.identity=Identity();self.permission=Holds();self.completion=Completion()
        self.authority=IssuanceAuthority(self.root/'authority.db',namespace='f'*64,source_provider=Source(),authorize=self.holds,resources=self.holds)
        self.addCleanup(self.authority.close)
        t,s,st,tiles=fixture();sid='1'*32
        s=s.model_copy(update={'session_id':sid});st=tuple(x.model_copy(update={'session_id':sid}) for x in st)
        tiles=tuple(dict(x,session_id=sid) for x in tiles)
        self.registration=self.authority.register(t,s,st,tiles,hidden_size=24,vocab_size=32,config_sha256='c'*64,source_id='source',
            source_binding={'input_snapshot':'a'*64,'token_digest':'c'*64,'preprocessing':'d'*64})
        self.descriptor=self.authority.describe(self.registration,0,node='a',device='device-a',instance='instance-a',resource_id='promise-a')
        self.store=RoleInstallStore(self.root/'role.db',role_node='a',identity=self.holds,authorize=self.holds,resources=self.holds,device=self.holds)
        self.addCleanup(self.store.close)
        cr,cw,sr,sw=streams('a','issuer');g=Grant('t',sid,1,0,(0,1),'a'*64,2)
        self.cc=Channel(cr,cw,config('issuer'),{'issuer':{g}},capacity_bytes=32768,capacity_tiles=1)
        self.sc=Channel(sr,sw,config('a'),{'a':{g}},capacity_bytes=32768,capacity_tiles=1)
        self.kw=dict(issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=self.identity,authorize=self.permission,resources=self.holds)
        self.client=IssuanceClient(self.cc,self.store,self.descriptor,completion=self.completion,**self.kw)
        self.service=IssuanceService(self.sc,self.authority,self.registration,self.descriptor,**self.kw)
        self.addAsyncCleanup(self.cleanup)
    async def cleanup(self):await asyncio.gather(self.client.close(),self.service.close(),return_exceptions=True)
    async def exchange(self):
        return await asyncio.gather(self.client.install(),self.service.serve_once(),return_exceptions=True)
    async def test_asymmetric_original_channels_real_sqlite_accept(self):
        handle,response=await self.exchange()
        if isinstance(handle,BaseException):raise handle
        if isinstance(response,BaseException):raise response
        self.assertNotEqual(self.cc.authenticated_identity,self.sc.authenticated_identity)
        self.assertFalse(response['execution_allowed']);self.assertNotIsInstance(handle,dict)
        self.assertEqual(self.authority.query(response['intent']['command_id'])['intent'],response['intent'])
        with self.assertRaises(Exception):await self.client.install()
    async def test_second_wrapper_is_denied(self):
        with self.assertRaises(Exception):IssuanceClient(self.cc,self.store,self.descriptor,completion=self.completion,**self.kw)
    async def test_missing_completion_denies_without_network(self):
        self.cc._online_issuance_claimed=False
        with self.assertRaises(Exception):IssuanceClient(self.cc,self.store,self.descriptor,**self.kw)
        self.assertEqual(self.cc.w.frames,[])
    async def test_wrong_expected_issuer_denied(self):
        self.cc._online_issuance_claimed=False
        kw=dict(self.kw,issuer_node='other')
        with self.assertRaises(Exception):IssuanceClient(self.cc,self.store,self.descriptor,completion=self.completion,**kw)
    async def test_permission_revoked_at_response_release(self):
        original=self.cc._release;entered=asyncio.Event();resume=asyncio.Event();count=0
        async def release(size):
            nonlocal count
            count+=1
            if count==2:entered.set();await resume.wait()
            await original(size)
        self.cc._release=release
        task=asyncio.create_task(self.exchange())
        await asyncio.wait_for(entered.wait(),3);self.permission.revoked=True;resume.set()
        handle,response=await task
        self.assertIsInstance(handle,BaseException)
        self.assertFalse(self.store.install_handles)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
    async def test_provider_fences_never_span_network_wait(self):
        original=Writer.drain
        async def drain(writer):
            self.assertEqual((self.holds.active,self.identity.active,self.permission.active),(0,0,0))
            await original(writer)
        with patch.object(Writer,'drain',drain):
            handle,response=await self.exchange()
        if isinstance(handle,BaseException):raise handle
        if isinstance(response,BaseException):raise response

    async def test_wrong_challenge_cannot_accept(self):
        original=self.service.send
        async def send(body,sequence):
            if body['kind']=='issued':body=dict(body,role_challenge='0'*64)
            await original(body,sequence)
        self.service.send=send
        handle,response=await self.exchange()
        self.assertIsInstance(handle,BaseException);self.assertFalse(self.store.install_handles)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
    async def test_boolean_version_is_rejected(self):
        original=self.service.send
        async def send(body,sequence):
            if body['kind']=='issued':body=dict(body,version=True)
            await original(body,sequence)
        self.service.send=send
        handle,response=await self.exchange()
        self.assertIsInstance(handle,BaseException);self.assertFalse(self.store.install_handles)
    async def test_source_declaration_changed_in_request_denied(self):
        original=self.client.send
        async def send(body,sequence):
            body=dict(body,intent=dict(body['intent'],resource_id='different-promise'))
            await original(body,sequence)
        self.client.send=send
        handle,response=await self.exchange()
        self.assertIsInstance(handle,BaseException);self.assertIsInstance(response,BaseException)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(0,))
    async def test_committed_issued_response_lost_cannot_query_install(self):
        original=self.service.send
        async def send(body,sequence):
            if body['kind']=='issued':raise ConnectionError('lost before reply')
            await original(body,sequence)
        self.service.send=send
        handle,response=await self.exchange()
        self.assertIsInstance(handle,BaseException);self.assertFalse(self.store.install_handles)
        intent=self.client._pending.request
        history=self.authority.query(intent['command_id'])
        self.assertTrue(history['exists']);self.assertFalse(history['execution_allowed'])
        with self.assertRaises(Exception):self.store.accept(self.client._pending,history,self.cc,lambda:None)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
    async def test_accept_failure_closes_original_connection(self):
        with patch.object(self.store,'accept',side_effect=RuntimeError('local acceptance failure')):
            handle,response=await self.exchange()
        self.assertIsInstance(handle,BaseException);self.assertTrue(self.cc._task.done())
        self.assertIsNone(self.client._verified_delivery)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
    async def test_install_guard_revoked_after_accept(self):
        handle,response=await self.exchange()
        if isinstance(handle,BaseException):raise handle
        self.permission.revoked=True
        @contextmanager
        def execution(*args):yield 'owner'
        with self.assertRaises(Exception):self.store.create_sender(handle,self.root/'never-created',execution_authorize=execution)
        self.assertFalse((self.root/'never-created').exists())
    async def test_cancel_at_response_release_retains_slot_and_joins(self):
        original=self.cc._release;entered=asyncio.Event();resume=asyncio.Event();count=0
        async def release(size):
            nonlocal count
            count+=1
            if count==2:entered.set();await resume.wait()
            await original(size)
        self.cc._release=release
        client=asyncio.create_task(self.client.install());server=asyncio.create_task(self.service.serve_once())
        await asyncio.wait_for(entered.wait(),3);client.cancel()
        result=await asyncio.gather(client,server,return_exceptions=True)
        self.assertIsInstance(result[0],asyncio.CancelledError)
        self.assertTrue(self.cc._task.done());self.assertFalse(self.store.install_handles)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))

    async def test_principal_change_at_response_release_denied(self):
        original=self.cc._release;entered=asyncio.Event();resume=asyncio.Event();count=0
        async def release(size):
            nonlocal count
            count+=1
            if count==2:entered.set();await resume.wait()
            await original(size)
        self.cc._release=release
        task=asyncio.create_task(self.exchange())
        await asyncio.wait_for(entered.wait(),3);self.identity.suffix='-different-principal';resume.set()
        handle,response=await task
        self.assertIsInstance(handle,BaseException);self.assertFalse(self.store.install_handles)
        self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
    async def test_accept_context_exit_failure_cannot_export_live_handle(self):
        original=self.permission.hold
        @contextmanager
        def hold(principal,identity,binding,action):
            with original(principal,identity,binding,action):
                yield None
                if action=='accept':raise PermissionError('lost acceptance authorization exit')
        self.permission.hold=hold
        handle,response=await self.exchange()
        self.assertIsInstance(handle,BaseException);self.assertTrue(self.cc._closed)
        self.assertIsNone(self.client._verified_delivery)
        @contextmanager
        def execution(*args):yield 'owner'
        for internal in self.store.install_handles.values():
            with self.assertRaises(Exception):self.store.create_sender(internal,self.root/'forbidden',execution_authorize=execution)
        self.assertFalse((self.root/'forbidden').exists())

if __name__=='__main__':unittest.main()
