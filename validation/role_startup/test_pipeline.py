"""Online-issued full chain; no shared NonceCoordinator or real model/network."""
import asyncio
from dataclasses import replace
import hashlib,json,os,sqlite3,struct,tempfile
from pathlib import Path
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from cluster_p1.contracts import Task,Session,Stage
from cluster_p1.projection_driver import Spec,Policy
from cluster_p1.byte_spool import Book
from cluster_p1.channel import Channel,Grant
from cluster_p1.prepared_receipt import PreparedReceiptConnection
from result_artifacts import _Ledger,Limits
from token_input_snapshot import InputBinding
from validation.split_roles import test_pipeline as split
from validation.one_token import test_assembly as fixtures
from validation.role_bridge import test_transport as wire

from validation.online_issuance.test_pipeline import Tests as Online, Holds, Sources, BootstrapResources, BootstrapDevice

class Tests(Online):
    async def online_bootstrap(self):
        from cluster_p1.role_startup import RoleStartup
        from cluster_p1.online_issuance import IssuanceAuthority,RoleInstallStore
        from cluster_p1.online_issuance_transport import IssuanceClient,IssuanceService
        t,session,stages,tiles=self.declaration_args
        authority=IssuanceAuthority(self.root/'authority.db',namespace='f'*64,source_provider=Sources(),authorize=Holds(),resources=Holds())
        self.addCleanup(authority.close)
        token_hash=hashlib.sha256(struct.pack('<q',1)).hexdigest()
        registration=authority.register(t,session,stages,tiles,hidden_size=self.hidden,vocab_size=self.vocab,config_sha256='c'*64,source_id='source-one',source_binding=dict(input_snapshot=self.task.snapshot.digest,token_digest=token_hash,preprocessing='c'*64))
        plan=json.loads(registration.body)['plan_digest']
        self.artifacts.record_pending('owner',t.task_id,session.session_id,1,plan,(('a','device-a','1'*64),('b','device-b','2'*64)))
        token_hash=hashlib.sha256(struct.pack('<q',1)).hexdigest()
        self.spec=Spec('owner',t.task_id,session.session_id,1,'a'*64,'a'*64,self.task.snapshot.digest,'c'*64,plan,token_hash,self.hidden,self.vocab,(token_hash,))
        grant=Grant(t.task_id,session.session_id,1,0,(0,1),self.spec.model,2)
        ar,aw,br,bw=wire.streams('a','b')
        self.hidden_send=Channel(ar,aw,wire.config('b'),{'b':{grant}},capacity_bytes=65536,capacity_tiles=1)
        prepared=await PreparedReceiptConnection.create(br,bw,wire.config('a'),{'a':{grant}})
        self.channels.extend((self.hidden_send,prepared.channel));self.bound=prepared.bound
        self.install_stores=[];self.install_clients=[];self.authority=authority
        for stage,node in enumerate(('a','b')):
            store=RoleInstallStore(self.root/f'install-{stage}.db',role_node=node,identity=Holds(),authorize=Holds(),resources=BootstrapResources('reservation-'+node,self.role_resources[stage]),device=Holds())
            self.addCleanup(store.close);self.install_stores.append(store)
            desc=authority.describe(registration,stage,node=node,device='device-'+node,instance=str(stage+1)*64,resource_id='reservation-'+node)
            cr,cw,sr,sw=wire.streams(node,'issuer')
            grant=Grant(t.task_id,session.session_id,1,stage,(0,1),self.spec.model,2)
            server=Channel(sr,sw,wire.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
            self.channels.append(server)
            opts=dict(identity=split.Identity(),authorize=Holds(),resources=Holds(),issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest())
            startup=await RoleStartup.create(cr,cw,wire.config('issuer'),{'issuer':{grant}},store,desc,completion=BootstrapDevice(),runtime=self.kwargs(stage),**opts)
            self.endpoints.append(startup)
            self.current_startup=startup
            client=startup.client;self.channels.append(startup.channel)
            service=IssuanceService(server,authority,registration,desc,**opts)
            self.install_clients.append(client);self.endpoints.extend((client,service))
            tasks=[asyncio.create_task(service.serve_once()),asyncio.create_task(startup.install(self.root/('sender.db' if stage==0 else 'receiver.db'),**(dict(execution_authorize=fixtures.journal_owner) if stage==0 else dict(prepared=prepared,prepare_fence=getattr(self,'prepare_fence',None)))))]
            try:response,handle=await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
            self.assertFalse(response['execution_allowed'])
            if stage==0:self.sender=handle
            else:self.receiver=handle
            self.addCleanup(handle.close)
            self.assertEqual(sum(f[:1]==b'H' for f in cw.frames),1)
            self.assertEqual(sum(f[:1]==b'H' for f in sw.frames),1)
        self.assertFalse(hasattr(self.sender,'_coordinator'))
        self.assertFalse(hasattr(self.receiver,'_coordinator'))
        self.assertIsNot(self.sender._online_issuer,self.receiver._online_issuer)


    async def test_missing_device_denies_before_issuance(self):
        original=self.kwargs
        self.kwargs=lambda stage:dict(original(stage),device=None)
        with self.assertRaises(Exception):await self.online_bootstrap()
        self.assertEqual(self.role_loaders[0].calls,[])
        self.assertEqual(self.authority.db.execute('SELECT count(*) FROM slots').fetchone(),(0,))

    async def test_b_gate_waits_for_preparation_then_cancel_retires(self):
        entered=asyncio.Event();release=asyncio.Event()
        async def fence():
            entered.set();await release.wait()
        self.prepare_fence=fence
        task=asyncio.create_task(self.online_bootstrap())
        try:
            await asyncio.wait_for(entered.wait(),5)
            self.assertEqual(sum(f[:1]==b'H' for f in self.bound._channel.w.frames),0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            self.assertEqual(sum(f[:1]==b'H' for f in self.bound._channel.w.frames),0)
            self.assertEqual(self.role_loaders[1].calls,[])
        finally:
            release.set()
            if not task.done():task.cancel()
            await asyncio.gather(task,return_exceptions=True)

    async def test_close_during_b_preparation_retires_pending_journal(self):
        entered=asyncio.Event();release=asyncio.Event()
        async def fence():entered.set();await release.wait()
        self.prepare_fence=fence
        task=asyncio.create_task(self.online_bootstrap())
        try:
            await asyncio.wait_for(entered.wait(),5)
            startup=self.current_startup
            self.assertIsNotNone(startup.prepared.journal)
            await startup.close()
            self.assertTrue(startup.prepared.channel._task.done())
            self.assertEqual(sum(f[:1]==b'H' for f in self.bound._channel.w.frames),0)
            with self.assertRaises(Exception):
                with startup.prepared.journal._authorized():pass
            release.set()
            with self.assertRaises(Exception):await task
            self.assertEqual(self.role_loaders[1].calls,[])
        finally:
            release.set()
            if not task.done():task.cancel()
            await asyncio.gather(task,return_exceptions=True)

del Online
if __name__=='__main__':unittest.main()
