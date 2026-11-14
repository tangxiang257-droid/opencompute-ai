"""Actual private stage bodies dispatch over memory control Channels only.

Computation, identity, execution permission and resources are explicit doubles;
no real model/device/node/socket is started by this integration test.
"""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import struct
import unittest
from cluster_p1.channel import Channel,Grant
from cluster_p1.control_journal import ControlJournal
from cluster_p1.projection_driver import Denied
from validation.role_bridge import test_transport as data
from validation.one_token import test_assembly as fixtures
from validation.control_exchange.test_transport import Holds
try:
    from cluster_p1.control_exchange import ControlClient,ControlService,ControlPair
except ImportError:
    ControlClient=ControlService=ControlPair=None

class Promises(Holds):
    def __init__(self):super().__init__();self.records={}
    @contextmanager
    def hold(self,rid,binding,charge):
        if self.records.get(rid)!=(binding,charge):raise PermissionError('existing exact commitment required')
        with super().hold(rid,binding,charge):yield None

class Resources(fixtures.SyntheticResources):
    def __init__(self,promises):super().__init__();self.promises=promises;self.bound=[];self.fail_after_bind=False
    async def bind_control(self,spec,ticket,stage,descriptor):
        if ticket is not self.ticket:raise PermissionError('existing pipeline ticket required')
        rid=descriptor['reservation_id'];entry=(descriptor['binding'],descriptor['charge'])
        if rid in self.promises.records and self.promises.records[rid]!=entry:raise PermissionError('changed commitment')
        self.promises.records[rid]=entry;self.bound.append((stage,descriptor))
        if self.fail_after_bind:raise RuntimeError('lost binding confirmation')

class Authority(fixtures.SyntheticAuthority):
    def __init__(self):super().__init__();self.active=0
    @contextmanager
    def context(self,name):
        with super().context(name):
            self.active+=1
            try:yield None
            finally:self.active-=1

class Tests(unittest.IsolatedAsyncioTestCase):
    make_receiver=data.Tests.make_receiver
    factory=fixtures.Tests.factory
    def setUp(self):
        self.assertIsNotNone(ControlPair,'control exchange missing')
        data.Tests.setUp(self)
    async def asyncSetUp(self):
        await data.Tests.asyncSetUp(self)
        self.authority=Authority();self.policy=replace(self.policy,host_capacity=524288)
        self.identities=Holds();self.permissions=Holds();self.promises=Promises()
        self.resources=Resources(self.promises)
        self.control_journals={};self.services={};self.clients={};pairs={}
        for stage,node in enumerate(('a','b')):
            journal=ControlJournal(self.root/f'control-{stage}.db',4194304,resources=self.promises)
            self.control_journals[stage]=journal;self.addCleanup(journal.close)
            cr,cw,sr,sw=data.streams('controller',node)
            grant=Grant(self.spec.request,self.spec.session,1,stage,(0,1),self.spec.model,2)
            client=Channel(cr,cw,data.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
            server=Channel(sr,sw,data.config('controller'),{'controller':{grant}},capacity_bytes=32768,capacity_tiles=1)
            self.channels.extend((client,server))
            service=ControlService(server,journal,stage=stage,node=node,coordinator_node='controller',
                device='device-'+node,instance=str(stage+1)*64,identity=self.identities,authorize=self.permissions)
            endpoint=ControlClient(client);self.services[stage]=service;self.clients[stage]=endpoint
            pairs[stage]=(endpoint,service)
            await asyncio.gather(client.start(),server.start())
        self.control=ControlPair(pairs,charges={0:2097152,1:2097152})
        # Real existing writer methods must never await while local owner lock is held.
        for channel in self.channels:
            original=channel.w.drain
            async def drain(original=original):
                self.assertEqual(self.authority.active,0,'owner fence spans transport await')
                self.assertEqual((self.identities.active,self.permissions.active,self.promises.active),(0,0,0))
                return await original()
            channel.w.drain=drain
    async def asyncTearDown(self):
        if hasattr(self,'control'):await self.control.close()
        await data.Tests.asyncTearDown(self)
    async def execute(self,**changes):
        return await fixtures.Tests.execute(self,conduit=self.bridge,control=self.control,**changes)
    async def test_actual_stage_bodies_run_only_after_remote_consumption(self):
        original=self.factory;entered=[]
        def factory(stage,loaded,spec,bridge):
            row=self.control_journals[stage]._db.execute('select state from control_calls').fetchone()
            self.assertEqual(row,('CONSUMED',));entered.append(stage)
            return original(stage,loaded,spec,bridge)
        self.factory=factory
        result=await self.execute();chunks=[]
        self.artifacts.fetch('owner',self.spec.request,lambda _,b:chunks.append(b))
        self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*151936)
        self.assertEqual(entered,[0,1]);self.assertEqual(result['output_bytes'],607744)
        self.assertEqual(self.resources.requests[0]['control_buffers'],131072)
        self.assertEqual([stage for stage,_ in self.resources.bound],[0,1])
        self.assertNotEqual(self.resources.bound[0][1]['binding']['input_digest'],self.resources.bound[1][1]['binding']['input_digest'])
        for stage in (0,1):
            receipt=await self.control.query(stage)
            self.assertEqual(receipt['state'],'COMMITTED');self.assertFalse(receipt['execution_allowed'])
        self.assertEqual(sum(c[0]=='stage0' for c in self.calls),1)
    async def test_missing_service_permission_denies_before_local_resources(self):
        self.services[1].authorize=None
        with self.assertRaises((ValueError,PermissionError)):await self.execute()
        self.assertEqual(self.loader.calls,[]);self.assertEqual(self.resources.requests,[])
        self.assertEqual(self.source.used(),0)
    async def test_resource_unknown_prevents_stage0_body_and_retains_control_charge(self):
        self.promises.fail=True
        with self.assertRaises((ValueError,PermissionError)):await self.execute()
        self.assertEqual(self.loader.calls,[]);self.assertFalse(self.calls)
        row=self.control_journals[0]._db.execute('select state,charge from control_calls').fetchone()
        self.assertEqual(row,('UNKNOWN',2097152))
    async def test_insufficient_declared_control_charge_denies_before_local_reserve(self):
        self.control._charges={0:1,1:1}
        with self.assertRaises(ValueError):await self.execute()
        self.assertEqual(self.loader.calls,[]);self.assertEqual(self.resources.requests,[])

    async def test_unknown_resource_binding_retains_original_pipeline_ticket(self):
        self.resources.fail_after_bind=True
        with self.assertRaises(RuntimeError):await self.execute()
        self.assertEqual(self.loader.calls,[]);self.assertFalse(self.calls)
        self.assertEqual(len(self.promises.records),1)
        self.assertTrue(self.resources.quarantined)
        self.assertEqual(self.source.used(),640512)
        self.assertEqual(self.control_journals[0]._db.execute('select count(*) from control_calls').fetchone(),(0,))

    async def test_lost_stage0_commit_reply_never_starts_stage1_or_reexecutes(self):
        from cluster_p1.channel import ChannelError
        writer=self.services[0].channel.w;original=writer.write;responses=0
        def lose_response(data):
            nonlocal responses
            if data[:1]==b'D':
                responses+=1
                if responses==2:raise ConnectionError('lost committed stage0 reply')
            original(data)
        writer.write=lose_response
        with self.assertRaises((ConnectionError,ChannelError,BaseExceptionGroup)):await self.execute()
        self.assertEqual(self.loader.calls,[0]);self.assertEqual(sum(c[0]=='stage0' for c in self.calls),1)
        row=self.control_journals[0]._db.execute('select state,charge from control_calls').fetchone()
        self.assertEqual(row,('COMMITTED',2097152))
        self.assertEqual(self.control_journals[1]._db.execute('select count(*) from control_calls').fetchone(),(0,))
        before=list(self.calls)
        with self.assertRaises((ValueError,PermissionError)):await self.execute()
        self.assertEqual(self.calls,before)

    async def test_source_change_during_stage1_control_wait_denies_before_loading(self):
        import sqlite3
        entered=asyncio.Event();resume=asyncio.Event();releases=0
        channel=self.services[1].channel;original=channel._release
        async def blocked_release(size):
            nonlocal releases
            releases+=1
            if releases==2:
                entered.set();await resume.wait()
            return await original(size)
        channel._release=blocked_release
        task=asyncio.create_task(self.execute())
        try:
            await asyncio.wait_for(entered.wait(),3)
            self.assertEqual(self.authority.active,0)
            with sqlite3.connect(self.root/'sender.db') as c:
                c.execute('update p1_local_attempts set output=? where tile=0',(struct.pack('<f',2.)*self.hidden,))
            resume.set()
            with self.assertRaises((ValueError,PermissionError,BaseExceptionGroup)):await task
            self.assertEqual(self.loader.calls,[0])
            self.assertFalse(any(c[0]=='stage1' for c in self.calls))
        finally:
            resume.set()
            if not task.done():task.cancel()
            await asyncio.gather(task,return_exceptions=True)

if __name__=='__main__':unittest.main()
