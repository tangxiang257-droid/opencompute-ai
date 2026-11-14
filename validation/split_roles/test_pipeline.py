"""Split roles: actual journals, control/data wire and storage; synthetic compute only."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import struct
import unittest
from unittest.mock import patch
from contextvars import ContextVar
from cluster_p1.envelope import Envelope
from isolated_tls_delivery import ReceiveBinding
from local_delivery_gate import Scope

ROLE=ContextVar("role",default=None)
async def as_role(name,coro):
    token=ROLE.set(name)
    try:return await coro
    finally:ROLE.reset(token)
class Identity:
    @contextmanager
    def hold(self,who):yield who.node_id
class Permission:
    @contextmanager
    def hold(self,*args):yield None
from cluster_p1.channel import Channel,Grant
from cluster_p1.control_exchange import ControlClient,ControlService
from cluster_p1.control_journal import ControlJournal
from cluster_p1.role_control import RegisteredRole,CoordinatorControl
from cluster_p1.byte_spool import Binding,Plan,Spool
from validation.role_bridge import test_transport as wire
from validation.one_token import test_assembly as fixtures
from validation.control_exchange.test_transport import Holds

class LocalResources:
    def __init__(self):self.ticket=object();self.quarantined=[];self.records={}
    def verify_stage(self,spec,ticket,stage):
        if ticket is not self.ticket:raise PermissionError('wrong local ticket')
    def quarantine(self,*args):self.quarantined.append(args)
    @contextmanager
    def hold(self,rid,binding,charge):
        if self.records.get(rid)!=(binding,charge):raise PermissionError('unbound local promise')
        yield None

class Tests(unittest.IsolatedAsyncioTestCase):
    make_receiver=wire.Tests.make_receiver
    async def asyncSetUp(self):
        self.channels=[]
        await fixtures.Tests.asyncSetUp(self)
        self.role_devices=[fixtures.SyntheticDevice(),fixtures.SyntheticDevice()]
        self.role_loaders=[fixtures.SyntheticLoader(),fixtures.SyntheticLoader()]
        self.role_authorities=[fixtures.SyntheticAuthority(),fixtures.SyntheticAuthority()]
        self.role_resources=[LocalResources(),LocalResources()]
        self.role_calls=[[],[]];self.controls=[];self.operations=[];self.endpoints=[]
        self.bound=self.coordinator._issued_journals[id(self.receiver)][-1]
        self.addAsyncCleanup(self.shutdown)
    async def shutdown(self):
        for c in self.controls:await c.close()
        for e in self.endpoints:await e.close()
        for op in self.operations:
            if hasattr(op,"close"):op.close()
        await asyncio.gather(*(c.close() for c in self.channels))
    async def register(self,stage,spec,operation):
        node=('a','b')[stage];resource=self.role_resources[stage]
        journal=ControlJournal(self.root/f'local-control-{stage}.db',4194304,resources=resource)
        self.addCleanup(journal.close)
        cr,cw,sr,sw=wire.streams('controller',node)
        grant=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
        client=Channel(cr,cw,wire.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
        server=Channel(sr,sw,wire.config('controller'),{'controller':{grant}},capacity_bytes=32768,capacity_tiles=1)
        self.channels.extend((client,server));await asyncio.gather(client.start(),server.start())
        service=ControlService(server,journal,stage=stage,node=node,coordinator_node='controller',
            device='device-'+node,instance=str(stage+1)*64,identity=Holds(),authorize=Holds())
        declaration=hashlib.sha256(self.sender._coordinator_body.encode()).hexdigest()
        service.check(spec,declaration);binding=service.binding(spec)
        from cluster_p1.role_control import _ids
        command,rid=_ids(binding);resource.records[rid]=(binding,2097152)
        async def local_body(scope):
            token=ROLE.set(node)
            try:return await operation(scope)
            finally:ROLE.reset(token)
        role=RegisteredRole.register(service,spec,local_body,declaration_digest=declaration,charge=2097152,completion=operation.completion)
        coordinator=CoordinatorControl(ControlClient(client),role.descriptor);self.controls.append(coordinator)
        return role,coordinator,journal
    async def dispatch(self,role,coordinator):
        async def serve():
            await role.serve_once();await role.serve_once()
        tasks=[asyncio.create_task(as_role(role.descriptor.role_node,serve())),asyncio.create_task(coordinator.dispatch())]
        try:return (await asyncio.gather(*tasks))[1]
        finally:
            for t in tasks:
                if not t.done():t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
    def kwargs(self,stage):
        def factory(number,loaded,spec,completion):
            self.assertEqual(number,stage)
            return fixtures.SyntheticBackend(stage,spec,completion,self.role_calls[stage])
        return dict(instance=str(stage+1)*64,layer_range=(stage,stage+1),
            authority=self.role_authorities[stage],resources=self.role_resources[stage],
            ticket=self.role_resources[stage].ticket,device=self.role_devices[stage],
            loader=self.role_loaders[stage],factories=factory)

    async def run_split(self,failure=None):
        from cluster_p1.local_stage_operations import StageAOperation,StageBOperation
        from cluster_p1.split_role_transport import HiddenSender,HiddenReceiver,ResultSender,ResultIngress,transport_scope
        a=self.launch_operation(0,self.spec,tokens=struct.pack('<q',1)) if hasattr(self,'launch_operation') else StageAOperation(self.spec,self.policy,self.sender,struct.pack('<q',1),**self.kwargs(0))
        self.operations.append(a)
        ar,ac,aj=await self.register(0,self.spec,a)
        original_a=self.sender._store._tx;original_b=self.receiver._store._tx
        @contextmanager
        def a_tx():
            self.assertIn(ROLE.get(),(None,'a'),'foreign role accessed A database')
            with original_a() as c:yield c
        @contextmanager
        def b_tx():
            self.assertIn(ROLE.get(),(None,'b'),'foreign role accessed B database')
            with original_b() as c:yield c
        with patch.object(self.sender._store,'_tx',a_tx),patch.object(self.receiver._store,'_tx',b_tx):
            receipt=await self.dispatch(ar,ac);self.assertEqual(receipt['state'],'COMMITTED')
            identity=Identity();permission=Permission()
            hs=HiddenSender(self.hidden_send,scope=transport_scope(self.spec),identity=identity,authorize=permission)
            hr=HiddenReceiver(self.bound,self.receiver,scope=transport_scope(self.spec),identity=identity,authorize=permission)
            self.endpoints.extend((hs,hr))
            await self.hidden_send.start()
            raw=Envelope(self.spec.request,self.spec.session,1,0,0,(0,1),self.spec.model,2,(1,1,self.spec.hidden),'float32',a.committed_bytes()).encode()
            ack_a,ack_b=await asyncio.gather(as_role('a',hs.send(raw)),as_role('b',hr.receive()))
            self.assertEqual(ack_a,ack_b)
            actual=replace(self.spec,inputs=(ack_b['payload_digest'],))
            dsr,dsw,dir,diw=wire.streams('b','c')
            msr,msw,mir,miw=wire.streams('b','c')
            grant=Grant(actual.request,actual.session,1,1,(0,1),actual.model,2)
            data=Channel(dsr,dsw,wire.config('c'),{'c':{grant}},capacity_bytes=32816,capacity_tiles=1)
            sm=Channel(msr,msw,wire.config('c'),{'c':{grant}},capacity_bytes=32768,capacity_tiles=1)
            im=Channel(mir,miw,wire.config('b'),{'b':{grant}},capacity_bytes=32768,capacity_tiles=1)
            self.channels.extend((data,sm,im))
            scope=Scope(actual.request,1,actual.plan_digest,actual.layout,'out','logits',actual.session,'single-token','b',hashlib.sha256(b'b').hexdigest(),'placeholder')
            binding=ReceiveBinding(scope,1,actual.model,2,'float32',((0,1,(1,1,actual.vocab)),))
            rs=ResultSender(data,sm,binding,identity=identity,authorize=permission)
            from validation.split_roles.test_transport import RecipientAuthority
            ri=None if hasattr(self,'receiving_gate') else ResultIngress(dir,diw,wire.config('b'),im,binding,identity=identity,authorize=permission,recipient_node='c',receive_authorize=RecipientAuthority())
            self.endpoints.append(rs)
            if ri is not None:self.endpoints.append(ri)
            bp=Plan(Binding(actual.owner,actual.request,actual.session,'single-token',1,rs.generation(),actual.model,actual.layout,actual.input_snapshot,actual.token_digest,actual.preprocessing,actual.plan_digest),(1,),actual.vocab)
            self.source.reserve('split-result',bp,'source');self.destination.reserve('split-result',bp,'destination')
            spool=Spool(self.root/'split-spool',self.source,self.destination,'split-result',bp)
            b=self.launch_operation(1,actual,spool=spool) if hasattr(self,'launch_operation') else StageBOperation(actual,self.policy,self.receiver,spool,journal_path=self.root/'split-driver.db',**self.kwargs(1))
            self.operations.append(b)
            br,bc,bj=await self.register(1,actual,b)
            if hasattr(self,'receiving_gate'):
                ri=await self.receiving_gate(br,bc,actual,dir,diw,im,binding,identity,permission)
                self.endpoints.append(ri)
            before_b=len(self.role_loaders[1].calls)
            if failure=='completion':self.role_devices[1].fail_stage=1
            if failure=='control-commit':
                original=bj.complete
                def lost(*args):original(*args);raise OSError('lost local control commit response')
                bj.complete=lost
            receipt=await self.dispatch(br,bc)
            if failure in ('completion','control-commit'):
                self.assertTrue(receipt['state']=='UNKNOWN' or receipt['retired'])
                with self.assertRaises(Exception):b.enable_read()
                self.assertFalse(any(f[:1]==b'D' for f in data.w.frames))
                self.assertEqual(self.artifacts._metadata('owner',actual.request)[0].state,'STORED')
                self.assertEqual(bj.query(br.descriptor.command_id)['charge'],2097152)
                before=list(self.role_calls[1])
                tasks=[asyncio.create_task(br.serve_once()),asyncio.create_task(bc.dispatch())]
                try:await asyncio.gather(*tasks)
                finally:
                    for t in tasks:
                        if not t.done():t.cancel()
                    await asyncio.gather(*tasks,return_exceptions=True)
                self.assertEqual(self.role_calls[1],before)
                return
            self.assertEqual(receipt['state'],'COMMITTED')
            self.assertEqual(len(self.role_loaders[1].calls),before_b+1)
            b.enable_read()
            @contextmanager
            def publication():yield None
            def check_b():
                b.check_export()
                self.assertEqual(hashlib.sha256(b.received_bytes()).hexdigest(),actual.inputs[0])
            if failure=='retire-transfer':
                entered=asyncio.Event();resume=asyncio.Event();original_drain=data.w.drain;blocked=False
                async def drain():
                    nonlocal blocked
                    if data.w.last_kind==b'D' and not blocked:
                        blocked=True;entered.set();await resume.wait()
                    await original_drain()
                data.w.drain=drain
                before=list(self.role_calls[1])
                tasks=[asyncio.create_task(as_role('b',rs.send(actual,spool,b.metadata['output_digest'],check_b))),asyncio.create_task(as_role('c',ri.receive(actual,self.artifacts,lambda:None,publication)))]
                try:
                    await asyncio.wait_for(entered.wait(),3)
                    bj.fail(br.descriptor.command_id,br.descriptor.reservation_id,br.descriptor.binding)
                    resume.set()
                    outcomes=await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),3)
                    self.assertTrue(all(isinstance(x,BaseException) for x in outcomes))
                    self.assertEqual(sum(f[:1]==b'D' for f in data.w.frames),1)
                    self.assertEqual(self.role_calls[1],before)
                    self.assertEqual(self.source.used(),640512);self.assertEqual(self.destination.used(),640512)
                    self.assertEqual(self.artifacts._metadata('owner',actual.request)[0].state,'STORED')
                    self.assertEqual(self.db.execute('select state from artifact_tickets').fetchone(),('WRITING',))
                finally:
                    resume.set()
                    for t in tasks:
                        if not t.done():t.cancel()
                    await asyncio.gather(*tasks,return_exceptions=True)
                return
            sent,persisted=await asyncio.gather(as_role('b',rs.send(actual,spool,b.metadata['output_digest'],check_b)),as_role('c',ri.receive(actual,self.artifacts,lambda:None,publication)))
            self.assertEqual(sent['manifest_digest'],persisted)
        chunks=[];self.artifacts.fetch('owner',actual.request,lambda _,x:chunks.append(x))
        self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*actual.vocab)
        for stage,op in enumerate((a,b)):
            self.assertEqual(self.role_devices[stage].finished,[op.scope])
            self.assertEqual(list(op.bridge.scopes),[op.scope])
            self.assertEqual(self.role_loaders[stage].calls,[stage])
        self.assertEqual(self.source.used(),640512);self.assertEqual(self.destination.used(),640512)
        self.assertEqual(sum(f[:1]==b'D' for f in data.w.frames),19)
        self.assertEqual(len(self.controls),2)
        self.assertFalse(hasattr(ac,'_service'));self.assertFalse(hasattr(b,'sender'))
        self.assertFalse(hasattr(a,'receiver'))

    async def test_split_complete_path(self):await self.run_split()
    async def test_retire_during_export_stops_next_frame_retains_charge(self):await self.run_split('retire-transfer')
    async def test_device_uncertain_blocks_export_and_recompute(self):await self.run_split('completion')
    async def test_control_commit_uncertain_blocks_export_and_recompute(self):await self.run_split('control-commit')

if __name__=='__main__':unittest.main()
