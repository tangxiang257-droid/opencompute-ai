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

from validation.online_issuance.test_pipeline import Holds, Sources, BootstrapDevice
from validation.role_startup.test_pipeline import Tests as Startup
from cluster_p1.role_resources import RoleResources
from cluster_p1.control_journal import ControlJournal
from cluster_p1.control_exchange import ControlService, ControlClient
from cluster_p1.role_control import RegisteredRole, CoordinatorControl
from validation.split_roles.test_pipeline import ROLE


class Tests(Startup):
    async def asyncSetUp(self):
        await super().asyncSetUp();self.tickets=[None,None]
    def kwargs(self,stage):
        # Same persistent provider is used by startup, install, control and operation.
        resource=self.role_resources[stage]
        if type(resource)is not RoleResources:return super().kwargs(stage)
        def factory(number,loaded,spec,completion):
            self.assertEqual(number,stage)
            return fixtures.SyntheticBackend(stage,spec,completion,self.role_calls[stage])
        return dict(instance=str(stage+1)*64,layer_range=(stage,stage+1),
            authority=self.role_authorities[stage],resources=resource,ticket=self.tickets[stage],
            device=self.role_devices[stage],loader=self.role_loaders[stage],factories=factory)
    async def test_changed_source_provenance_cannot_register(self):
        from cluster_p1.local_stage_operations import StageAOperation
        await self.online_bootstrap()
        for field in ('input_snapshot','token_digest','preprocessing'):
            with self.assertRaises(Exception):
                StageAOperation(replace(self.spec,**{field:'e'*64}),self.policy,self.sender,struct.pack('<q',1),**self.kwargs(0))
        self.assertEqual(self.role_loaders[0].calls,[])

    async def test_online_issued_actual_full_chain(self):
        await super().test_online_issued_actual_full_chain()
        for stage,node in enumerate(('a','b')):
            resource=self.role_resources[stage]
            self.assertIs(self.install_stores[stage].resources,resource)
            self.assertIs(self.operations[stage].resources,resource)
            self.assertIs(self.operations[stage].control.resources,resource)
            self.assertEqual(resource.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))
            self.assertEqual(resource.db.execute('SELECT count(*) FROM controls').fetchone(),(1,))
            history=resource.query('reservation-'+node)
            self.assertFalse(history['execution_allowed'])
            self.assertFalse(history['physical_capacity_verified'])
            self.assertEqual(history['demand']['source'],640512)
            self.assertEqual(history['demand']['destination'],640512)
        row=self.role_resources[1].db.execute('SELECT binding FROM controls').fetchone()
        self.assertEqual(json.loads(row[0])['input_digest'],self.operations[1].spec.inputs[0])
        self.assertNotEqual(self.operations[1].spec.inputs[0],self.spec.token_digest)

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
            desc=authority.describe(registration,stage,node=node,device='device-'+node,instance=str(stage+1)*64,resource_id='reservation-'+node)
            resource=RoleResources(self.root/f'resources-{stage}.db',node=node,
                pool_limits=dict(device=1000000,host=1048576,source=2097152,destination=2097152),
                extra_host_bytes=2*(32768+32768+65536+(32816+32768 if stage else 0))+32768,
                authority=Holds(),capacity=Holds())
            self.addCleanup(resource.close);self.role_resources[stage]=resource
            self.tickets[stage]=resource.reserve('reservation-'+node,desc,self.spec,self.policy,charge=2097152)
            store=RoleInstallStore(self.root/f'install-{stage}.db',role_node=node,identity=Holds(),authorize=Holds(),resources=resource,device=Holds())
            self.addCleanup(store.close);self.install_stores.append(store)
            cr,cw,sr,sw=wire.streams(node,'issuer')
            grant=Grant(t.task_id,session.session_id,1,stage,(0,1),self.spec.model,2)
            server=Channel(sr,sw,wire.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
            self.channels.append(server)
            opts=dict(identity=split.Identity(),authorize=Holds(),resources=resource,issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest())
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
        command,rid=_ids(binding);resource.bind_control('reservation-'+node,rid,binding,2097152)
        async def local_body(scope):
            token=ROLE.set(node)
            try:return await operation(scope)
            finally:ROLE.reset(token)
        role=RegisteredRole.register(service,spec,local_body,declaration_digest=declaration,charge=2097152,completion=operation.completion)
        coordinator=CoordinatorControl(ControlClient(client),role.descriptor);self.controls.append(coordinator)
        return role,coordinator,journal

del Startup
if __name__=='__main__':unittest.main()
