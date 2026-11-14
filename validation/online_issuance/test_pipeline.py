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

class Holds:
    @contextmanager
    def hold(self,*args):yield None
class Sources:
    def check(self,namespace,source,owner,task,digest,source_binding):
        if namespace!='f'*64 or source!='source-one' or owner!='owner':raise PermissionError('fixed synthetic source')
class BootstrapResources(Holds):
    def __init__(self,parent,local):self.parent=parent;self.local=local
    @contextmanager
    def hold_control(self,parent,rid,binding,charge):
        if parent!=self.parent or self.local.records.get(rid)!=(binding,charge):
            raise PermissionError('synthetic existing resource association mismatch')
        yield None
class BootstrapDevice:
    def check(self,*args):pass
    def begin(self,*args):pass
    def finish(self,*args):pass
    def quarantine(self,*args):pass

class Tests(unittest.IsolatedAsyncioTestCase):
    register=split.Tests.register
    dispatch=split.Tests.dispatch
    kwargs=split.Tests.kwargs
    run_split=split.Tests.run_split
    shutdown=split.Tests.shutdown
    async def asyncSetUp(self):
        self.channels=[];self.controls=[];self.operations=[];self.endpoints=[]
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.addAsyncCleanup(self.shutdown)
        fds=[]
        for name in ('inputs','results'):
            path=self.root/name;path.mkdir(mode=0o700)
            fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY);fds.append(fd);self.addCleanup(os.close,fd)
        self.db=sqlite3.connect(self.root/'artifacts.db',isolation_level=None);self.addCleanup(self.db.close)
        self.artifacts=_Ledger(self.db,fds[0],result_fd=fds[1],limits=Limits(2097152,4194304,8388608,16777216,100,4),initialize=True)
        self.vocab=151936;self.hidden=4
        task=self.artifacts.create('owner','input-key',InputBinding('owner','online-request','a'*64,'test-v1','c'*64,self.vocab),(1,));self.task=task
        t=Task(task_id=task.task_id,owner_id='owner',model_hash='a'*64,total_layers=2,payload_ref='not-read')
        session=Session(session_id='1'*32,task_id=t.task_id,owner_id='owner',epoch=1,model_hash='a'*64,layout_hash='a'*64,expected_tokens=1,selected_nodes=('a','b'))
        stages=tuple(Stage(session_id=session.session_id,stage_id=i,layer_start=i,layer_end=i+1,model_hash='a'*64,layout_hash='a'*64,assigned_node=('a','b')[i]) for i in (0,1))
        ds=(dict(role='token_ids',dtype='int64',shape=(1,1)),dict(role='hidden_states',dtype='float32',shape=(1,1,self.hidden)),dict(role='full_logits',dtype='float32',shape=(1,1,self.vocab)))
        tiles=tuple(dict(schema_version=2,task_id=t.task_id,owner_id='owner',session_id=session.session_id,epoch=1,model_hash='a'*64,layout_hash='a'*64,stage_id=i,tile_id=0,token_start=0,token_count=1,input=ds[i],output=ds[i+1]) for i in (0,1))
        self.declaration_args=(t,session,stages,tiles)
        self.role_devices=[fixtures.SyntheticDevice(),fixtures.SyntheticDevice()]
        self.role_loaders=[fixtures.SyntheticLoader(),fixtures.SyntheticLoader()]
        self.role_authorities=[fixtures.SyntheticAuthority(),fixtures.SyntheticAuthority()]
        self.role_resources=[split.LocalResources(),split.LocalResources()];self.role_calls=[[],[]]
        self.policy=Policy(100,200,300,1000000,262144,2097152,2097152)
        self.source=Book(self.root/'source.db',2097152);self.destination=Book(self.root/'destination-mirror.db',2097152)
        self.addCleanup(self.source.close);self.addCleanup(self.destination.close)

    async def online_bootstrap(self):
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
            c=Channel(cr,cw,wire.config('issuer'),{'issuer':{grant}},capacity_bytes=32768,capacity_tiles=1)
            server=Channel(sr,sw,wire.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
            self.channels.extend((c,server))
            opts=dict(identity=split.Identity(),authorize=Holds(),resources=Holds(),issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest())
            client=IssuanceClient(c,store,desc,completion=BootstrapDevice(),**opts)
            service=IssuanceService(server,authority,registration,desc,**opts)
            self.install_clients.append(client);self.endpoints.extend((client,service))
            tasks=[asyncio.create_task(service.serve_once()),asyncio.create_task(client.install())]
            try:response,handle=await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
            self.assertFalse(response['execution_allowed'])
            if stage==0:
                self.sender=store.create_sender(handle,self.root/'sender.db',execution_authorize=fixtures.journal_owner)
                self.addCleanup(self.sender.close)
            else:
                self.receiver=await prepared.prepare_online(store,handle,self.root/'receiver.db')
                self.addCleanup(self.receiver.close)
        self.assertFalse(hasattr(self.sender,'_coordinator'))
        self.assertFalse(hasattr(self.receiver,'_coordinator'))
        self.assertIsNot(self.sender._online_issuer,self.receiver._online_issuer)

    async def test_installed_without_control_cannot_begin(self):
        from cluster_p1.local_stage_operations import StageAOperation
        await self.online_bootstrap()
        op=StageAOperation(self.spec,self.policy,self.sender,struct.pack('<q',1),**self.kwargs(0))
        with self.assertRaises(Exception):op.begin('unbound')
        self.assertEqual(self.role_loaders[0].calls,[])
        self.assertIsNone(op.scope)

    async def test_registered_not_consumed_cannot_begin(self):
        from cluster_p1.local_stage_operations import StageAOperation
        await self.online_bootstrap()
        op=StageAOperation(self.spec,self.policy,self.sender,struct.pack('<q',1),**self.kwargs(0))
        await self.register(0,self.spec,op)
        with self.assertRaises(Exception):op.begin('unconsumed')
        self.assertEqual(self.role_loaders[0].calls,[])
        self.assertIsNone(op.scope)

    async def test_changed_source_provenance_cannot_register(self):
        from cluster_p1.local_stage_operations import StageAOperation
        await self.online_bootstrap()
        # First bad registration burns association, so exercise each field before it.
        for field in ('input_snapshot','token_digest','preprocessing'):
            bad=replace(self.spec,**{field:'e'*64})
            op=StageAOperation(bad,self.policy,self.sender,struct.pack('<q',1),**self.kwargs(0))
            with self.assertRaises(Exception):
                # Existing operation check must enforce installed source, before body.
                binding=dict(owner=bad.owner,task=bad.request,session=bad.session,epoch=bad.epoch,
                    stage=0,model=bad.model,layout=bad.layout,plan=bad.plan_digest,
                    input_snapshot=bad.input_snapshot,input_digest=bad.inputs[0],token_digest=bad.token_digest,
                    preprocessing=bad.preprocessing,instance=op.instance,device='device-a',
                    declaration_digest=op.declaration_digest)
                op._check_binding(binding)
        self.assertEqual(self.role_loaders[0].calls,[])

    async def test_online_issued_actual_full_chain(self):
        from cluster_p1.nonce_coordinator import NonceCoordinator
        with patch.object(NonceCoordinator,'initialize',side_effect=AssertionError('legacy coordinator forbidden')):
            await self.online_bootstrap()
            with patch.object(self.authority,'_check',side_effect=AssertionError('role accessed central runtime database')):
                await self.run_split()

    async def test_disconnected_issuance_prevents_operation_load(self):
        await self.online_bootstrap()
        await self.install_clients[0].close()
        with self.assertRaises(Exception):await self.run_split()
        self.assertEqual(self.role_loaders[0].calls,[])
        self.assertEqual(self.role_loaders[1].calls,[])

    async def test_online_issued_export_retirement_retains_storage(self):
        await self.online_bootstrap()
        await self.run_split('retire-transfer')

if __name__=='__main__':unittest.main()
