"""Actual local journals/spool with ordinary-byte execution/provider replacements."""
import hashlib,sqlite3,struct,unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from validation.one_token.test_assembly import Tests as Fixture,SyntheticLoader,SyntheticDevice
from cluster_p1.local_stage_operations import StageAOperation,StageBOperation
from cluster_p1.byte_spool import Binding,Plan,Spool
from cluster_p1.envelope import Envelope
from cluster_p1.projection_driver import Denied
from cluster_p1.control_journal import ControlJournal
from cluster_p1.role_control import RoleDescriptor,_ids
from cluster_p1.control_exchange import _json,result_digest
from cluster_p1.store import canonical
from validation.control_exchange.test_transport import Holds

class Tests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=Fixture.asyncSetUp
    asyncTearDown=Fixture.asyncTearDown
    factory=Fixture.factory
    make_receiver=Fixture.make_receiver
    def resources_local(self):
        return SimpleNamespace(verify_stage=lambda spec,ticket,stage:self.resources.verify_stage1(spec,ticket),quarantine=self.resources.quarantine)
    def a(self,**changes):
        args=dict(instance='1'*64,layer_range=(0,1),authority=self.authority,resources=self.resources_local(),ticket=self.resources.ticket,device=self.device,loader=self.loader,factories=self.factory)
        args.update(changes)
        return StageAOperation(self.spec,self.policy,self.sender,struct.pack('<q',1),**args)
    async def deliver_hidden(self):
        a=self.a();a.begin('scope-A');metadata=await a('scope-A');a.finish('scope-A')
        value=self.sender.query(0)['output'];edge=self.receiver._receipt_edge
        frame=Envelope(self.spec.request,self.spec.session,1,0,0,(0,1),self.spec.model,edge.layout_version,(1,1,self.hidden),'float32',value)
        self.receiver.make_receiver().receive_bytes(frame.encode())
        return a,value,metadata
    def b(self,value,**changes):
        actual=replace(self.spec,inputs=(hashlib.sha256(value).hexdigest(),))
        plan=Plan(Binding(actual.owner,actual.request,actual.session,'single-token',1,'result-connection',actual.model,actual.layout,actual.input_snapshot,actual.token_digest,actual.preprocessing,actual.plan_digest),(1,),actual.vocab)
        self.source.reserve('result-key',plan,'source');self.destination.reserve('result-key',plan,'destination')
        spool=Spool(self.root/'spool',self.source,self.destination,'result-key',plan)
        args=dict(journal_path=self.root/'stage1.db',instance='2'*64,layer_range=(1,2),authority=self.authority,resources=self.resources_local(),ticket=self.resources.ticket,device=SyntheticDevice(),loader=SyntheticLoader(),factories=self.factory)
        args.update(changes);op=StageBOperation(actual,self.policy,self.receiver,spool,**args);self.addCleanup(op.close)
        return op
    async def test_A_actual_owner_commit_same_scope_no_foreign_role_handle(self):
        a=self.a();a.begin('owned-A');r=await a('owned-A');a.finish('owned-A')
        self.assertEqual(r['output_digest'],hashlib.sha256(self.sender.query(0)['output']).hexdigest())
        self.assertEqual(tuple(a.bridge.scopes),('owned-A',))
        for name in ('receiver','artifacts','spool','driver'):self.assertFalse(hasattr(a,name))
        with self.assertRaises(Denied):a.committed_bytes() # no control COMMITTED grant
        with self.assertRaises(Denied):await a('owned-A')
    async def test_B_receipt_driver_spool_uses_only_local_db_and_same_scope(self):
        _,value,_=await self.deliver_hidden();b=self.b(value);b.begin('owned-B')
        with patch.object(self.sender._store,'_tx',side_effect=AssertionError('foreign A database')):
            r=await b('owned-B');b.finish('owned-B')
        self.assertEqual(tuple(b.bridge.scopes),('owned-B',))
        self.assertEqual(b.driver._used_scopes,{'owned-B'})
        self.assertEqual(r['output_digest'],b.sink.digest.hexdigest())
        self.assertEqual(self.source.record('result-key')['state'],'SEALED')
        with self.assertRaises(Denied):b.enable_read()
        for name in ('sender','artifacts'):self.assertFalse(hasattr(b,name))
    async def test_missing_device_or_resources_denies_before_load(self):
        with self.assertRaises(Denied):self.a(device=None)
        with self.assertRaises(Denied):self.a(resources=None)
        self.assertEqual(self.loader.calls,[]);self.assertIsNone(self.sender.query(0))
    async def test_stageA_claim_before_load_failure_prevents_reconstruction(self):
        a=self.a();a.begin('A-fail')
        with patch.object(self.loader,'load',side_effect=RuntimeError('load unknown')):
            with self.assertRaises(RuntimeError):await a('A-fail')
        a.quarantine()
        with self.assertRaises(Denied):self.a().begin('A-new')
        self.assertIsNone(self.sender.query(0))
    async def test_receipt_replacement_after_begin_denies_before_B_load(self):
        _,value,_=await self.deliver_hidden();b=self.b(value);b.begin('B')
        with sqlite3.connect(self.root/'receiver.db') as c:c.execute("UPDATE p1_received_frames SET frame=x'00'")
        with self.assertRaises(Exception):await b('B')
        self.assertEqual(b.loader.calls,[])
    async def test_wrong_load_range_rejected_without_projection(self):
        a=self.a(layer_range=(0,2))
        with self.assertRaises(Denied):a.begin('wrong')
        self.assertEqual(self.loader.calls,[])
    async def test_wrong_scope_cannot_run_registered_work(self):
        a=self.a();a.begin('owned')
        with self.assertRaises(Denied):await a('other')
        self.assertEqual(self.loader.calls,[])
    async def test_failed_preclaim_quarantine_does_not_require_uncreated_table(self):
        a=self.a(layer_range=(0,2))
        with self.assertRaises(Denied):a.begin('bad-preclaim')
        a.quarantine()
        self.assertTrue(a.retired);self.assertEqual(self.loader.calls,[])
    def control(self,op):
        s=op.spec
        binding=dict(owner=s.owner,task=s.request,session=s.session,epoch=s.epoch,stage=op.stage,
            input_snapshot=s.input_snapshot,model=s.model,layout=s.layout,plan=s.plan_digest,input_digest=s.inputs[0],
            device='local-device',instance=op.instance,peer='f'*64,declaration_digest=op.declaration_digest,
            token_digest=s.token_digest,preprocessing=s.preprocessing)
        command,reservation=_ids(binding)
        d=RoleDescriptor('a' if op.stage==0 else 'b',command,reservation,_json(binding),64)
        j=ControlJournal(self.root/('control-'+str(op.stage)),1000,resources=Holds());self.addCleanup(j.close)
        op.bind_control(j,d);j.prepare(command,reservation,binding,64);j.consume(command,reservation,binding)
        return j,d
    async def test_A_export_binds_actual_bytes_to_committed_control_digest(self):
        a=self.a();j,d=self.control(a);a.begin('A');metadata=await a('A');a.finish('A')
        with self.assertRaises(Denied):a.committed_bytes()
        j.complete(d.command_id,d.reservation_id,d.binding,result_digest(metadata))
        self.assertEqual(hashlib.sha256(a.committed_bytes()).hexdigest(),metadata['output_digest'])
        changed=bytes(self.hidden*4)
        with sqlite3.connect(self.root/'sender.db') as c:
            c.execute('UPDATE p1_local_attempts SET output=?',(changed,))
            c.execute("UPDATE p1_events SET payload=? WHERE id='local:output:0'",(canonical(['local_output',self.spec.plan_digest,0,hashlib.sha256(changed).hexdigest()]),))
        with self.assertRaises(Denied):a.committed_bytes()
    async def test_B_export_rechecks_local_control_retirement_after_enable(self):
        _,value,_=await self.deliver_hidden();b=self.b(value);j,d=self.control(b);b.begin('B')
        metadata=await b('B');b.finish('B')
        with self.assertRaises(Denied):b.enable_read()
        j.complete(d.command_id,d.reservation_id,d.binding,result_digest(metadata))
        b.enable_read();b.check_export();j.fail(d.command_id,d.reservation_id,d.binding)
        with self.assertRaises(Denied):b.check_export()
    def test_explicit_driver_scope_cannot_be_reused_for_next_token(self):
        from validation.stage1_projection.test_driver import Doubles
        from cluster_p1.projection_driver import Spec,Policy,Journal,Driver
        values=(struct.pack('<f',1),struct.pack('<f',2));d=Doubles()
        spec=Spec('owner','request','nonce',1,*['a'*64]*6,1,3,tuple(hashlib.sha256(v).hexdigest() for v in values))
        journal=Journal(self.root/'scope-driver',spec);self.addCleanup(journal.close)
        driver=Driver(journal,spec,Policy(1,1,1,1000000,262144,2097152,2097152),authority=d,resources=d,backend=d,completion=d,sink=d)
        driver.run(0,values[0],scope='same')
        with self.assertRaises(Denied):driver.run(1,values[1],scope='same')
        self.assertEqual(d.log.count('prepare'),1)

del Fixture
if __name__=='__main__':unittest.main()
