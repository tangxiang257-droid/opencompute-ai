"""Ordinary finite synthetic metadata, actual SQLite/files, no computation/network."""
import asyncio, dataclasses, hashlib, importlib.abc, json, os, sqlite3, struct, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self,name,*args):
        if name.split('.')[0] in {'torch','transformers','safetensors','tensorflow','public_pool'} or name.endswith('.worker'):raise AssertionError('offline import denied')
def guard(event,args):
    if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):raise AssertionError('offline event denied')
sys.meta_path.insert(0,Block());sys.addaudithook(guard)
import controlled_delivery_authority as control
import noncompute_application_service as app
import task_input_api as identity
import pair_plan_contract as pair
import token_input_snapshot as snapshots
from ledger_database import PathTrust
from result_artifacts import Limits
from text_input_ledger import TextInputLedger
from cluster_p1.channel import TLSConfig
try:import input_declaration as declaration
except ImportError:declaration=None
MODEL=dict(kind='ModelIdentity',version=1,config_sha256='1'*64,weights_sha256='2'*64,semantics='stock-qwen2-full-causal-v1',weight_format='safetensors-single',compute_dtype='float32',kv_dtype='float32',activation_dtype='float32',logits_dtype='float32')
CONTRACT='c'*64
class Users:
    def __init__(self):self.revoked=False;self.subject='alice';self.calls=[];self.authority=None
    def verify(self,token):
        if self.authority:self.calls.append(self.authority._lock.locked())
        if self.revoked:return None
        return identity.Principal('synthetic-declaration-test',self.subject if token=='alice-token' else 'bob') if token in ('alice-token','bob-token') else None
class Checks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(declaration,'read-only input declaration module missing')
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.fds=[]
        for n in ('db','inputs','results'):(self.root/n).mkdir(mode=0o700)
        for p in (self.root,self.root/'inputs',self.root/'results'):
            fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY);self.fds.append(fd);self.addCleanup(os.close,fd)
        self.users=Users();self.owner=identity._owner(identity.Principal('synthetic-declaration-test','alice'))
        with patch.object(control,'_UnifiedLedger',TextInputLedger):
            self.service=app.open_service(PathTrust(self.fds[0],('db',)),self.fds[1],self.fds[2],Limits(2097152,4194304,8388608,16777216,100,4),tls=TLSConfig('not-read','not-read','not-read',{'unused':'a'*64}),recipient='unused',deployment=control.ControlledDeployment(True),verifier=self.users,initialize=True,enable_unified=True)
        self.a=self.service._authority;self.users.authority=self.a;self.ledger=self.a._store._managed._ledger
        self.binding=snapshots.InputBinding(self.owner,'request',pair._digest(MODEL),'declared-test-v1',CONTRACT,100)
        self.ref=snapshots.publish_input(self.fds[1],self.binding,(1,2,3));raw=b'ordinary text'
        async with self.a._lock:
            with self.ledger._transaction():
                self.draft=self.ledger._create_draft(self.owner,'1'*32,raw)['draft_id']
                self.task=self.ledger.commit_text(self.ledger._db,self.owner,'source-key',self.draft,1,raw,CONTRACT,'d'*64,self.binding,self.ref)
        self.reader=self.make_reader()
    async def asyncTearDown(self):
        if hasattr(self,'service'):await self.service.close()
    def make_reader(self,**kw):
        args=dict(service=self.service,model=MODEL,preprocessing_version='declared-test-v1',contract_sha256=CONTRACT);args.update(kw)
        return declaration.InputDeclarationReader(**args)
    async def prepare(self,**kw):
        args=dict(credential='alice-token',task_id=self.task.task_id);args.update(kw)
        return await self.reader.prepare_input_declaration(**args)
    async def test_stable_frozen_declaration_separates_digest_domains(self):
        before=self.ledger._db.total_changes;dump=list(self.ledger._db.iterdump());value=await self.prepare()
        self.assertEqual(value,await self.prepare());self.assertEqual(before,self.ledger._db.total_changes);self.assertEqual(dump,list(self.ledger._db.iterdump()))
        self.assertEqual(value.model_record_digest,pair._digest(MODEL));self.assertEqual(value.model.weights_sha256,'2'*64);self.assertNotEqual(value.model_record_digest,value.model.weights_sha256)
        self.assertEqual(value.snapshot_digest,self.ref.digest);self.assertEqual(value.token_count,3);self.assertEqual(value.token_blob_sha256,hashlib.sha256(struct.pack('<qqq',1,2,3)).hexdigest())
        self.assertEqual(value.contract_sha256,CONTRACT);self.assertEqual(value.render_sha256,'d'*64)
        self.assertFalse(value.execution_allowed);self.assertFalse(value.computation_verified);self.assertEqual(value.tokenizer_status,'internal_metadata_only');self.assertEqual(value.model_status,'unverified_declared')
        with self.assertRaises(dataclasses.FrozenInstanceError):value.execution_allowed=True
        with self.assertRaises(dataclasses.FrozenInstanceError):value.model.weights_sha256='e'*64
        self.assertNotIn('alice-token',repr(value));self.assertNotIn('ordinary text',repr(value));self.assertNotIn('tokens',dataclasses.asdict(value));self.assertIn(True,self.users.calls)
    async def test_default_disabled(self):
        with self.assertRaises(declaration.DeclarationRejected):await declaration.InputDeclarationReader().prepare_input_declaration('alice-token',self.task.task_id)
    async def test_foreign_missing_and_invalid_credentials_same_fixed_error(self):
        for kw in ({'credential':'bob-token'},{'credential':'admin'},{'task_id':'0'*32},{'task_id':'../db'}):
            with self.assertRaises(declaration.DeclarationRejected) as got:await self.prepare(**kw)
            self.assertEqual(str(got.exception),'input declaration unavailable')
        self.assertFalse(self.a._poisoned);await self.prepare()
    async def test_revoke_while_waiting_lock_no_file_read(self):
        entered=asyncio.Event();old=self.users.verify
        def verify(token):value=old(token);entered.set();return value
        self.users.verify=verify
        await self.a._lock.acquire()
        try:
            with patch.object(declaration,'read_input',side_effect=AssertionError('file read after revoke')) as read:
                job=asyncio.create_task(self.prepare());await entered.wait();self.users.revoked=True;self.a._lock.release()
                with self.assertRaises(declaration.DeclarationRejected):await job
                read.assert_not_called()
        finally:
            if self.a._lock.locked():self.a._lock.release()
    async def test_owner_changed_while_waiting_lock_rejected(self):
        entered=asyncio.Event();old=self.users.verify
        def verify(token):value=old(token);entered.set();return value
        self.users.verify=verify;await self.a._lock.acquire()
        job=asyncio.create_task(self.prepare());await entered.wait();self.users.subject='bob';self.a._lock.release()
        with self.assertRaises(declaration.DeclarationRejected):await job
    async def test_revoke_after_file_read_discards_declaration(self):
        old=declaration.read_input
        def read(*a):value=old(*a);self.users.revoked=True;return value
        with patch.object(declaration,'read_input',side_effect=read):
            with self.assertRaises(declaration.DeclarationRejected):await self.prepare()
    async def test_cancelled_task_denied(self):
        await self.a.cancel(self.owner,self.task.task_id)
        with self.assertRaises(declaration.DeclarationRejected):await self.prepare()
    async def test_snapshot_corruption_denied_no_db_mutation(self):
        file=self.root/'inputs'/(self.ref.digest+'.tokens');os.chmod(file,0o600);raw=file.read_bytes();file.write_bytes(raw[:-1]+bytes([raw[-1]^1]));os.chmod(file,0o400)
        before=self.ledger._db.total_changes
        with self.assertRaises(declaration.DeclarationRejected):await self.prepare()
        self.assertEqual(before,self.ledger._db.total_changes)
    async def test_model_record_not_weights_pin(self):
        for field,value in (('weights_sha256','e'*64),('config_sha256','f'*64)):
            model=dict(MODEL);model[field]=value;reader=self.make_reader(model=model)
            with self.assertRaises(declaration.DeclarationRejected):await reader.prepare_input_declaration('alice-token',self.task.task_id)
    async def test_model_schema_rejects_extra_and_wrong_types(self):
        for model in ({**MODEL,'extra':'x'},{**MODEL,'version':True},{**MODEL,'semantics':'arbitrary-code'},{**MODEL,'weights_sha256':'z'*64}):
            with self.assertRaises(declaration.DeclarationRejected):self.make_reader(model=model)
    async def test_trusted_config_copied_not_mutable(self):
        model=dict(MODEL);reader=self.make_reader(model=model);model['weights_sha256']='f'*64
        value=await reader.prepare_input_declaration('alice-token',self.task.task_id);self.assertEqual(value.model.weights_sha256,'2'*64)
    async def test_contract_and_preprocessing_pins_mismatch_rejected(self):
        for kwargs in ({'contract_sha256':'e'*64},{'preprocessing_version':'other-version'}):
            with self.assertRaises(declaration.DeclarationRejected):await self.make_reader(**kwargs).prepare_input_declaration('alice-token',self.task.task_id)
    async def test_legacy_ordinary_source_not_text_rejected(self):
        binding=dataclasses.replace(self.binding,request_id='legacy');task=await self.a.create(self.owner,'legacy-key',binding,(1,2,3))
        with self.assertRaises(declaration.DeclarationRejected):await self.prepare(task_id=task.task_id)
    async def test_current_draft_edit_does_not_reinterpret_source(self):
        before=await self.prepare()
        async with self.a._lock:
            with self.ledger._transaction():self.ledger._db.execute('UPDATE prompt_drafts SET body=?,version=version+1 WHERE draft_id=?',(b'new text',self.draft))
        self.assertEqual(before,await self.prepare())
    async def test_v12_plan_denial_unchanged_and_no_registry_import(self):
        before=self.ledger._db.total_changes;await self.prepare();self.assertEqual(before,self.ledger._db.total_changes)
        with self.assertRaises(control.Denied):self.ledger.bind_plan(self.owner,self.task.task_id,b'{}')
        self.assertIn('text_source_no_plan',{r[0] for r in self.ledger._db.execute("SELECT name FROM sqlite_master WHERE type='trigger'")})
        self.assertNotIn('cluster_p1.nonce_coordinator',sys.modules)
    async def test_closed_authority_denied(self):
        await self.a.close()
        with self.assertRaises(declaration.DeclarationRejected):await self.prepare()
    async def test_async_verifier_not_awaited(self):
        ran=[]
        async def verify(token):ran.append(token);return identity.Principal('x','alice')
        self.users.verify=verify
        with self.assertRaises(declaration.DeclarationRejected):await self.prepare()
        self.assertEqual(ran,[])
    async def test_cancel_waiting_lock_no_result_or_file_read(self):
        entered=asyncio.Event();old=self.users.verify
        def verify(token):value=old(token);entered.set();return value
        self.users.verify=verify;await self.a._lock.acquire()
        try:
            with patch.object(declaration,'read_input',side_effect=AssertionError('read cancelled')) as read:
                job=asyncio.create_task(self.prepare());await entered.wait();job.cancel()
                with self.assertRaises(asyncio.CancelledError):await job
                read.assert_not_called()
        finally:self.a._lock.release()
        await self.prepare()
    async def test_read_leaves_files_and_database_bytes_unchanged(self):
        def state():return {str(p.relative_to(self.root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob('*') if p.is_file()}
        before=state();await self.prepare();self.assertEqual(before,state())
    async def test_corrupt_source_binding_never_declared(self):
        # Deliberate test-only corruption: restore original trigger before read.
        # Outside writers are unsupported; this probes detection, not recovery.
        trigger=self.ledger._db.execute("SELECT sql FROM sqlite_master WHERE name='text_source_no_update'").fetchone()[0]
        for field in ('token_blob_sha256','snapshot_digest','contract_sha256'):
            original=self.ledger._db.execute('SELECT '+field+' FROM text_input_sources WHERE task_id=?',(self.task.task_id,)).fetchone()[0]
            with self.ledger._transaction():
                self.ledger._db.execute('DROP TRIGGER text_source_no_update')
                self.ledger._db.execute('UPDATE text_input_sources SET '+field+'=? WHERE task_id=?',('e'*64,self.task.task_id))
                self.ledger._db.execute(trigger)
            with self.assertRaises(declaration.DeclarationRejected):await self.prepare()
            with self.ledger._transaction():
                self.ledger._db.execute('DROP TRIGGER text_source_no_update')
                self.ledger._db.execute('UPDATE text_input_sources SET '+field+'=? WHERE task_id=?',(original,self.task.task_id))
                self.ledger._db.execute(trigger)
        await self.prepare()
if __name__=='__main__':unittest.main(verbosity=2)
