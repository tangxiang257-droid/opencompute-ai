"""Real v12 STORED snapshot/reader, synthetic USER fence only; no execution."""
import asyncio
from contextlib import contextmanager
import hashlib
import threading
import struct
import unittest
from unittest.mock import patch
from validation.input_declaration import test_declaration as fixture
from private_source_bridge import LocalUserFence
from cluster_p1.contracts import Task, Session, Stage
from cluster_p1.online_issuance import IssuanceAuthority
from cluster_p1.registered_input_source import RegisteredInputSource, SourceRejected

class Permission:
    @contextmanager
    def hold(self,*args):yield None
class SyntheticFence(LocalUserFence):
    """Explicit substitute: no actual issuer, audience, expiry or signature checks."""
    def __init__(self,users):self.users=users;self.active=0;self.exit_fail=False;self.verified=asyncio.Event()
    def verify(self,credential):
        result=self.users.verify(credential);self.verified.set();return result
    @contextmanager
    def hold(self,credential):
        self.active+=1
        try:yield None
        finally:
            self.active-=1
            if self.exit_fail:raise RuntimeError('USER fence exit unknown')

class Tests(unittest.IsolatedAsyncioTestCase):
    make_reader=fixture.Checks.make_reader
    async def asyncSetUp(self):
        await fixture.Checks.asyncSetUp(self)
        self.fence=SyntheticFence(self.users);self.service._verifier=self.fence
        self.source=RegisteredInputSource(self.reader,identity_fence=self.fence)
        self.issuer=IssuanceAuthority(self.root/'issuer.db',namespace='f'*64,source_provider=self.source,authorize=Permission(),resources=Permission())
        self.addCleanup(self.issuer.close)
        model=fixture.MODEL['weights_sha256'];sid='1'*32;tid=self.task.task_id
        self.ptask=Task(task_id=tid,owner_id=self.owner,model_hash=model,total_layers=3,payload_ref=self.ref.digest)
        self.session=Session(session_id=sid,task_id=tid,owner_id=self.owner,epoch=1,model_hash=model,layout_hash='a'*64,expected_tokens=3,selected_nodes=('a','b'))
        self.stages=(Stage(session_id=sid,stage_id=0,layer_start=0,layer_end=1,model_hash=model,layout_hash='a'*64,assigned_node='a'),Stage(session_id=sid,stage_id=1,layer_start=1,layer_end=3,model_hash=model,layout_hash='a'*64,assigned_node='b'))
        self.tiles=[]
        for stage in (0,1):
            inp=dict(role='token_ids',dtype='int64',shape=(1,3)) if stage==0 else dict(role='hidden_states',dtype='float32',shape=(1,3,24))
            out=dict(role='hidden_states',dtype='float32',shape=(1,3,24)) if stage==0 else dict(role='full_logits',dtype='float32',shape=(1,3,100))
            self.tiles.append(dict(schema_version=2,task_id=tid,owner_id=self.owner,session_id=sid,epoch=1,model_hash=model,layout_hash='a'*64,stage_id=stage,tile_id=0,token_start=0,token_count=3,input=inp,output=out))
    async def asyncTearDown(self):await fixture.Checks.asyncTearDown(self)
    async def register(self,**changes):
        kw=dict(hidden_size=24,vocab_size=100,config_sha256=fixture.MODEL['config_sha256']);kw.update(changes)
        return await self.source.register(self.issuer,'alice-token',self.ptask,self.session,self.stages,tuple(self.tiles),**kw)
    async def test_real_stored_source_registration_is_read_only_for_v12(self):
        before=list(self.ledger._db.iterdump());registration=await self.register()
        descriptor=self.issuer.describe(registration,0,node='a',device='gpu',instance='fresh',resource_id='preexisting')
        self.assertEqual(descriptor['source_id'],self.task.task_id)
        self.assertEqual(descriptor['source_binding'],dict(input_snapshot=self.ref.digest,token_digest=hashlib.sha256(struct.pack("<qqq",1,2,3)).hexdigest(),preprocessing=fixture.CONTRACT))
        self.assertEqual(before,list(self.ledger._db.iterdump()))
        self.assertEqual(self.ledger._db.execute('select count(*) from structural_plans').fetchone(),(0,))
        self.assertEqual(self.fence.active,0)
    async def test_missing_or_wrong_USER_fence_denied(self):
        for provider in (None,self.users):
            with self.assertRaises(SourceRejected):RegisteredInputSource(self.reader,identity_fence=provider)
    async def test_check_outside_register_cannot_grant(self):
        with self.assertRaises(SourceRejected):self.source.check('f'*64,self.task.task_id,self.owner,self.task.task_id,'a'*64,{})
        await self.register()
        with self.assertRaises(SourceRejected):self.source.check('f'*64,self.task.task_id,self.owner,self.task.task_id,'a'*64,{})
        with self.assertRaises(SourceRejected):await self.register()
    async def test_wrong_model_config_rejected_before_registration(self):
        with self.assertRaises(SourceRejected):await self.register(config_sha256='e'*64)
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_USER_revoked_while_waiting_authority(self):
        await self.a._lock.acquire();self.fence.verified.clear()
        task=asyncio.create_task(self.register());await self.fence.verified.wait()
        self.users.revoked=True;self.a._lock.release()
        with self.assertRaises(Exception):await task
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_post_registration_fence_failure_burns_issuer(self):
        self.fence.exit_fail=True
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(1,))
        self.assertTrue(self.issuer.retired)
        self.fence.exit_fail=False
        with self.assertRaises(Exception):await self.register()
    async def test_separate_execution_authorizer_still_required(self):
        self.issuer.authorize=None
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))

    async def test_cancelled_actual_source_does_not_register(self):
        await self.a.cancel(self.owner,self.task.task_id)
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_actual_token_snapshot_corruption_rejected(self):
        import os
        path=self.root/'inputs'/(self.ref.digest+'.tokens')
        os.chmod(path,0o600);path.write_bytes(b'corrupt');os.chmod(path,0o400)
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_second_check_in_same_scope_denies(self):
        original=self.source.check
        def double(*args):original(*args);return original(*args)
        with patch.object(self.source,'check',side_effect=double):
            with self.assertRaises(SourceRejected):await self.register()
        self.assertTrue(self.issuer.retired)
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_other_thread_cannot_use_active_scope(self):
        original=self.source.check;errors=[]
        def check(*args):
            def foreign():
                try:original(*args)
                except BaseException as e:errors.append(e)
            thread=threading.Thread(target=foreign);thread.start();thread.join(3)
            self.assertFalse(thread.is_alive());self.assertEqual(len(errors),1)
            return original(*args)
        with patch.object(self.source,'check',side_effect=check):await self.register()
    async def test_separate_resource_provider_is_not_filled_in(self):
        self.issuer.resources=None
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_issuer_commit_response_lost_retires(self):
        original=self.issuer._commit
        def lost(c):original(c);raise RuntimeError('durable commit response lost')
        with patch.object(self.issuer,'_commit',side_effect=lost):
            with self.assertRaises(Exception):await self.register()
        self.assertTrue(self.issuer.retired)
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(1,))
    async def test_expired_after_USER_scope_exit_retires(self):
        original=self.fence.hold
        @contextmanager
        def expire(credential):
            with original(credential):yield None
            self.users.revoked=True
        with patch.object(self.fence,'hold',expire):
            with self.assertRaises(Exception):await self.register()
        self.assertTrue(self.issuer.retired)
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(1,))

    async def test_execution_authorizer_enter_cancels_source_before_claim(self):
        @contextmanager
        def cancel_on_enter(*args):
            self.ledger.cancel(self.owner,self.task.task_id)
            yield None
        self.issuer.authorize.hold=cancel_on_enter
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_execution_authorizer_exit_cancels_after_commit(self):
        @contextmanager
        def cancel_on_exit(*args):
            yield None
            self.ledger.cancel(self.owner,self.task.task_id)
        self.issuer.authorize.hold=cancel_on_exit
        with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(1,))
        self.assertTrue(self.issuer.retired)
        with self.assertRaises(Exception):await self.register()

    async def test_RejectAll_verifier_cannot_be_used_as_source_authority(self):
        from task_input_api import RejectAllUsers
        self.service._verifier=RejectAllUsers()
        with self.assertRaises(SourceRejected):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_valid_but_different_model_plan_does_not_match_source(self):
        self.ptask=self.ptask.model_copy(update={'model_hash':'e'*64})
        self.session=self.session.model_copy(update={'model_hash':'e'*64})
        self.stages=tuple(s.model_copy(update={'model_hash':'e'*64}) for s in self.stages)
        self.tiles=[dict(t,model_hash='e'*64) for t in self.tiles]
        with self.assertRaises(SourceRejected):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_valid_but_different_token_count_does_not_match_source(self):
        self.session=self.session.model_copy(update={'expected_tokens':2})
        for tile in self.tiles:
            tile['token_count']=2
            for key in ('input','output'):
                shape=list(tile[key]['shape']);shape[1]=2;tile[key]=dict(tile[key],shape=tuple(shape))
        with self.assertRaises(SourceRejected):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))
    async def test_valid_but_different_vocab_does_not_match_source(self):
        self.tiles[1]['output']=dict(self.tiles[1]['output'],shape=(1,3,101))
        with self.assertRaises(SourceRejected):await self.register(vocab_size=101)
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(0,))

    async def test_USER_scope_exit_source_cancellation_retires_committed_issuer(self):
        original=self.fence.hold
        @contextmanager
        def cancel_after_exit(credential):
            with original(credential):yield None
            self.ledger.cancel(self.owner,self.task.task_id)
        with patch.object(self.fence,'hold',cancel_after_exit):
            with self.assertRaises(Exception):await self.register()
        self.assertEqual(self.issuer.db.execute('select count(*) from registrations').fetchone(),(1,))
        self.assertTrue(self.issuer.retired)
        with self.assertRaises(Exception):await self.register()

if __name__=='__main__':unittest.main()
