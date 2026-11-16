"""Isolated SQLite/live memory-channel tests; providers are synthetic, not grants."""
import asyncio
from contextlib import contextmanager
import json
import threading
import unittest
from unittest.mock import patch
from cluster_p1.online_issuance import IssuanceAuthority, RoleInstallStore
from cluster_p1.nonce_coordinator import _require_issued_journal
from validation.online_issuance.test_transport import Tests as WireFixture, Holds, Source

@contextmanager
def execution(*args):yield 'owner'

class Tests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=WireFixture.asyncSetUp
    cleanup=WireFixture.cleanup
    exchange=WireFixture.exchange
    async def installed(self):
        handle,response=await self.exchange()
        if isinstance(handle,BaseException):raise handle
        if isinstance(response,BaseException):raise response
        return handle,response
    def reopen_role(self):
        self.store.close()
        x=RoleInstallStore(self.root/'role.db',role_node='a',identity=self.holds,authorize=self.holds,resources=self.holds,device=self.holds)
        self.addCleanup(x.close);return x
    async def test_actual_factory_and_exact_issued_guard(self):
        h,r=await self.installed()
        j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        self.assertEqual(_require_issued_journal(j,0),(self.store,h))
        self.assertEqual(self.store.query(r['intent']['command_id'])['state'],'INSTALLED')
        with self.assertRaises(Exception):self.store.create_sender(h,self.root/'second.db',execution_authorize=execution)
        self.assertFalse((self.root/'second.db').exists())
    async def test_reopen_accepted_is_history_only(self):
        h,r=await self.installed();x=self.reopen_role()
        self.assertEqual(x.query(r['intent']['command_id'])['state'],'ACCEPTED')
        with self.assertRaises(Exception):x.create_sender(h,self.root/'sender.db',execution_authorize=execution)
        with self.assertRaises(Exception):x.pending(self.descriptor,self.cc,challenge='a'*64)
        with self.assertRaises(Exception):x.retire(r['intent']['command_id'])
    async def test_factory_error_burns_install_slot(self):
        h,r=await self.installed();path=self.root/'occupied';path.write_bytes(b'existing')
        with self.assertRaises(Exception):self.store.create_sender(h,path,execution_authorize=execution)
        self.assertEqual(self.store.query(r['intent']['command_id'])['state'],'INSTALL_CONSUMED')
        with self.assertRaises(Exception):self.store.create_sender(h,self.root/'alternate',execution_authorize=execution)
        self.assertFalse((self.root/'alternate').exists())
    async def test_copied_response_and_callback_do_not_install(self):
        h,r=await self.installed()
        with self.assertRaises(Exception):self.store.accept(h.pending,json.loads(json.dumps(r)),self.cc,lambda:None)
        with self.assertRaises(Exception):self.store.accept(h.pending,r,self.cc,self.client)
    async def test_live_permission_revocation_after_factory_blocks_journal(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        self.permission.revoked=True
        with self.assertRaises(Exception):_require_issued_journal(j,0)
    async def test_original_channel_close_blocks_factory(self):
        h,r=await self.installed();await self.client.close()
        with self.assertRaises(Exception):self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution)
        self.assertFalse((self.root/'sender.db').exists())
    async def test_retirement_blocks_export_qualification(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        self.store.retire(r['intent']['command_id'])
        with self.assertRaises(Exception):_require_issued_journal(j,0)
    async def test_reopened_authority_has_query_but_no_claim(self):
        h,r=await self.installed();self.authority.close()
        a=IssuanceAuthority(self.root/'authority.db',namespace='f'*64,source_provider=Source(),authorize=self.holds,resources=self.holds);self.addCleanup(a.close)
        self.assertTrue(a.query(r['intent']['command_id'])['exists'])
        with self.assertRaises(Exception):a.claim_stage(self.registration,r['intent'],self.sc.authenticated_identity)
    async def test_same_stage_concurrent_claim_exactly_once(self):
        p=self.store.pending(self.descriptor,self.cc,challenge='b'*64);peer=self.sc.authenticated_identity
        barrier=threading.Barrier(3);results=[]
        def claim():
            barrier.wait()
            try:results.append(self.authority.claim_stage(self.registration,p.request,peer))
            except BaseException as exc:results.append(exc)
        ts=[threading.Thread(target=claim) for _ in range(2)]
        for t in ts:t.start()
        barrier.wait()
        for t in ts:t.join(5);self.assertFalse(t.is_alive())
        self.assertEqual(sum(type(x)is dict for x in results),1)
        self.assertEqual(self.authority.db.execute('SELECT count(*) FROM slots').fetchone(),(1,))
    async def test_default_missing_providers_denied(self):
        x=RoleInstallStore(self.root/'denied.db',role_node='a');self.addCleanup(x.close)
        with self.assertRaises(Exception):x.pending(self.descriptor,self.cc,challenge='b'*64)
        self.assertEqual(x.db.execute('SELECT count(*) FROM installs').fetchone(),(0,))
    async def test_same_source_cannot_reregister(self):
        d=self.descriptor['declaration'];p=d['plan']
        with self.assertRaises(Exception):self.authority.register(d['task'],d['session'],d['stages'],p['tiles'],hidden_size=p['hidden_size'],vocab_size=p['vocab_size'],config_sha256=d['config_sha256'],source_id='source',source_binding=self.descriptor['source_binding'])
        self.assertEqual(self.authority.db.execute('SELECT count(*) FROM registrations').fetchone(),(1,))

    async def test_same_source_new_task_and_nonce_still_denied(self):
        d=json.loads(json.dumps(self.descriptor['declaration']));p=d['plan']
        d['task']['task_id']='new-task';d['session']['task_id']='new-task';d['session']['session_id']='2'*32
        for stage in d['stages']:stage['session_id']='2'*32
        for tile in p['tiles']:tile['task_id']='new-task';tile['session_id']='2'*32
        from cluster_p1.online_issuance import _make_declaration
        # Prove the alternative declaration itself is valid before checking uniqueness.
        _make_declaration(d['task'],d['session'],d['stages'],p['tiles'],p['hidden_size'],p['vocab_size'],d['config_sha256'])
        with self.assertRaisesRegex(Exception,'permanently occupied'):
            self.authority.register(d['task'],d['session'],d['stages'],p['tiles'],hidden_size=p['hidden_size'],vocab_size=p['vocab_size'],config_sha256=d['config_sha256'],source_id='source',source_binding=self.descriptor['source_binding'])
    async def test_commit_ack_lost_burns_consumption(self):
        h,r=await self.installed();original=self.store._commit
        def commit(c):
            original(c)
            raise OSError('synthetic commit acknowledgement loss')
        with patch.object(self.store,'_commit',commit):
            with self.assertRaises(OSError):self.store.create_sender(h,self.root/'never.db',execution_authorize=execution)
        self.assertEqual(self.store.query(r['intent']['command_id'])['state'],'INSTALL_CONSUMED')
        self.assertFalse((self.root/'never.db').exists())
        with self.assertRaises(Exception):self.store.create_sender(h,self.root/'retry.db',execution_authorize=execution)
    async def test_installed_journal_identity_fields_cannot_drift(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        j._task_id='other'
        with self.assertRaises(Exception):_require_issued_journal(j,0)
    async def test_accept_commit_ack_lost_returns_no_handle(self):
        original=self.store._commit
        def commit(c):
            row=c.execute('SELECT state FROM installs').fetchone()
            original(c)
            if row==('ACCEPTED',):raise OSError('synthetic accepted commit acknowledgement loss')
        with patch.object(self.store,'_commit',commit):h,r=await self.exchange()
        self.assertIsInstance(h,BaseException);self.assertFalse(self.store.install_handles)
        self.assertEqual(self.store.db.execute('SELECT state FROM installs').fetchone(),('ACCEPTED',))
        self.assertTrue(self.store.retired)
    def control_descriptor(self,**changes):
        from cluster_p1.role_control import RoleDescriptor,_ids
        from cluster_p1.control_exchange import _json
        d=self.descriptor;decl=d['declaration'];session=decl['session']
        b=dict(owner=d['owner'],task=d['task'],session=d['nonce'],epoch=1,stage=0,device=d['device'],instance=d['instance'],
            declaration_digest=d['declaration_digest'],model=session['model_hash'],layout=session['layout_hash'],plan=decl['plan_digest'],
            input_snapshot='a'*64,input_digest='c'*64,token_digest='c'*64,preprocessing='d'*64,peer='e'*64)
        b.update(changes);cmd,rid=_ids(b)
        return RoleDescriptor('a',cmd,rid,_json(b),64)
    async def test_control_mapping_missing_provider_burns_binding(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        d=self.control_descriptor()
        with self.assertRaises(Exception):self.store.bind_control(j,0,d)
        self.assertEqual(self.store.db.execute('SELECT reservation,charge FROM install_control').fetchone(),(d.reservation_id,64))
        self.assertTrue(self.store.retired)
        with self.assertRaises(Exception):self.store.bind_control(j,0,self.control_descriptor(input_digest='f'*64))
    async def test_control_mapping_checked_on_each_issued_guard(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        d=self.control_descriptor();calls=[];revoked=False
        @contextmanager
        def hold_control(parent,rid,binding,charge):
            self.assertEqual((parent,rid,binding,charge),(self.descriptor['resource_id'],d.reservation_id,d.binding,d.charge))
            calls.append(1)
            if revoked:raise PermissionError('mapping revoked')
            yield None
        self.holds.hold_control=hold_control
        self.store.bind_control(j,0,d);_require_issued_journal(j,0)
        self.assertGreater(len(calls),1);revoked=True
        with self.assertRaises(PermissionError):_require_issued_journal(j,0)
    async def test_control_device_drift_denied_before_mapping(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        with self.assertRaises(Exception):self.store.bind_control(j,0,self.control_descriptor(device='other'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM install_control').fetchone(),(0,))
    async def test_source_provenance_mismatch_cannot_bind_control(self):
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        for key in ('input_snapshot','token_digest','preprocessing','input_digest'):
            with self.subTest(key=key),self.assertRaises(Exception):self.store.bind_control(j,0,self.control_descriptor(**{key:'f'*64}))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM install_control').fetchone(),(0,))
    async def test_missing_source_binding_denied(self):
        d=self.descriptor['declaration'];p=d['plan']
        with self.assertRaises(TypeError):
            self.authority.register(d['task'],d['session'],d['stages'],p['tiles'],hidden_size=p['hidden_size'],vocab_size=p['vocab_size'],config_sha256=d['config_sha256'],source_id='other')
    async def test_descriptor_source_mutation_does_not_change_registration(self):
        self.descriptor['source_binding']['token_digest']='f'*64
        again=self.authority.describe(self.registration,0,node='a',device='device-a',instance='instance-a',resource_id='promise-a')
        self.assertEqual(again['source_binding']['token_digest'],'c'*64)
    async def test_control_role_node_drift_denied_before_mapping(self):
        from dataclasses import replace
        h,r=await self.installed();j=self.store.create_sender(h,self.root/'sender.db',execution_authorize=execution);self.addCleanup(j.close)
        d=replace(self.control_descriptor(),role_node='other-role')
        with self.assertRaises(Exception):self.store.bind_control(j,0,d)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM install_control').fetchone(),(0,))
    async def test_query_does_not_create_live_handles(self):
        h,r=await self.installed();before=(len(self.store.install_handles),len(self.store.issued))
        for _ in range(3):
            self.assertFalse(self.store.query(r['intent']['command_id'])['execution_allowed'])
            self.assertFalse(self.authority.query(r['intent']['command_id'])['execution_allowed'])
        self.assertEqual(before,(len(self.store.install_handles),len(self.store.issued)))

del WireFixture
if __name__=='__main__':unittest.main()
