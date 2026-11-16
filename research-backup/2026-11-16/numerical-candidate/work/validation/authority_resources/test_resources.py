"""Ordinary SQLite budgets and thread barriers; no hardware evidence."""
import json,threading
from contextlib import contextmanager
from dataclasses import replace
import unittest
from unittest.mock import patch
from cluster_p1.role_resources import RoleResources,Ticket
from cluster_p1.projection_driver import Spec,Policy
from cluster_p1.control_exchange import _json
from validation.online_issuance import test_transport as fixture

class Provider:
    def __init__(self):self.reject=False;self.exit_error=False;self.seen=[]
    @contextmanager
    def hold(self,*args):
        self.seen.append(args)
        if self.reject:raise PermissionError('synthetic provider rejects')
        yield None
        if self.exit_error:raise PermissionError('synthetic uncertain exit')

class Tests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=fixture.Tests.asyncSetUp
    cleanup=fixture.Tests.cleanup
    def make(self,*,limits=None,authority=True,capacity=True):
        self.a=Provider();self.c=Provider();d=self.descriptor;decl=d['declaration']
        self.spec=Spec(d['owner'],d['task'],d['nonce'],1,decl['session']['model_hash'],decl['session']['layout_hash'],
            d['source_binding']['input_snapshot'],d['source_binding']['preprocessing'],decl['plan_digest'],d['source_binding']['token_digest'],24,32,(d['source_binding']['token_digest'],))
        self.policy=Policy(100,200,300,1000000,262144,2097152,2097152);self.demand=self.policy.requirements(self.spec);self.demand['host']+=32768
        r=RoleResources(self.root/'resources.db',node='a',pool_limits=limits or dict(self.demand),extra_host_bytes=32768,authority=self.a if authority else None,capacity=self.c if capacity else None)
        self.addCleanup(r.close);self.r=r;return r
    def reserve(self,d=None):return self.r.reserve((d or self.descriptor)['resource_id'],d or self.descriptor,self.spec,self.policy,charge=64)
    def binding(self,d=None,**changes):
        d=d or self.descriptor;s=self.spec
        b=dict(owner=d['owner'],task=d['task'],session=d['nonce'],epoch=1,stage=d['stage'],device=d['device'],instance=d['instance'],declaration_digest=d['declaration_digest'],model=s.model,layout=s.layout,plan=s.plan_digest,**d['source_binding'],input_digest=s.inputs[0],peer='e'*64)
        b.update(changes);return b
    async def test_exact_live_ticket_and_history_not_ticket(self):
        r=self.make();t=self.reserve();r.verify_stage(self.spec,t,0)
        with self.assertRaises(Exception):r.verify_stage(self.spec,Ticket(t.reservation_id),0)
        with self.assertRaises(Exception):r.verify_stage(self.spec,r.query(t.reservation_id),0)
        self.assertFalse(r.query(t.reservation_id)['execution_allowed'])
    async def test_missing_authority_default_denied(self):
        r=self.make(authority=False)
        with self.assertRaises(Exception):self.reserve()
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(0,))
    async def test_missing_capacity_default_denied(self):
        r=self.make(capacity=False)
        with self.assertRaises(Exception):self.reserve()
    async def test_capacity_failure_unknown_keeps_all_budgets(self):
        r=self.make();self.c.reject=True
        with self.assertRaises(PermissionError):self.reserve()
        row=r.query(self.descriptor['resource_id']);self.assertEqual(row['state'],'UNKNOWN');self.assertEqual(row['demand'],self.demand)
        self.c.reject=False;d=dict(self.descriptor,resource_id='another')
        with self.assertRaises(Exception):self.reserve(d)
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))
    async def test_provider_exit_failure_retains_unknown(self):
        r=self.make();self.c.exit_error=True
        with self.assertRaises(PermissionError):self.reserve()
        self.assertEqual(r.query(self.descriptor['resource_id'])['state'],'UNKNOWN');self.assertFalse(r.tickets)
    async def test_concurrent_same_pool_last_budget_only_one(self):
        r=self.make();barrier=threading.Barrier(3);results=[]
        def run(n):
            barrier.wait()
            try:
                from cluster_p1.online_issuance import _make_declaration,_hash
                d=json.loads(json.dumps(self.descriptor));old=d['declaration'];nonce=str(n+2)*32;task='task-'+str(n)
                old['task']['task_id']=task;old['session'].update(task_id=task,session_id=nonce)
                for stage in old['stages']:stage['session_id']=nonce
                for tile in old['plan']['tiles']:tile.update(task_id=task,session_id=nonce)
                decl=_make_declaration(old['task'],old['session'],old['stages'],old['plan']['tiles'],24,32,old['config_sha256'])
                d.update(resource_id='promise-'+str(n),task=task,nonce=nonce,declaration=decl,declaration_digest=_hash(decl),source_id='source-'+str(n))
                spec=replace(self.spec,request=task,session=nonce,plan_digest=decl['plan_digest'])
                results.append(r.reserve(d['resource_id'],d,spec,self.policy,charge=64))
            except BaseException as e:results.append(e)
        threads=[threading.Thread(target=run,args=(n,)) for n in range(2)]
        for t in threads:t.start()
        barrier.wait()
        for t in threads:t.join(5);self.assertFalse(t.is_alive())
        self.assertEqual(sum(type(x)is Ticket for x in results),1)
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))
    async def test_one_time_mapping_reuses_charge_no_second_allocate(self):
        r=self.make();t=self.reserve();b=self.binding();r.bind_control(t.reservation_id,'control',b,64)
        with r.hold_control(t.reservation_id,'control',b,64):pass
        with r.hold('control',b,64):pass
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))
        self.assertEqual(r.query(t.reservation_id)['demand'],self.demand)
        with self.assertRaises(Exception):r.bind_control(t.reservation_id,'different',b,64)
    async def test_hold_cannot_implicitly_bind(self):
        r=self.make();t=self.reserve()
        with self.assertRaises(Exception):
            with r.hold_control(t.reservation_id,'new',self.binding(),64):pass
        self.assertEqual(r.db.execute('SELECT count(*) FROM controls').fetchone(),(0,))
    async def test_bad_charge_and_source_rejected_before_mapping(self):
        r=self.make();t=self.reserve()
        for b,charge in ((self.binding(),1),(self.binding(input_snapshot='f'*64),64)):
            with self.assertRaises(Exception):r.bind_control(t.reservation_id,'control',b,charge)
        self.assertEqual(r.db.execute('SELECT count(*) FROM controls').fetchone(),(0,))
    async def test_quarantine_never_refunds(self):
        r=self.make();t=self.reserve();r.quarantine(self.spec,t,'scope')
        self.assertEqual(r.query(t.reservation_id)['state'],'UNKNOWN')
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        with self.assertRaises(Exception):self.reserve(dict(self.descriptor,resource_id='other'))
    async def test_reopen_history_only(self):
        r=self.make();t=self.reserve();r.close()
        x=RoleResources(self.root/'resources.db',node='a',pool_limits=self.demand,extra_host_bytes=32768,authority=self.a,capacity=self.c);self.addCleanup(x.close)
        self.assertEqual(x.query(t.reservation_id)['state'],'HELD')
        for call in (lambda:x.reserve(t.reservation_id,self.descriptor,self.spec,self.policy,charge=64),lambda:x.verify_stage(self.spec,t,0),lambda:x.bind_control(t.reservation_id,'c',self.binding(),64),lambda:x.quarantine(self.spec,t,'s')):
            with self.assertRaises(Exception):call()
        with self.assertRaises(Exception):
            with x.hold(t.reservation_id,self.descriptor):pass
    async def test_commit_ack_loss_has_no_ticket_and_unknown_charge(self):
        r=self.make();original=r._commit;calls=0
        def commit(c):
            nonlocal calls
            calls+=1;original(c)
            if calls==1:raise OSError('commit acknowledgement lost')
        with patch.object(r,'_commit',commit):
            with self.assertRaises(OSError):self.reserve()
        self.assertEqual(r.query(self.descriptor['resource_id'])['state'],'UNKNOWN');self.assertFalse(r.tickets)
    async def test_mapping_provider_failure_burns_mapping(self):
        r=self.make();t=self.reserve();self.c.reject=True
        with self.assertRaises(PermissionError):r.bind_control(t.reservation_id,'control',self.binding(),64)
        self.assertEqual(r.db.execute('SELECT state FROM controls').fetchone(),('UNKNOWN',))
        self.c.reject=False
        with self.assertRaises(Exception):r.bind_control(t.reservation_id,'retry',self.binding(),64)
    async def test_actual_B_input_only_fixed_once(self):
        r=self.make()
        d=self.authority.describe(self.registration,1,node='b',device='device-b',instance='instance-b',resource_id='promise-b')
        r.close();r=RoleResources(self.root/'b.db',node='b',pool_limits=self.demand,extra_host_bytes=32768,authority=self.a,capacity=self.c);self.addCleanup(r.close);self.r=r
        t=self.reserve(d);r.verify_stage(self.spec,t,1)
        b=self.binding(d,input_digest='f'*64);r.bind_control(t.reservation_id,'control-b',b,64)
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,1)
        r.verify_stage(replace(self.spec,inputs=('f'*64,)),t,1)
        with self.assertRaises(Exception):r.bind_control(t.reservation_id,'control-b2',self.binding(d,input_digest='e'*64),64)

    async def test_PID_and_file_replacement_reject_live_usage(self):
        r=self.make();t=self.reserve()
        with patch.object(r,'pid',-1):
            with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        path=r.path;path.rename(self.root/'moved.db');path.write_bytes(b'other')
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        path.unlink();(self.root/'moved.db').rename(path)
    async def test_control_ID_cannot_alias_bootstrap_ID(self):
        r=self.make();t=self.reserve()
        with self.assertRaises(Exception):r.bind_control(t.reservation_id,t.reservation_id,self.binding(),64)
        self.assertEqual(r.db.execute('SELECT count(*) FROM controls').fetchone(),(0,))
    async def test_descriptor_and_stage_drift_denied(self):
        r=self.make();t=self.reserve()
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,1)
        with self.assertRaises(Exception):
            with r.hold(t.reservation_id,dict(self.descriptor,device='other')):pass
        self.assertEqual(r.query(t.reservation_id)['state'],'HELD')
    async def test_each_dimension_has_independent_aggregate_limit(self):
        for pool in ('device','host','source','destination'):
            with self.subTest(pool=pool):
                if hasattr(self,'r'):self.r.close();(self.root/'resources.db').unlink()
                # A single deficient pool must reject despite ample other pools.
                policy=Policy(100,200,300,1000000,262144,2097152,2097152)
                # Initial make determines a fixed valid demand.
                r=self.make();demand=dict(self.demand);r.close();(self.root/'resources.db').unlink()
                limits={k:v*4 for k,v in demand.items()};limits[pool]=demand[pool]-1
                r=self.make(limits=limits)
                with self.assertRaises(Exception):self.reserve()
                self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(0,))

    async def test_same_task_stage_alternate_rid_never_restores(self):
        r=self.make(limits={k:2097152 for k in ('device','host','source','destination')});t=self.reserve()
        with self.assertRaises(Exception):self.reserve(dict(self.descriptor,resource_id='alternate'))
        r.quarantine(self.spec,t,'s')
        with self.assertRaises(Exception):self.reserve(dict(self.descriptor,resource_id='alternate-after-unknown'))
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))
    async def test_fork_PID_denied_before_lock_acquisition(self):
        r=self.make()
        class NoLock:
            def __enter__(self):raise AssertionError('lock acquired before PID rejection')
            def __exit__(self,*args):pass
        with patch.object(r,'pid',-1),patch.object(r,'lock',NoLock()):
            from cluster_p1.store import Conflict
            for call in (lambda:r.query('x'),lambda:r.reserve('x',self.descriptor,self.spec,self.policy,charge=64),lambda:r.close()):
                with self.assertRaises(Conflict):call()
            with self.assertRaises(Conflict):
                with r.hold_control('a','b',{},64):pass

    async def test_charge_requires_exact_positive_int(self):
        r=self.make();t=self.reserve();b=self.binding()
        for charge in (64.0,True,False,0,-1):
            with self.subTest(charge=charge),self.assertRaises(Exception):r.bind_control(t.reservation_id,'control',b,charge)
        r.bind_control(t.reservation_id,'control',b,64)
        with self.assertRaises(Exception):
            with r.hold('control',b,64.0):pass
        with self.assertRaises(Exception):
            with r.hold_control(t.reservation_id,'control',b,64.0):pass

    async def test_quarantine_missing_provider_and_failed_sql_still_poison(self):
        r=self.make();t=self.reserve();r.capacity=None
        with patch.object(r,'_commit',side_effect=OSError('retirement commit rejected')):
            with self.assertRaises(OSError):r.quarantine(self.spec,t,'scope')
        self.assertEqual(r.query(t.reservation_id)['state'],'HELD')
        r.capacity=self.c
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        with self.assertRaises(Exception):
            with r.hold(t.reservation_id,self.descriptor):pass
    async def test_mapping_acklost_and_retirement_rollback_remain_poisoned(self):
        r=self.make();t=self.reserve();original=r._commit;calls=0
        def fail(c):
            nonlocal calls
            calls+=1
            if calls==1:original(c)
            raise OSError('first ack lost then retirement refused')
        with patch.object(r,'_commit',fail):
            with self.assertRaises(OSError):r.bind_control(t.reservation_id,'control',self.binding(),64)
        self.assertEqual(r.query(t.reservation_id)['state'],'HELD')
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        with self.assertRaises(Exception):r.bind_control(t.reservation_id,'retry',self.binding(),64)

    async def test_host_pool_includes_explicit_channel_and_metadata_extra(self):
        r=self.make();r.close();(self.root/'resources.db').unlink()
        limits=dict(self.demand);limits['host']=self.policy.requirements(self.spec)['host']
        r=self.make(limits=limits)
        with self.assertRaises(Exception):self.reserve()
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(0,))
    async def test_same_node_other_stage_shares_pool(self):
        from cluster_p1.online_issuance import _make_declaration,_hash
        r=self.make();self.reserve()
        d=json.loads(json.dumps(self.descriptor));old=d['declaration']
        old['session']['selected_nodes']=['b','a'];old['stages'][0]['assigned_node']='b';old['stages'][1]['assigned_node']='a'
        decl=_make_declaration(old['task'],old['session'],old['stages'],old['plan']['tiles'],24,32,old['config_sha256'])
        d.update(stage=1,declaration=decl,declaration_digest=_hash(decl),resource_id='other-stage',instance='stage-one-instance')
        spec=replace(self.spec,plan_digest=decl['plan_digest'])
        # Prove descriptor is valid independently; failure must be aggregate exhaustion.
        from cluster_p1.online_issuance import _descriptor
        _descriptor(d)
        with self.assertRaises(Exception):r.reserve(d['resource_id'],d,spec,self.policy,charge=64)
        self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))

    async def test_rolled_back_insert_poisoned_rid_cannot_retry(self):
        r=self.make()
        with patch.object(r,'_commit',side_effect=OSError('transaction refused')):
            with self.assertRaises(OSError):self.reserve()
        self.assertIsNone(r.query(self.descriptor['resource_id']))
        with self.assertRaises(Exception):self.reserve()
        self.assertIsNone(r.query(self.descriptor['resource_id']));self.assertFalse(r.tickets)

    async def test_untrusted_or_missing_path_does_not_prevent_owned_db_close(self):
        import sqlite3
        r=self.make();t=self.reserve();r.path.rename(self.root/'owned-original.db');r.path.write_bytes(b'replaced')
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        r.close();self.assertTrue(r.closed)
        with self.assertRaises(sqlite3.ProgrammingError):r.db.execute('SELECT 1')
        r.path.unlink()
        r=self.make();t=self.reserve();r.path.unlink()
        with self.assertRaises(Exception):r.verify_stage(self.spec,t,0)
        r.close();self.assertTrue(r.closed)
        with self.assertRaises(sqlite3.ProgrammingError):r.db.execute('SELECT 1')

if __name__=='__main__':unittest.main()
