"""Temporary SQLite and bytes only; resource promises are explicit test doubles."""
from contextlib import contextmanager
from pathlib import Path
import tempfile,threading,unittest
from unittest.mock import patch
try:
    from cluster_p1.control_journal import ControlJournal,Conflict
except ImportError:
    ControlJournal=None
    from cluster_p1.store import Conflict

class Resources:
    def __init__(self):self.calls=[];self.fail=None
    @contextmanager
    def hold(self,rid,binding,charge):
        self.calls.append((rid,binding,charge))
        if self.fail=='enter':raise RuntimeError('unknown resource check')
        yield None
        if self.fail=='exit':raise RuntimeError('resource exit failed')

class Tests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(ControlJournal,'persistent control journal missing')
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'book';self.resources=Resources()
        self.j=ControlJournal(self.path,100,resources=self.resources);self.addCleanup(self.j.close)
        self.b=dict(owner='owner',task='task',session='nonce',epoch=1,stage=0,
                    input_snapshot='a'*64,model='b'*64,layout='c'*64,plan='d'*64,
                    input_digest='e'*64,device='device',instance='instance',peer='f'*64)
        self.b.update(declaration_digest='1'*64,token_digest='2'*64,preprocessing='3'*64)
    def prepare(self):return self.j.prepare('cmd','res',self.b,60)
    def consume(self):return self.j.consume('cmd','res',self.b)
    def test_defaults_deny_before_reservation(self):
        j=ControlJournal(Path(self.tmp.name)/'deny',100);self.addCleanup(j.close)
        with self.assertRaises(Conflict):j.require_ready()
        with self.assertRaises(Conflict):j.prepare('cmd','res',self.b,60)
        self.assertIsNone(j.query('cmd'))
    def test_stable_prepare_does_not_repeat_provider_or_grant(self):
        first=self.prepare();self.assertEqual(first,self.prepare())
        self.assertEqual(len(self.resources.calls),1);self.assertEqual(first['state'],'RESERVED')
        self.assertFalse(first['execution_allowed']);self.assertFalse(first['resource_verified'])
    def test_full_binding_and_id_changes_rejected(self):
        self.prepare()
        for key,value in [('input_digest','0'*64),('instance','different'),('session','new')]:
            with self.subTest(key=key),self.assertRaises(Conflict):self.j.prepare('cmd','res',dict(self.b,**{key:value}),60)
        with self.assertRaises(Conflict):self.j.prepare('other','new',self.b,40)
        with self.assertRaises(Conflict):self.j.prepare('cmd','other',self.b,60)
    def test_single_consume_no_body_retry(self):
        self.prepare();calls=[]
        for i in range(2):
            try:self.consume()
            except Conflict:continue
            calls.append(b'ordinary bytes')
        self.assertEqual(calls,[b'ordinary bytes']);self.assertEqual(self.j.query('cmd')['state'],'CONSUMED')
    def test_terminal_history_retains_all_charge(self):
        self.prepare();self.consume()
        r=self.j.complete('cmd','res',self.b,'1'*64)
        self.assertEqual(r['state'],'COMMITTED');self.j.fail('cmd','res',self.b)
        r=self.j.query('cmd');self.assertEqual(r['state'],'COMMITTED');self.assertTrue(r['retired']);self.assertEqual(r['charge'],60)
        with self.assertRaises(Conflict):self.j.prepare('next','other',dict(self.b,task='other'),41)
    def test_resource_prepare_enter_and_exit_failures_hold_charge(self):
        for failure in ('enter','exit'):
            with self.subTest(failure=failure):
                r=Resources();r.fail=failure
                j=ControlJournal(Path(self.tmp.name)/failure,100,resources=r);self.addCleanup(j.close)
                with self.assertRaises(RuntimeError):j.prepare('cmd','res',self.b,60)
                receipt=j.query('cmd');self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(receipt['charge'],60)
                r.fail=None;self.assertEqual(j.prepare('cmd','res',self.b,60),receipt);self.assertEqual(len(r.calls),1)
                with self.assertRaises(Conflict):j.consume('cmd','res',self.b)
    def test_consume_resource_failure_never_reenters(self):
        self.prepare();self.resources.fail='exit'
        with self.assertRaises(RuntimeError):self.consume()
        self.resources.fail=None
        self.assertEqual(self.j.query('cmd')['state'],'UNKNOWN')
        with self.assertRaises(Conflict):self.consume()
    def test_reopen_does_not_restore_consumed(self):
        self.prepare();self.consume();self.j.close()
        reopened=ControlJournal(self.path,100,resources=self.resources);self.addCleanup(reopened.close)
        self.assertEqual(reopened.query('cmd')['state'],'CONSUMED')
        with self.assertRaises(Conflict):reopened.consume('cmd','res',self.b)
    def test_reopen_reserved_is_history_only_no_body(self):
        self.prepare();self.j.close()
        reopened=ControlJournal(self.path,100,resources=self.resources);self.addCleanup(reopened.close)
        calls=[]
        self.assertEqual(reopened.query('cmd')['state'],'RESERVED')
        with self.assertRaises(Conflict):
            reopened.consume('cmd','res',self.b);calls.append(b'body')
        self.assertEqual(calls,[])
        with self.assertRaises(Conflict):reopened.prepare('cmd','res',self.b,60)
        with self.assertRaises(Conflict):reopened.complete('cmd','res',self.b,'1'*64)
        with self.assertRaises(Conflict):reopened.require_ready()
        self.assertEqual(reopened.fail('cmd','res',self.b)['state'],'UNKNOWN')
    def test_concurrent_reservations_share_atomic_capacity(self):
        ready=threading.Barrier(3);out=[]
        def work(i):
            try:
                ready.wait();self.j.prepare('cmd'+str(i),'res'+str(i),dict(self.b,task='t'+str(i)),60);out.append('ok')
            except Conflict:out.append('denied')
        threads=[threading.Thread(target=work,args=(i,)) for i in (1,2)]
        for t in threads:t.start()
        ready.wait()
        for t in threads:t.join(5);self.assertFalse(t.is_alive())
        self.assertCountEqual(out,['ok','denied'])
    def test_concurrent_consumption_one_winner(self):
        self.prepare();ready=threading.Barrier(3);out=[]
        def work():
            try:
                ready.wait();self.j.consume('cmd','res',self.b);out.append('ok')
            except Conflict:out.append('denied')
        ts=[threading.Thread(target=work) for _ in range(2)]
        for t in ts:t.start()
        ready.wait()
        for t in ts:t.join(5);self.assertFalse(t.is_alive())
        self.assertCountEqual(out,['ok','denied'])
    def test_after_commit_exception_is_queryable_not_retry(self):
        self.prepare();commit=self.j._commit
        calls=[]
        def lost(c):
            commit(c);calls.append(1)
            if len(calls)==1:raise RuntimeError('commit response lost')
        with patch.object(self.j,'_commit',side_effect=lost):
            with self.assertRaises(RuntimeError):self.consume()
        self.assertIn(self.j.query('cmd')['state'],('CONSUMED','UNKNOWN'))
        with self.assertRaises(Conflict):self.consume()
    def test_query_cannot_change_state_and_bad_policy_rejected(self):
        self.assertIsNone(self.j.require_ready())
        self.prepare();before=self.j.query('cmd')
        for _ in range(3):self.assertEqual(self.j.query('cmd'),before)
        with self.assertRaises(Conflict):ControlJournal(self.path,101,resources=self.resources)
        with self.assertRaises((ValueError,TypeError)):self.j.prepare('other','other',dict(self.b,epoch=2),1)
    def test_new_nonce_cannot_reenter_same_stage(self):
        self.prepare()
        with self.assertRaises(Conflict):self.j.prepare('newcmd','newres',dict(self.b,session='newnonce'),1)
    def test_full_declaration_must_match(self):
        self.prepare()
        with self.assertRaises(Conflict):self.j.consume('cmd','res',dict(self.b,declaration_digest='0'*64))
        self.assertEqual(self.j.query('cmd')['state'],'RESERVED')
    def test_unknown_intent_commit_response_does_not_repeat_resource_check(self):
        commit=self.j._commit;calls=[]
        def lost(c):
            commit(c);calls.append(1)
            if len(calls)==1:raise RuntimeError('intent reply lost')
        with patch.object(self.j,'_commit',side_effect=lost):
            with self.assertRaises(RuntimeError):self.prepare()
        self.assertEqual(self.j.query('cmd')['state'],'UNKNOWN');self.assertEqual(self.resources.calls,[])
        self.assertEqual(self.prepare()['state'],'UNKNOWN');self.assertEqual(self.resources.calls,[])
    def test_completion_commit_response_lost_keeps_committed_history(self):
        self.prepare();self.consume();commit=self.j._commit;calls=[]
        def lost(c):
            commit(c);calls.append(1)
            if len(calls)==1:raise RuntimeError('completion reply lost')
        with patch.object(self.j,'_commit',side_effect=lost):
            with self.assertRaises(RuntimeError):self.j.complete('cmd','res',self.b,'1'*64)
        row=self.j.query('cmd');self.assertEqual(row['state'],'COMMITTED');self.assertTrue(row['retired'])
        self.assertEqual(row['output_digest'],'1'*64)
        with self.assertRaises(Conflict):self.consume()
    def test_retire_while_provider_holds_prevents_reserved_or_consume_return(self):
        for phase in ('prepare','consume'):
            with self.subTest(phase=phase):
                p=Path(self.tmp.name)/phase;r=Resources();j=ControlJournal(p,100,resources=r);self.addCleanup(j.close)
                if phase=='consume':j.prepare('cmd','res',self.b,60)
                entered=threading.Event();release=threading.Event();out=[]
                @contextmanager
                def hold(*args):
                    entered.set()
                    if not release.wait(5):raise AssertionError('provider barrier')
                    yield None
                r.hold=hold
                def run():
                    try:
                        if phase=='prepare':j.prepare('cmd','res',self.b,60)
                        else:j.consume('cmd','res',self.b)
                        out.append('unexpected permission')
                    except BaseException as e:out.append(e)
                thread=threading.Thread(target=run);thread.start()
                try:
                    self.assertTrue(entered.wait(5));j.fail('cmd','res',self.b)
                finally:release.set();thread.join(5)
                self.assertFalse(thread.is_alive());self.assertEqual(len(out),1);self.assertIsInstance(out[0],Conflict)
                row=j.query('cmd');self.assertEqual(row['state'],'UNKNOWN');self.assertTrue(row['retired']);self.assertEqual(row['charge'],60)
    def test_cleanup_failure_reports_unknown_without_releasing(self):
        self.prepare();self.resources.fail='exit'
        with patch.object(self.j,'fail',side_effect=OSError('retirement disk failed')):
            with self.assertRaises(BaseExceptionGroup):self.consume()
        row=self.j.query('cmd');self.assertEqual(row['state'],'CONSUMED');self.assertEqual(row['charge'],60)
        self.resources.fail=None
        with self.assertRaises(Conflict):self.consume()
    def test_revalidate_consumed_checks_without_reconsuming_or_charging(self):
        self.prepare();self.consume();before=self.j.query('cmd')
        self.assertEqual(self.j.revalidate_consumed('cmd','res',self.b),before)
        self.assertEqual(len(self.resources.calls),3)
        self.assertEqual(self.j.query('cmd')['charge'],60)
        with self.assertRaises(Conflict):self.consume()
    def test_revalidate_retired_consumption_denied_before_provider(self):
        self.prepare();self.consume();self.j.fail('cmd','res',self.b)
        calls=len(self.resources.calls)
        with self.assertRaises(Conflict):self.j.revalidate_consumed('cmd','res',self.b)
        self.assertEqual(len(self.resources.calls),calls)
    def test_revalidate_resource_exit_failure_stays_unknown(self):
        self.prepare();self.consume();self.resources.fail='exit'
        with self.assertRaises(RuntimeError):self.j.revalidate_consumed('cmd','res',self.b)
        self.resources.fail=None
        self.assertEqual(self.j.query('cmd')['state'],'UNKNOWN')
        with self.assertRaises(Conflict):self.j.revalidate_consumed('cmd','res',self.b)

if __name__=='__main__':unittest.main()
