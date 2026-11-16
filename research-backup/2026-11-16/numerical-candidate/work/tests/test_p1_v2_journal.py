import copy
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from test_p1_tile_contract_v2 import fixture
from cluster_p1.v2_journal import V2Journal
from cluster_p1.store import Conflict
from cluster_p1.contracts import State

@contextmanager
def auth(): yield 'owner'

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'journal.db'
        self.args=fixture()
    def make(self,authorize=auth):
        return V2Journal.create(self.path,*self.args[:3],authorize=authorize)
    def submit(self,j,event='plan'):
        return j.register(self.args[3],hidden_size=24,vocab_size=32,event_id=event)
    def test_fresh_and_replay(self):
        j=self.make();r=self.submit(j);self.assertFalse(r['execution_allowed'])
        self.assertFalse(r['historical_receipt']);self.assertTrue(self.submit(j)['historical_receipt'])
        self.assertEqual(r['plan_digest'],self.submit(j)['plan_digest'])
    def test_existing_untouched(self):
        self.path.write_bytes(b'untouched')
        with self.assertRaises(FileExistsError): self.make()
        self.assertEqual(self.path.read_bytes(),b'untouched')
    def test_revoked_replay(self):
        allowed=[True]
        @contextmanager
        def verifier():
            if not allowed[0]:raise PermissionError('revoked')
            yield 'owner'
        j=self.make(verifier);self.submit(j);allowed[0]=False
        with self.assertRaises(PermissionError):self.submit(j)
    def test_wrong_identity(self):
        @contextmanager
        def wrong():yield 'other'
        with self.assertRaises(PermissionError):self.make(wrong)
        self.assertFalse(self.path.exists())
    def test_cancel_replay_and_new_write(self):
        j=self.make();self.submit(j);j.cancel(event_id='cancel')
        self.assertTrue(self.submit(j)['historical_receipt'])
        with self.assertRaises(Conflict):self.submit(j,'new')
    def test_cancel_first(self):
        j=self.make();j.cancel(event_id='cancel')
        with self.assertRaises(Conflict):self.submit(j)
    def test_stale_epoch(self):
        j=self.make();j._store.transition('s',1,State.INTERRUPTED,event_id='interrupt')
        j._store.advance_epoch('s',1,event_id='advance')
        with self.assertRaises(Conflict):self.submit(j)
    def test_event_and_plan_conflict(self):
        j=self.make()
        with self.assertRaises(Conflict):self.submit(j,'v2:create')
        self.submit(j)
        with self.assertRaises(Conflict):self.submit(j,'another')
    def test_fault_rolls_back_both_rows(self):
        j=self.make()
        with patch.object(j,'_record_event',side_effect=RuntimeError('injected')):
            with self.assertRaises(RuntimeError):self.submit(j)
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('select count(*) from p1_v2_plans').fetchone()[0],0)
            self.assertEqual(c.execute("select count(*) from p1_events where id='plan'").fetchone()[0],0)
        self.submit(j)
    def test_legacy_insert_fenced_before_registration(self):
        j=self.make()
        for table,values in [('p1_tiles',"'s',1,0,0,'{}'"),('p1_tile_states',"'s',1,0,0,'{}','received'")]:
            with sqlite3.connect(self.path) as c:
                with self.assertRaises(sqlite3.IntegrityError):c.execute(f'insert into {table} values({values})')
    def test_path_replacement_refused_without_delete(self):
        j=self.make();self.path.rename(self.path.with_suffix('.old'));self.path.write_bytes(b'other')
        with self.assertRaises(Conflict):self.submit(j)
        self.assertEqual(self.path.read_bytes(),b'other')
    def test_close_fences(self):
        j=self.make();j.close();j.close()
        with self.assertRaises(Conflict):self.submit(j)
    def test_initialization_failure_preserves_partial_not_reopenable(self):
        with patch('cluster_p1.v2_journal.P1Store.bind_session',side_effect=RuntimeError('fail')):
            with self.assertRaises(RuntimeError) as caught:self.make()
        self.assertIn('No automatic cleanup',caught.exception.__notes__[0])
        self.assertTrue(self.path.exists())
        with self.assertRaises(FileExistsError):self.make()
    def test_cancel_waits_for_registration_transaction(self):
        import threading
        j=self.make();entered=threading.Event();release=threading.Event();attempt=threading.Event();errors=[]
        original=j._record_event
        def record(*args):
            entered.set()
            if not release.wait(3):raise RuntimeError('barrier timeout')
            original(*args)
        def register():
            try:self.submit(j)
            except BaseException as e:errors.append(e)
        def cancel():
            attempt.set()
            try:j.cancel(event_id='cancel')
            except BaseException as e:errors.append(e)
        with patch.object(j,'_record_event',side_effect=record):
            a=threading.Thread(target=register);a.start()
            try:
                self.assertTrue(entered.wait(3));b=threading.Thread(target=cancel);b.start()
                self.assertTrue(attempt.wait(3))
            finally:release.set();a.join(3)
            b.join(3);self.assertFalse(a.is_alive());self.assertFalse(b.is_alive())
        self.assertFalse(errors)
        self.assertEqual(j._store.get_session('s').state,State.CANCELLED)
        self.assertTrue(self.submit(j)['historical_receipt'])
    def test_diagnostic_failure_preserves_original_exception(self):
        class HostileError(RuntimeError):
            def add_note(self, note):raise AssertionError('must not dispatch override')
        for error in (RuntimeError('original'),HostileError('override')):
            with self.subTest(error=type(error).__name__):
                path=Path(self.tmp.name)/type(error).__name__
                if type(error) is RuntimeError:error.__notes__='malformed'
                cause=ValueError('cause');error.__cause__=cause
                with patch('cluster_p1.v2_journal.P1Store.bind_session',side_effect=error):
                    with self.assertRaises(RuntimeError) as caught:
                        V2Journal.create(path,*self.args[:3],authorize=auth)
                self.assertIs(caught.exception,error)
                self.assertIs(caught.exception.__cause__,cause)
                self.assertTrue(path.exists())
    def test_authorization_exit_failure_after_commit_is_uncertain(self):
        exits=[0]
        @contextmanager
        def verifier():
            yield 'owner'
            exits[0]+=1
            if exits[0]==2:raise RuntimeError('authorization exit failed')
        j=self.make(verifier)
        with self.assertRaisesRegex(RuntimeError,'authorization exit failed'):self.submit(j)
        # An exception is not proof of rollback: exact authorized replay resolves it.
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('select count(*) from p1_v2_plans').fetchone()[0],1)
            self.assertEqual(c.execute("select count(*) from p1_events where id='plan'").fetchone()[0],1)
        self.assertTrue(self.submit(j)['historical_receipt'])
