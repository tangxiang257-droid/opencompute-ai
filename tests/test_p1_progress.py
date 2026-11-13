"""Actual P1Store metadata only; no executor, tensors, sockets or nodes."""
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from cluster_p1.contracts import Task, Session, Stage, Tile, State
from cluster_p1.store import P1Store, Conflict

H='a'*64
class ProgressTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'db';self.store=P1Store(self.path)
        self.store.create_task(Task(task_id='t',owner_id='owner',model_hash=H,total_layers=4,payload_ref='not-read'),event_id='t')
        self.store.bind_session(Session(session_id='s',task_id='t',owner_id='owner',epoch=1,model_hash=H,layout_hash=H,expected_tokens=5,selected_nodes=('a','b')),
            [Stage(session_id='s',stage_id=i,layer_start=2*i,layer_end=2*i+2,model_hash=H,layout_hash=H,assigned_node=('a','b')[i]) for i in range(2)],event_id='bind')
        for i in range(2):self.store.stage_transition('s',1,i,'ready',event_id=f'ready{i}')
        self.store.transition('s',1,State.RUNNING,event_id='run')
    def tile(self,stage,index,start,count):
        return Tile(model_hash=H,layout_hash=H,session_id='s',epoch=1,stage_id=stage,tile_id=index,token_start=start,token_count=count,shape=(1,count,4),dtype='float32')
    def commit(self,tile):
        for state in ('received','computed','verified','committed'):
            self.store.tile_transition(tile,state,event_id=f'{tile.stage_id}-{tile.tile_id}-{state}')
    def progress(self,epoch=1,owner='owner'):
        return self.store.pipeline_progress('s',epoch,owner_id=owner)
    def test_initial_and_no_rows_changed(self):
        with sqlite3.connect(self.path) as c:before=list(c.iterdump())
        p=self.progress()
        self.assertEqual((p.session_id,p.epoch,p.state,p.expected_tokens),('s',1,State.RUNNING,5))
        self.assertFalse(p.execution_allowed)
        for stage in p.stages:
            self.assertEqual((stage.committed_tokens,stage.next_tile_id,stage.remaining_tokens),(0,0,5))
            self.assertIsNone(stage.inflight_tile);self.assertIsNone(stage.upstream_tile)
        with sqlite3.connect(self.path) as c:self.assertEqual(list(c.iterdump()),before)
    def test_wavefront_does_not_add_reverse_dependency(self):
        a=self.tile(0,0,0,2);self.commit(a)
        p=self.progress();x,y=p.stages
        self.assertEqual((x.committed_tokens,x.next_tile_id),(2,1))
        self.assertEqual(y.upstream_tile,a)
        b=self.tile(1,0,0,2);self.store.tile_transition(b,'received',event_id='b-receive')
        a2=self.tile(0,1,2,2);self.store.tile_transition(a2,'received',event_id='a2-receive')
        p=self.progress();self.assertEqual(p.stages[0].inflight_tile,a2)
        self.assertEqual(p.stages[1].inflight_tile,b)
        self.assertEqual([s.inflight_state for s in p.stages],['received','received'])
        self.assertFalse(p.execution_allowed)
    def test_exact_upstream_tail_and_completed_observation(self):
        for tid,start,count in [(0,0,2),(1,2,2),(2,4,1)]:
            a=self.tile(0,tid,start,count);self.commit(a)
            p=self.progress();self.assertEqual(p.stages[1].upstream_tile,a)
            self.assertEqual(p.stages[1].remaining_tokens,5-start)
            self.commit(self.tile(1,tid,start,count))
        self.store.transition('s',1,State.VERIFYING,event_id='verify')
        self.store.transition('s',1,State.COMPLETED,event_id='done')
        p=self.progress();self.assertEqual(p.state,State.COMPLETED)
        self.assertEqual([s.remaining_tokens for s in p.stages],[0,0])
        self.assertTrue(all(s.upstream_tile is None for s in p.stages))
    def test_owner_epoch_cancel_and_new_epoch(self):
        with self.assertRaises(PermissionError):self.progress(owner='other')
        for epoch in (True,1.0,0,2):
            with self.assertRaises(Conflict):self.progress(epoch=epoch)
        self.store.identify_interrupted(event_id='interrupted')
        self.assertEqual(self.progress().state,State.INTERRUPTED)
        self.store.advance_epoch('s',1,event_id='advance')
        with self.assertRaises(Conflict):self.progress()
        p=self.progress(epoch=2);self.assertEqual(p.state,State.PREPARING)
        self.store.cancel_task('t',owner_id='owner',event_id='cancel')
        self.assertEqual(self.progress(epoch=2).state,State.CANCELLED)
    def test_report_immutable_and_fresh_reads(self):
        p=self.progress()
        with self.assertRaises((AttributeError,TypeError)):p.epoch=9
        with self.assertRaises((AttributeError,TypeError)):p.stages[0].committed_tokens=9
        self.commit(self.tile(0,0,0,2))
        self.assertEqual(p.stages[0].committed_tokens,0)
        self.assertEqual(self.progress().stages[0].committed_tokens,2)
    def test_old_observation_cannot_bypass_cancel(self):
        self.commit(self.tile(0,0,0,2))
        old=self.progress()
        self.assertIsNotNone(old.stages[1].upstream_tile)
        self.store.cancel_task('t',owner_id='owner',event_id='cancel-after-observation')
        with self.assertRaises(Conflict):
            self.store.tile_transition(self.tile(1,0,0,2),'received',event_id='late')
        self.assertEqual(self.progress().state,State.CANCELLED)

    def test_missing_database_not_recreated_and_read_flags(self):
        real=sqlite3.connect;calls=[]
        def connect(*a,**kw):
            calls.append((a,kw));return real(*a,**kw)
        with patch('cluster_p1.store.sqlite3.connect',side_effect=connect):self.progress()
        self.assertTrue(calls[0][1]['uri'])
        self.assertTrue(calls[0][0][0].endswith('?mode=ro'))
        self.path.rename(self.path.with_name('saved-db'))
        with self.assertRaises(sqlite3.OperationalError):self.progress()
        self.assertFalse(self.path.exists())

    def test_read_only_single_snapshot_during_other_commit(self):
        with sqlite3.connect(self.path) as c:c.execute('PRAGMA journal_mode=WAL')
        reached=threading.Event();release=threading.Event();errors=[];results=[];real=sqlite3.connect
        def connect(*a,**k):
            c=real(*a,**k)
            if threading.current_thread().name=='progress-reader':
                def trace(sql):
                    if sql.startswith('SELECT stage_id'):
                        reached.set()
                        if not release.wait(5):raise AssertionError('barrier timeout')
                c.set_trace_callback(trace)
            return c
        def read():
            try:results.append(self.progress())
            except BaseException as e:errors.append(e)
        with patch('cluster_p1.store.sqlite3.connect',side_effect=connect):
            thread=threading.Thread(target=read,name='progress-reader');thread.start()
            try:
                self.assertTrue(reached.wait(5))
                self.commit(self.tile(0,0,0,2))
            finally:release.set();thread.join(5)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertEqual(results[0].stages[0].committed_tokens,0)
        self.assertEqual(self.progress().stages[0].committed_tokens,2)

if __name__=='__main__':unittest.main()
