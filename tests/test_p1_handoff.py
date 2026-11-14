import sqlite3
import unittest
from contextlib import contextmanager
from unittest.mock import patch
import test_p1_v2_journal as jt
from cluster_p1.handoff import EdgeBinding, Receiver
from cluster_p1.envelope import Envelope
from cluster_p1.store import Conflict
from cluster_p1.contracts import State

@contextmanager
def peer():yield 'a'

class Tests(unittest.TestCase):
    make=jt.Tests.make
    submit=jt.Tests.submit
    def setUp(self):
        jt.Tests.setUp(self);self.j=self.make();self.receipt=self.submit(self.j)
        self.binding=EdgeBinding('t','s',1,self.receipt['plan_digest'],'a'*64,1,'a','b')
        self.receiver=Receiver(self.j,self.binding,authenticate_peer=peer)
    def frame(self,**changes):
        d=dict(task_id='t',session_id='s',execution_epoch=1,stage_id=0,tile_id=0,token_range=(0,2),model_hash='a'*64,layout_version=1,tensor_shape=(1,2,24),dtype='float32',payload=b'\0'*192)
        d.update(changes);return Envelope(**d).encode()
    def test_receive_and_duplicate(self):
        first=self.receiver.receive_bytes(self.frame());second=self.receiver.receive_bytes(self.frame())
        self.assertFalse(first['duplicate']);self.assertTrue(second['duplicate'])
        self.assertFalse(second['execution_allowed']);self.assertFalse(second['computed'])
    def test_conflicting_bytes(self):
        self.receiver.receive_bytes(self.frame())
        with self.assertRaises(Conflict):self.receiver.receive_bytes(self.frame(payload=b'1'*192))
    def test_wrong_envelope_fields(self):
        for changes in ({'stage_id':1},{'tile_id':1},{'execution_epoch':2},{'task_id':'other'},{'layout_version':2},{'model_hash':'b'*64},{'tensor_shape':(1,2,12),'dtype':'int64'}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):self.receiver.receive_bytes(self.frame(**changes))
    def test_peer_and_owner_revocation(self):
        @contextmanager
        def wrong():yield 'b'
        r=Receiver(self.j,self.binding,authenticate_peer=wrong)
        with self.assertRaises(PermissionError):r.receive_bytes(self.frame())
        @contextmanager
        def revoked():raise PermissionError('revoked');yield
        self.j._authorize=revoked
        with self.assertRaises(PermissionError):self.receiver.receive_bytes(self.frame())
    def test_cancelled_duplicate_rejected(self):
        self.receiver.receive_bytes(self.frame());self.j.cancel(event_id='cancel')
        with self.assertRaises(Conflict):self.receiver.receive_bytes(self.frame())
    def test_insert_event_failure_atomic(self):
        with patch.object(self.j,'_record_event',side_effect=RuntimeError('fail')):
            with self.assertRaises(RuntimeError):self.receiver.receive_bytes(self.frame())
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
            self.assertEqual(c.execute('select count(*) from p1_v2_edges').fetchone()[0],0)
    def test_version_mapping_not_rebound(self):
        self.receiver.receive_bytes(self.frame())
        b=EdgeBinding('t','s',1,self.receipt['plan_digest'],'a'*64,2,'a','b')
        with self.assertRaises(Conflict):Receiver(self.j,b,authenticate_peer=peer).receive_bytes(self.frame(layout_version=2))
    def test_closed_and_budget(self):
        with self.assertRaises(ValueError):self.receiver.receive_bytes(b' '*65537)
        self.j.close()
        with self.assertRaises(Conflict):self.receiver.receive_bytes(self.frame())
    def test_wrong_trusted_binding(self):
        from dataclasses import replace
        for fields in ({'layout_hash':'b'*64},{'plan_digest':'b'*64},{'sender_node':'other'},{'receiver_node':'other'}):
            with self.subTest(fields=fields),self.assertRaises((Conflict,PermissionError)):
                Receiver(self.j,replace(self.binding,**fields),authenticate_peer=peer).receive_bytes(self.frame())
    def test_stale_epoch_duplicate_rejected(self):
        self.receiver.receive_bytes(self.frame())
        self.j._store.transition('s',1,State.INTERRUPTED,event_id='interrupt')
        self.j._store.advance_epoch('s',1,event_id='advance')
        with self.assertRaises(Conflict):self.receiver.receive_bytes(self.frame())
    def test_tail_and_out_of_order_are_receipts_not_progress(self):
        r=self.receiver.receive_bytes(self.frame(tile_id=2,token_range=(4,5),tensor_shape=(1,1,24),payload=b'\0'*96))
        self.assertFalse(r['computed']);self.assertFalse(r['execution_allowed'])
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('select count(*) from p1_tiles').fetchone()[0],0)
            self.assertEqual(c.execute('select count(*) from p1_tile_states').fetchone()[0],0)
    def test_peer_exit_failure_can_follow_committed_receipt(self):
        calls=[0]
        @contextmanager
        def exit_failure():
            yield 'a';calls[0]+=1
            if calls[0]==1:raise RuntimeError('exit failed')
        r=Receiver(self.j,self.binding,authenticate_peer=exit_failure)
        with self.assertRaises(RuntimeError):r.receive_bytes(self.frame())
        self.assertTrue(r.receive_bytes(self.frame())['duplicate'])
