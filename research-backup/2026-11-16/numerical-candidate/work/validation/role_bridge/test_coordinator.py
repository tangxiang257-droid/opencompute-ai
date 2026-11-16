"""Explicit temporary-SQLite metadata tests; no transport or execution provider."""
from contextlib import contextmanager
import json
import asyncio,hashlib,ssl
from types import SimpleNamespace
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
from cluster_p1.contracts import Task,Session,Stage
from cluster_p1.local_attempt import LocalAttemptJournal
from cluster_p1.nonce_coordinator import NonceCoordinator,_CoordinatedSender
import cluster_p1.nonce_coordinator as module
from cluster_p1.store import Conflict
from cluster_p1.channel import Channel,TLSConfig
from cluster_p1.bound_connection import BoundReceiptConnection

@contextmanager
def auth():yield 'owner'

class MetadataReader:
    async def readexactly(self,n):await asyncio.Future()

class MetadataWriter:
    def __init__(self):
        self.closed=False;self.context=SimpleNamespace(verify_mode=ssl.CERT_REQUIRED,check_hostname=True)
    def get_extra_info(self,key):return self
    @property
    def server_side(self):return False
    def version(self):return 'TLSv1.3'
    def getpeercert(self,binary_form=False):return b'coordinator-test-certificate'
    def is_closing(self):return self.closed
    def close(self):self.closed=True
    async def wait_closed(self):pass
    def write(self,value):pass
    async def drain(self):pass

def metadata_channel():
    fingerprint=hashlib.sha256(b'coordinator-test-certificate').hexdigest()
    return Channel(MetadataReader(),MetadataWriter(),TLSConfig('not-read','not-read','not-read',{'a':fingerprint}),{'a':set()})

class Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);sid='1'*32;h='a'*64
        task=Task(task_id='task',owner_id='owner',model_hash=h,total_layers=2,payload_ref='not-read')
        session=Session(session_id=sid,task_id='task',owner_id='owner',epoch=1,model_hash=h,layout_hash=h,expected_tokens=1,selected_nodes=('a','b'))
        stages=tuple(Stage(session_id=sid,stage_id=i,layer_start=i,layer_end=i+1,model_hash=h,layout_hash=h,assigned_node=n) for i,n in enumerate(('a','b')))
        hidden={'role':'hidden_states','dtype':'float32','shape':(1,1,4)}
        tiles=tuple(dict(schema_version=2,task_id='task',owner_id='owner',session_id=sid,epoch=1,model_hash=h,layout_hash=h,stage_id=i,tile_id=0,token_start=0,token_count=1,input=({'role':'token_ids','dtype':'int64','shape':(1,1)} if i==0 else hidden),output=(hidden if i==0 else {'role':'full_logits','dtype':'float32','shape':(1,1,8)})) for i in (0,1))
        self.args=(task,session,stages,tiles)
        self.co=NonceCoordinator.initialize(self.root/'registry',authorize=auth);self.addCleanup(self.co.close)
        self.r=self.co.register(*self.args,hidden_size=4,vocab_size=8,config_sha256='c'*64)
        self.sender=self.co.create_sender(self.r,self.root/'sender',execution_authorize=auth);self.addCleanup(self.sender.close)
        self.channels=[]
    async def asyncTearDown(self):
        for c in self.channels:await c.close()
    def receiver(self,co=None,r=None,name='receiver'):
        channel=metadata_channel();self.channels.append(channel)
        bound=BoundReceiptConnection(channel)
        journal=(co or self.co).create_receiver(r or self.r,bound,self.root/name,execution_authorize=auth)
        self.addCleanup(journal.close)
        return journal,bound,channel
    def check(self,value=None):
        check=getattr(module,'_require_coordinated_sender',None)
        self.assertIsNotNone(check,'private exact coordinator admission missing')
        return check(self.sender if value is None else value)
    def test_real_issued_sender_accepted_without_execution(self):
        self.assertIs(self.check(),self.sender);self.assertIsNone(self.sender.query(0))
        self.assertFalse(self.co.query('1'*32)['execution_allowed'])
    def test_base_journal_rejected(self):
        j=LocalAttemptJournal.create(self.root/'base',*self.args[:3],authorize=auth,execution_authorize=auth);self.addCleanup(j.close)
        with self.assertRaises((TypeError,Conflict)):self.check(j)
    def test_subclass_rejected(self):
        class Other(_CoordinatedSender):pass
        clone=object.__new__(Other);clone.__dict__.update(self.sender.__dict__)
        with self.assertRaises((TypeError,Conflict)):self.check(clone)
    def test_exact_type_clone_not_issued(self):
        clone=object.__new__(_CoordinatedSender);clone.__dict__.update(self.sender.__dict__)
        with self.assertRaises(Conflict):self.check(clone)
    def test_closed_coordinator_and_reopen_never_restore(self):
        self.co.close()
        with self.assertRaises(Conflict):self.check()
        reopened=NonceCoordinator.open_existing(self.root/'registry',authorize=auth);self.addCleanup(reopened.close)
        with patch.object(self.sender,'_coordinator',reopened,create=True):
            with self.assertRaises(Conflict):self.check()
    def test_slot_replacement_rejected(self):
        with sqlite3.connect(self.root/'registry') as c:c.execute("UPDATE slots SET binding='{}'")
        with self.assertRaises(Conflict):self.check()
    def test_session_replacement_rejected(self):
        with sqlite3.connect(self.root/'registry') as c:c.execute("UPDATE sessions SET body='{}'")
        with self.assertRaises(Conflict):self.check()
    def test_authorization_callback_replacement_rejected(self):
        with patch.object(self.sender,'_authorize',auth):
            with self.assertRaises(Conflict):self.check()
    def test_execution_callback_replacement_rejected(self):
        with patch.object(self.sender,'_execution',lambda:None):
            with self.assertRaises(Conflict):self.check()
    def test_reservation_must_remain_live(self):
        self.co._reservations.clear()
        with self.assertRaises(Conflict):self.check()
    def test_journal_path_rebinding_rejected(self):
        with patch.object(self.sender._store,'path',str(self.root/'different')):
            with self.assertRaises(Conflict):self.check()
    def test_closed_sender_rejected(self):
        self.sender.close()
        with self.assertRaises(Conflict):self.check()
    def test_registry_file_replacement_rejected(self):
        p=self.root/'registry';p.rename(self.root/'old');p.write_bytes(b'bad')
        with self.assertRaises(Conflict):self.check()
    def test_slot_burn_survives_creation_failure(self):
        other=NonceCoordinator.initialize(self.root/'other',authorize=auth);self.addCleanup(other.close)
        r=other.register(*self.args,hidden_size=4,vocab_size=8,config_sha256='c'*64)
        with patch.object(_CoordinatedSender,'create',side_effect=RuntimeError('creation')):
            with self.assertRaises(RuntimeError):other.create_sender(r,self.root/'failed',execution_authorize=auth)
        with self.assertRaises(Conflict):other.create_sender(r,self.root/'retry',execution_authorize=auth)
    def test_close_barrier_rejects_subsequent_admission(self):
        ready=threading.Event();closed=threading.Event();errors=[]
        def job():
            ready.set()
            try:
                if not closed.wait(5):raise AssertionError('barrier')
                self.check()
            except BaseException as e:errors.append(e)
        t=threading.Thread(target=job);t.start()
        self.assertTrue(ready.wait(5));self.co.close();closed.set();t.join(5)
        self.assertFalse(t.is_alive());self.assertEqual(len(errors),1);self.assertIsInstance(errors[0],Conflict)
    async def test_pair_requires_both_issued_slots(self):
        receiver,bound,channel=self.receiver()
        self.assertEqual(module._require_coordinated_pair(self.sender,receiver),(self.sender,receiver))
        self.assertEqual(self.co.query('1'*32)['claimed_stages'],(0,1))
    async def test_unissued_receiver_clone_rejected(self):
        receiver,_,_=self.receiver();clone=object.__new__(type(receiver));clone.__dict__.update(receiver.__dict__)
        with self.assertRaises(Conflict):module._require_coordinated_pair(self.sender,clone)
    async def test_receiver_other_coordinator_rejected(self):
        co=NonceCoordinator.initialize(self.root/'other',authorize=auth);self.addCleanup(co.close)
        r=co.register(*self.args,hidden_size=4,vocab_size=8,config_sha256='c'*64)
        receiver,_,_=self.receiver(co,r)
        with self.assertRaises(Conflict):module._require_coordinated_pair(self.sender,receiver)
    async def test_receipt_connection_revocation_rejected(self):
        receiver,_,channel=self.receiver();channel.revoke_identity()
        with self.assertRaises((Conflict,PermissionError,ValueError)):module._require_coordinated_pair(self.sender,receiver)
    async def test_receiver_slot_replacement_rejected(self):
        receiver,_,_=self.receiver()
        with sqlite3.connect(self.root/'registry') as c:c.execute("UPDATE slots SET binding='{}' WHERE stage=1")
        with self.assertRaises(Conflict):module._require_coordinated_pair(self.sender,receiver)

if __name__=='__main__':unittest.main()
