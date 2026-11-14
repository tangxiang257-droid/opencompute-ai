"""Real local journals/files/frames, explicit SYNTHETIC execution boundaries.

No model/torch import, actual forward, device, node, TLS or network. Backend
bytes are deliberately constant and establish no numerical correctness.
"""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import sqlite3
import struct
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cluster_p1.contracts import Task, Session, Stage
from cluster_p1.local_attempt import LocalAttemptJournal
from cluster_p1.receipt_attempt import ReceiptAttemptJournal, PeerIdentity
from cluster_p1.handoff import EdgeBinding
from cluster_p1.envelope import decode
from cluster_p1.byte_spool import Book, Rejected
from cluster_p1.projection_driver import Spec, Policy, Denied
from cluster_p1.private_one_token import run_one_token, Loaded
from isolated_tls_delivery import ReceiveBinding
from local_delivery_gate import Scope
from result_artifacts import _Ledger, Limits
from token_input_snapshot import InputBinding


@contextmanager
def journal_owner(*args):
    # Trusted TEST authorization only, not a remote principal.
    yield 'owner'


class SyntheticAuthority:
    def __init__(self):self.fail=None;self.log=[]
    @contextmanager
    def context(self,name):
        self.log.append(name)
        if self.fail==name:raise PermissionError(name)
        yield None
        if self.fail==name+'_exit':raise PermissionError(name+'_exit')
    def hold_pipeline(self,*args):return self.context('pipeline')
    def hold_load(self,spec,stage):return self.context('load'+str(stage))
    def hold_stage0(self,*args):return self.context('stage0')
    def hold_call(self,*args):return self.context('stage1')
    def hold_publish(self,*args):return self.context('publish')
    def finalize(self,spec,token,digest,commit):
        if self.fail=='finalize':raise PermissionError('finalize')
        commit()
        if self.fail=='finalize_exit':raise PermissionError('lost commit response')


class SyntheticResources:
    def __init__(self):self.ticket=object();self.requests=[];self.quarantined=[]
    def reserve_pipeline(self,spec,policy,required):self.requests.append(required);return self.ticket
    def verify_stage1(self,spec,ticket):
        if ticket is not self.ticket:raise ValueError('wrong synthetic ticket')
    def quarantine(self,*args):self.quarantined.append(args)


class SyntheticDevice:
    def __init__(self):self.fail_stage=None;self.finished=[];self.quarantined=[]
    @contextmanager
    def hold_load(self,*args):yield None
    def open_scope(self,*args):pass
    def require_open(self,*args):pass
    def materialize(self,scope,pending,size):
        if pending.owner.stage_id==self.fail_stage:raise RuntimeError('synthetic completion failure')
        return struct.pack('<f',1.)*(size//4)
    def finish_scope(self,scope):self.finished.append(scope)
    def require_finished(self,scope,adapter):
        if scope not in self.finished:raise ValueError('unfinished synthetic work')
    def quarantine(self,scope):self.quarantined.append(scope)


class SyntheticLoader:
    def __init__(self):self.calls=[]
    def load(self,stage,spec):
        self.calls.append(stage)
        return Loaded(object(),spec.model,spec.layout,(stage,stage+1),str(stage+1)*64)
    def quarantine(self,spec):pass


class SyntheticBackend:
    def __init__(self,stage,spec,completion,calls):
        self.stage_id=stage;self.spec=spec;self.completion=completion;self.calls=calls
    def pending(self,scope,column,count):
        pending=SimpleNamespace(tensor=object(),scope=scope,owner=self,column=column,count=count)
        self.completion.register_pending(scope,self,pending)
        return pending
    def operation(self,scope,value,*,token_start,stage_id):
        self.calls.append(('stage0',value))
        self.completion.require_open(scope,self.spec,self)
        pending=self.pending(scope,0,self.spec.hidden)
        result=self.completion.materialize(scope,pending,self.spec.hidden*4)
        self.completion.finish_scope(scope);self.completion.require_finished(scope,self)
        return result
    def check_binding(self,spec):
        if spec!=self.spec:raise ValueError('wrong spec')
    def prepare_token(self,scope,value,token):
        self.calls.append(('stage1',value));self.completion.require_open(scope,self.spec,self)
        return object()
    def project_slice(self,scope,handle,column,count):
        self.calls.append(('slice',column,count))
        self.completion.require_materialized(scope,self)
        return self.pending(scope,column,count)
    def install_cache(self,scope,handle):
        self.completion.require_finished(scope,self);self.calls.append(('install',))
    def retire(self,spec):self.calls.append(('retire',self.stage_id))


class SyntheticConduit:
    def __init__(self):self.corrupt_hidden=False;self.hidden=[];self.results=[]
    def require_current(self,spec):pass
    def exchange_hidden(self,raw):
        self.hidden.append(raw)
        return raw[:-1]+bytes([raw[-1]^1]) if self.corrupt_hidden else raw
    def exchange_result(self,raw):self.results.append(raw);return raw
    def result_binding(self,spec):
        scope=Scope(spec.request,spec.epoch,spec.plan_digest,spec.layout,'out','logits',
                    spec.session,'single-token','b','b'*64,'connection-1')
        return ReceiveBinding(scope,1,spec.model,2,'float32',((0,1,(1,1,spec.vocab)),))


class Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        fds=[]
        for name in ('inputs','results'):
            path=self.root/name;path.mkdir(mode=0o700)
            fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY);fds.append(fd);self.addCleanup(os.close,fd)
        self.db=sqlite3.connect(self.root/'artifacts.db',isolation_level=None);self.addCleanup(self.db.close)
        self.artifacts=_Ledger(self.db,fds[0],result_fd=fds[1],
            limits=Limits(2097152,4194304,8388608,16777216,100,4),initialize=True)
        self.vocab=151936;self.hidden=4
        binding=InputBinding('owner','synthetic-request','a'*64,'test-v1','c'*64,self.vocab)
        task=self.artifacts.create('owner','input-key',binding,(1,));self.task=task
        sid='1'*32
        t=Task(task_id=task.task_id,owner_id='owner',model_hash='a'*64,total_layers=2,payload_ref='not-read')
        session=Session(session_id=sid,task_id=t.task_id,owner_id='owner',epoch=1,model_hash='a'*64,
                        layout_hash='a'*64,expected_tokens=1,selected_nodes=('a','b'))
        stages=tuple(Stage(session_id=sid,stage_id=i,layer_start=i,layer_end=i+1,model_hash='a'*64,
                           layout_hash='a'*64,assigned_node=('a','b')[i]) for i in (0,1))
        descriptors=(dict(role='token_ids',dtype='int64',shape=(1,1)),
                     dict(role='hidden_states',dtype='float32',shape=(1,1,self.hidden)),
                     dict(role='full_logits',dtype='float32',shape=(1,1,self.vocab)))
        tiles=tuple(dict(schema_version=2,task_id=t.task_id,owner_id='owner',session_id=sid,epoch=1,
                         model_hash='a'*64,layout_hash='a'*64,stage_id=i,tile_id=0,token_start=0,token_count=1,
                         input=descriptors[i],output=descriptors[i+1]) for i in (0,1))
        self.sender=LocalAttemptJournal.create(self.root/'sender.db',t,session,stages,
                                               authorize=journal_owner,execution_authorize=journal_owner)
        self.addCleanup(self.sender.close)
        plan=self.sender.register(tiles,hidden_size=self.hidden,vocab_size=self.vocab,event_id='plan')['plan_digest']
        peer=PeerIdentity('a','b'*64,'connection-1')
        @contextmanager
        def peer_auth(expected):yield peer
        self.receiver=ReceiptAttemptJournal.create(self.root/'receiver.db',t,session,stages,
            authorize=journal_owner,execution_authorize=journal_owner,receipt_binding=EdgeBinding(t.task_id,sid,1,plan,'a'*64,2,'a','b'),
            expected_peer=peer,peer_authorize=peer_auth,local_node='b',expected_config_sha256='c'*64,enable_receipt_stage1=True)
        self.addCleanup(self.receiver.close)
        self.receiver.register(tiles,hidden_size=self.hidden,vocab_size=self.vocab,event_id='plan')
        self.artifacts.record_pending('owner',t.task_id,sid,1,plan,(('a','device-a','1'*64),('b','device-b','2'*64)))
        token_hash=hashlib.sha256(struct.pack('<q',1)).hexdigest()
        self.spec=Spec('owner',t.task_id,sid,1,'a'*64,'a'*64,task.snapshot.digest,'c'*64,plan,token_hash,
                       self.hidden,self.vocab,(token_hash,))
        self.policy=Policy(100,200,300,1000000,262144,2097152,2097152)
        self.source=Book(self.root/'source.db',2097152);self.destination=Book(self.root/'destination.db',2097152)
        self.addCleanup(self.source.close);self.addCleanup(self.destination.close)
        self.authority=SyntheticAuthority();self.resources=SyntheticResources();self.device=SyntheticDevice()
        self.loader=SyntheticLoader();self.conduit=SyntheticConduit();self.calls=[]
    def factory(self,stage,loaded,spec,bridge):return SyntheticBackend(stage,spec,bridge,self.calls)
    async def execute(self,**changes):
        args=dict(spec=self.spec,policy=self.policy,sender=self.sender,receiver=self.receiver,artifacts=self.artifacts,
                  source_book=self.source,destination_book=self.destination,spool_root=self.root/'spool',
                  journal_path=self.root/'stage1.db',result_key='result-key',authority=self.authority,
                  resources=self.resources,device=self.device,loader=self.loader,conduit=self.conduit,factories=self.factory)
        args.update(changes);return await run_one_token(**args)
    def task_state(self):return self.artifacts._metadata('owner',self.task.task_id)[0].state
    async def test_default_denies_before_loading_or_reserving(self):
        with self.assertRaises(Denied):await self.execute(authority=None)
        self.assertEqual(self.loader.calls,[]);self.assertEqual(self.resources.requests,[])
        self.assertEqual(self.source.used(),0);self.assertIsNone(self.sender.query(0))
    async def test_missing_device_finish_scope_denies_before_load(self):
        self.device.finish_scope=None
        with self.assertRaises(Denied):await self.execute()
        self.assertEqual(self.loader.calls,[]);self.assertEqual(self.resources.requests,[])
        self.assertEqual(self.calls,[]);self.assertEqual(self.source.used(),0)
    async def test_load_scope_exit_failure_never_forwards_or_reloads(self):
        @contextmanager
        def failed_load(*args):
            yield None
            raise RuntimeError('synthetic load completion failed')
        self.device.hold_load=failed_load
        with self.assertRaises(RuntimeError):await self.execute()
        self.assertEqual(self.loader.calls,[0]);self.assertIsNone(self.sender.query(0))
        self.assertFalse(any(c[0] in ('stage0','stage1','slice') for c in self.calls))
        fresh_loader=SyntheticLoader();self.device=SyntheticDevice()
        with self.assertRaises((Denied,Rejected)):await self.execute(loader=fresh_loader)
        self.assertEqual(fresh_loader.calls,[])
        self.assertEqual((self.source.record('result-key')['retired'],self.destination.record('result-key')['retired']),(1,1))
    async def test_loader_failure_does_not_allow_new_loader_same_books(self):
        def broken_load(stage,spec):
            self.loader.calls.append(stage)
            raise RuntimeError('synthetic partial load failed')
        self.loader.load=broken_load
        with self.assertRaises(RuntimeError):await self.execute()
        self.assertEqual(self.loader.calls,[0]);self.assertIsNone(self.sender.query(0))
        self.assertEqual(self.calls,[])
        fresh_loader=SyntheticLoader()
        with self.assertRaises((Denied,Rejected)):await self.execute(loader=fresh_loader)
        self.assertEqual(fresh_loader.calls,[])
        self.assertEqual((self.source.record('result-key')['retired'],self.destination.record('result-key')['retired']),(1,1))
    async def test_single_large_logits_persist_through_real_ledgers_and_frames(self):
        result=await self.execute()
        self.assertEqual(result['output_bytes'],607744);self.assertFalse(result['execution_allowed'])
        self.assertEqual(self.task_state(),'RESULT_STORED')
        self.assertEqual(self.sender.query(0)['state'],'OUTPUT_COMMITTED')
        with sqlite3.connect(self.root/'receiver.db') as c:
            self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone()[0],1)
        self.assertEqual(len(self.conduit.hidden),1);self.assertEqual(len(self.conduit.results),19)
        self.assertTrue(all(len(raw)<=65536 for raw in self.conduit.results))
        chunks=[];digest=self.artifacts.fetch('owner',self.task.task_id,lambda tile,block:chunks.append(block))
        self.assertEqual(digest,result['manifest_digest'])
        self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*151936)
        self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))
        self.assertEqual(self.source.record('result-key')['state'],'READING')
        self.assertEqual(self.destination.record('result-key')['state'],'RESERVED')
    async def test_corrupt_hidden_frame_blocks_stage1_and_publication(self):
        self.conduit.corrupt_hidden=True
        with self.assertRaises(Denied):await self.execute()
        self.assertEqual(self.loader.calls,[0]);self.assertEqual(self.task_state(),'STORED')
        self.assertFalse(any(c[0]=='stage1' for c in self.calls))
        with sqlite3.connect(self.root/'receiver.db') as c:
            self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone()[0],0)
    async def test_wrong_result_direction_rejects_before_result_frames(self):
        original=self.conduit.result_binding
        def wrong_direction(spec):
            binding=original(spec)
            return replace(binding,scope=replace(binding.scope,direction='in'))
        self.conduit.result_binding=wrong_direction
        with self.assertRaises(Denied):await self.execute()
        self.assertEqual(self.conduit.results,[]);self.assertEqual(self.task_state(),'STORED')
        self.assertEqual(self.db.execute('select count(*) from artifact_tickets').fetchone()[0],0)
        self.assertEqual((self.source.record('result-key')['retired'],self.destination.record('result-key')['retired']),(1,1))
    async def test_duplicate_execution_never_recomputes(self):
        await self.execute();before=list(self.calls)
        with self.assertRaises(Denied):await self.execute()
        self.assertEqual(self.calls,before);self.assertEqual(self.loader.calls,[0,1])
    async def test_completion_failure_retains_charges_and_cannot_retry(self):
        self.device.fail_stage=1
        with self.assertRaises(RuntimeError):await self.execute()
        self.assertEqual(self.task_state(),'STORED');self.assertEqual(self.source.used(),640512)
        self.assertEqual(self.source.record('result-key')['retired'],1)
        before=sum(c[0]=='stage0' for c in self.calls)
        self.device.fail_stage=None
        with self.assertRaises(Denied):await self.execute()
        self.assertEqual(sum(c[0]=='stage0' for c in self.calls),before)
    async def test_authorization_exit_failure_prevents_artifact_publication(self):
        self.authority.fail='pipeline_exit'
        with self.assertRaises(PermissionError):await self.execute()
        self.assertEqual(self.task_state(),'STORED');self.assertEqual(self.conduit.results,[])
        self.assertEqual(self.source.record('result-key')['retired'],1)
    async def test_finalize_lost_response_is_not_success_or_retry(self):
        self.authority.fail='finalize_exit'
        with self.assertRaises(PermissionError):await self.execute()
        self.assertEqual(self.task_state(),'STORED');self.assertEqual(self.conduit.results,[])
        with sqlite3.connect(self.root/'stage1.db') as c:
            self.assertEqual(c.execute('select state from tokens').fetchone(),('COMMITTED',))
            self.assertEqual(c.execute('select retired from stage').fetchone(),(1,))

    async def test_source_replaced_after_all_frames_before_artifact_commit(self):
        entered=threading.Event();changed=threading.Event();errors=[]
        original=self.artifacts._commit_published
        def gated(*args,**kwargs):
            entered.set()
            self.assertTrue(changed.wait(5))
            return original(*args,**kwargs)
        def replace_source():
            try:
                if not entered.wait(5):raise AssertionError('final commit not reached')
                with sqlite3.connect(self.root/'sender.db') as c:
                    c.execute('update p1_local_attempts set output=? where tile=0',(bytes(self.hidden*4),))
            except BaseException as exc:errors.append(exc)
            finally:changed.set()
        thread=threading.Thread(target=replace_source);thread.start()
        try:
            with patch.object(self.artifacts,'_commit_published',side_effect=gated):
                with self.assertRaises(Denied):await self.execute()
        finally:
            entered.set();thread.join(5)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertEqual(len(self.conduit.results),19)
        self.assertEqual(self.task_state(),'STORED')
        self.assertEqual(self.db.execute('select state from artifact_tickets').fetchone(),('WRITING',))
        self.assertEqual(self.source.record('result-key')['retired'],1)


if __name__=='__main__':unittest.main()
