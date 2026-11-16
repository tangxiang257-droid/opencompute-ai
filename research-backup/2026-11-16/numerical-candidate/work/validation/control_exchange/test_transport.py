"""Actual control Channels and SQLite; invented SSL and ordinary providers only."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cluster_p1.channel import Channel, Grant
from cluster_p1.control_journal import ControlJournal, Conflict
from cluster_p1.projection_driver import Spec
from validation.role_bridge.test_transport import streams, config
try:
    from cluster_p1.control_exchange import ControlClient, ControlService, ControlPair, result_digest
except ImportError:
    ControlClient=ControlService=ControlPair=None


class Holds:
    def __init__(self):self.active=0;self.fail=False;self.calls=0
    @contextmanager
    def hold(self,*args):
        self.calls+=1
        if self.fail:raise PermissionError('test provider denied')
        self.active+=1
        try:yield None
        finally:self.active-=1


class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(ControlPair,'control transport missing')
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.identity=Holds();self.authority=Holds();self.resources=Holds()
        self.journal=ControlJournal(self.root/'control.db',1024,resources=self.resources)
        self.addCleanup(self.journal.close)
        self.spec=Spec('owner','task','session',1,*['a'*64]*6,4,8,('b'*64,))
        self.assignments=((0,'a','device-a','1'*64),(1,'b','device-b','2'*64))
        self.channels=[];self.services={};self.clients={};pairs={}
        for stage,node,device,instance in self.assignments:
            cr,cw,sr,sw=streams('controller',node)
            grant=Grant('task','session',1,stage,(0,1),'a'*64,2)
            client=Channel(cr,cw,config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
            server=Channel(sr,sw,config('controller'),{'controller':{grant}},capacity_bytes=32768,capacity_tiles=1)
            self.channels.extend((client,server))
            service=ControlService(server,self.journal,stage=stage,node=node,coordinator_node='controller',
                device=device,instance=instance,identity=self.identity,authorize=self.authority)
            endpoint=ControlClient(client)
            self.services[stage]=service;self.clients[stage]=endpoint;pairs[stage]=(endpoint,service)
            await asyncio.gather(client.start(),server.start())
        self.pair=ControlPair(pairs,charges={0:64,1:64})
        self.pair.check(self.spec,self.assignments,declaration_digest='c'*64)
    async def asyncTearDown(self):
        if hasattr(self,'pair'):await self.pair.close()
        if hasattr(self,'channels'):await asyncio.gather(*(c.close() for c in self.channels))
    async def test_wire_execution_is_consumed_first_and_fences_do_not_span_body(self):
        calls=[]
        def body():
            self.assertEqual(self.journal._db.execute('select state from control_calls').fetchone(),('CONSUMED',))
            self.assertEqual((self.identity.active,self.authority.active,self.resources.active),(0,0,0))
            calls.append(1);return {'output_digest':'d'*64}
        receipt=await self.pair.dispatch(0,self.spec,body)
        self.assertEqual(receipt['state'],'COMMITTED')
        self.assertEqual(receipt['result_digest'],result_digest({'output_digest':'d'*64}))
        self.assertFalse(receipt['execution_allowed']);self.assertEqual(calls,[1])
        self.assertEqual(sum(f[:1]==b'D' for f in self.clients[0].channel.w.frames),2)
        self.assertEqual(sum(f[:1]==b'D' for f in self.services[0].channel.w.frames),2)
        self.assertEqual(self.pair.buffer_bytes(),131072)
    async def test_duplicate_dispatch_is_wire_query_not_second_body(self):
        calls=[]
        first=await self.pair.dispatch(0,self.spec,lambda:calls.append('first'))
        second=await self.pair.dispatch(0,self.spec,lambda:calls.append('forbidden'))
        self.assertEqual(second,first);self.assertEqual(calls,['first'])
        self.assertEqual(sum(f[:1]==b'D' for f in self.clients[0].channel.w.frames),3)
    async def test_default_missing_authority_denies_without_charge_or_body(self):
        self.services[0].authorize=None;calls=[]
        with self.assertRaises((ValueError,PermissionError)):await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
        self.assertEqual(calls,[]);self.assertEqual(self.journal._db.execute('select count(*) from control_calls').fetchone(),(0,))
    async def test_wrong_assignment_and_capacity_change_denied(self):
        wrong=((0,'a','other-device','1'*64),self.assignments[1])
        with self.assertRaises(ValueError):self.pair.check(self.spec,wrong,declaration_digest='c'*64)
        self.clients[0].channel.capacity_tiles=2
        with self.assertRaises(ValueError):self.pair.buffer_bytes()
    async def test_body_failure_is_unknown_and_query_never_reexecutes(self):
        calls=[]
        def body():calls.append(1);raise RuntimeError('test operation failed')
        receipt=await self.pair.dispatch(0,self.spec,body)
        self.assertEqual(receipt['state'],'UNKNOWN')
        queried=await self.pair.query(0)
        self.assertEqual(queried,receipt);self.assertEqual(calls,[1])
        self.assertEqual(self.journal._db.execute('select sum(charge) from control_calls').fetchone(),(64,))
    async def test_resource_uncertainty_is_queryable_without_body(self):
        self.resources.fail=True;calls=[]
        receipt=await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
        self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(calls,[])
        self.resources.fail=False
        self.assertEqual((await self.pair.query(0))['state'],'UNKNOWN')
        self.assertEqual(self.resources.calls,1)
    async def test_cancel_after_consume_joins_service_and_never_replays(self):
        entered=asyncio.Event();exited=asyncio.Event();calls=[]
        async def body():
            calls.append(1);entered.set()
            try:await asyncio.Future()
            finally:exited.set()
        task=asyncio.create_task(self.pair.dispatch(0,self.spec,body))
        await asyncio.wait_for(entered.wait(),3);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(exited.is_set());self.assertEqual(calls,[1])
        self.assertIn(self.journal._db.execute('select state from control_calls').fetchone()[0],('UNKNOWN','CONSUMED'))
        with self.assertRaises(ValueError):await self.pair.dispatch(0,self.spec,lambda:calls.append(2))
        self.assertTrue(all(c._closed and c._task.done() for c in self.channels))
    async def test_oversize_or_nonordinary_result_cannot_be_committed(self):
        receipt=await self.pair.dispatch(0,self.spec,lambda:{'payload':b'not JSON'})
        self.assertEqual(receipt['state'],'UNKNOWN')
        self.assertIsNone(receipt['result_digest'])

    async def test_retired_during_execute_credit_release_never_calls_body(self):
        entered=asyncio.Event();resume=asyncio.Event();calls=[];releases=[]
        channel=self.services[0].channel;original=channel._release
        async def release(size):
            releases.append(size)
            if len(releases)==2:entered.set();await resume.wait()
            await original(size)
        with patch.object(channel,'_release',side_effect=release):
            task=asyncio.create_task(self.pair.dispatch(0,self.spec,lambda:calls.append(1)))
            try:
                await asyncio.wait_for(entered.wait(),3)
                command,reservation,raw=self.journal._db.execute('select command_id,reservation_id,binding from control_calls').fetchone()
                self.journal.fail(command,reservation,json.loads(raw))
            finally:resume.set()
            receipt=await task
        self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(calls,[])

    async def test_bool_wire_version_is_rejected_before_prepare(self):
        channel=self.clients[0].channel;original=channel.send
        async def send(envelope,**kwargs):
            value=json.loads(envelope.payload);value['version']=True
            raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
            return await original(replace(envelope,payload=raw,tensor_shape=(len(raw),)),**kwargs)
        with patch.object(channel,'send',side_effect=send):
            with self.assertRaises(ValueError):await self.pair.dispatch(0,self.spec,lambda:None)
        self.assertEqual(self.journal._db.execute('select count(*) from control_calls').fetchone(),(0,))

    async def test_bool_reply_version_cannot_trigger_execute(self):
        channel=self.services[0].channel;original=channel.send;calls=[]
        async def send(envelope,**kwargs):
            value=json.loads(envelope.payload);value['version']=True
            raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
            return await original(replace(envelope,payload=raw,tensor_shape=(len(raw),)),**kwargs)
        with patch.object(channel,'send',side_effect=send):
            with self.assertRaises(ValueError):await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
        self.assertEqual(calls,[])

    async def test_prepare_authorization_exit_failure_is_not_repaired_by_query(self):
        original=self.authority.hold;failed=[];calls=[]
        @contextmanager
        def hold(binding,action):
            with original(binding,action):yield None
            if action=='prepare' and not failed:failed.append(1);raise PermissionError('exit unknown')
        self.authority.hold=hold
        receipt=await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
        self.assertEqual(calls,[]);self.assertEqual(receipt['state'],'UNKNOWN');self.assertTrue(receipt['retired'])
        self.assertEqual(sum(f[:1]==b'D' for f in self.clients[0].channel.w.frames),1)

    async def test_execute_authorization_exit_failure_never_invokes_body(self):
        original=self.authority.hold;failed=[];calls=[]
        @contextmanager
        def hold(binding,action):
            with original(binding,action):yield None
            if action=='execute' and not failed:failed.append(1);raise PermissionError('exit unknown')
        self.authority.hold=hold
        receipt=await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
        self.assertEqual(calls,[]);self.assertEqual(receipt['state'],'UNKNOWN');self.assertTrue(receipt['retired'])

    async def test_describe_has_no_claim_and_matches_wire_reservation(self):
        descriptor=self.pair.describe(0,self.spec)
        self.assertEqual(set(descriptor),{'command_id','reservation_id','binding','charge'})
        self.assertEqual(self.resources.calls,0)
        self.assertEqual(self.journal._db.execute('select count(*) from control_calls').fetchone(),(0,))
        receipt=await self.pair.dispatch(0,self.spec,lambda:None)
        self.assertEqual({k:receipt[k] for k in descriptor},descriptor)

    async def test_minimum_planner_charge_and_missing_resources_deny_before_wire(self):
        with self.assertRaises(ValueError):
            self.pair.check(self.spec,self.assignments,declaration_digest='c'*64,minimum_charge=65)
        self.assertEqual(self.journal._db.execute('select count(*) from control_calls').fetchone(),(0,))
        self.assertFalse(any(f[:1]==b'D' for channel in self.channels for f in channel.w.frames))

    async def test_missing_identity_or_resource_provider_denies_before_wire(self):
        self.services[0].identity=None
        with self.assertRaises(ValueError):await self.pair.dispatch(0,self.spec,lambda:None)
        self.services[0].identity=self.identity;self.journal.resources=None
        with self.assertRaises(Conflict):await self.pair.dispatch(0,self.spec,lambda:None)
        self.assertEqual(self.journal._db.execute('select count(*) from control_calls').fetchone(),(0,))
        self.assertFalse(any(f[:1]==b'D' for channel in self.channels for f in channel.w.frames))

    async def test_lost_execute_reply_keeps_committed_history_without_reexecution(self):
        channel=self.services[0].channel;original=channel.send;sent=[];calls=[]
        async def send(*args,**kwargs):
            sent.append(1)
            if len(sent)==2:raise ConnectionError('reply lost after commit')
            return await original(*args,**kwargs)
        with patch.object(channel,'send',side_effect=send):
            with self.assertRaises(ConnectionError):await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
        self.assertEqual(self.journal._db.execute('select state,charge from control_calls').fetchone(),('COMMITTED',64))
        with self.assertRaises(ValueError):await self.pair.dispatch(0,self.spec,lambda:calls.append(2))
        self.assertEqual(calls,[1])


if __name__=='__main__':unittest.main()
