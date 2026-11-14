"""Query-only actual SQLite/Channels; fake SSL is not a real handshake."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from cluster_p1.channel import Channel, Grant, ChannelError
from cluster_p1.control_journal import ControlJournal, Conflict
from validation.role_bridge.test_transport import streams, config
try:
    from cluster_p1.history_query import HistoryClient, HistoryService
except ImportError:
    HistoryClient=HistoryService=None


class Resources:
    def __init__(self):self.calls=0
    @contextmanager
    def hold(self,*args):self.calls+=1;yield None


class Identity:
    def __init__(self):self.principal='owner';self.active=0;self.seen=[]
    @contextmanager
    def hold(self,current):
        self.seen.append(current);self.active+=1
        try:yield self.principal
        finally:self.active-=1


class Authorize:
    def __init__(self):self.revoked=False;self.active=0;self.seen=[]
    @contextmanager
    def hold(self,principal,current,binding,action):
        if self.revoked or principal!=binding['owner'] or current.node_id!='new-client' or action!='query':
            raise PermissionError('query access denied')
        self.seen.append((principal,current,binding.copy(),action));self.active+=1
        try:yield None
        finally:self.active-=1


class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(HistoryService,'history query implementation missing')
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.resources=Resources();fresh=ControlJournal(self.root/'history.db',1024,resources=self.resources)
        self.bindings=[]
        for n in range(4):
            binding=dict(owner='owner',task='task'+str(n),session='old-session',epoch=1,stage=0,
                         input_snapshot='a'*64,model='a'*64,layout='a'*64,plan='a'*64,input_digest='b'*64,
                         device='device-old',instance='old-instance',peer='f'*64,declaration_digest='c'*64,
                         token_digest='d'*64,preprocessing='e'*64)
            self.bindings.append(binding);fresh.prepare('cmd'+str(n),'rid'+str(n),binding,64)
            if n in (1,2):fresh.consume('cmd'+str(n),'rid'+str(n),binding)
            if n==2:fresh.complete('cmd2','rid2',binding,'9'*64)
            if n==3:fresh.fail('cmd3','rid3',binding)
        fresh.close();self.resource_calls=self.resources.calls
        self.journal=ControlJournal(self.root/'history.db',1024)
        self.addCleanup(self.journal.close)
        self.before=tuple(self.journal._db.iterdump());self.original_changes=self.journal._db.total_changes
        grants={Grant(b['task'],b['session'],1,0,(0,1),b['model'],2) for b in self.bindings}
        cr,cw,sr,sw=streams('new-client','history-node')
        self.client_channel=Channel(cr,cw,config('history-node'),{'history-node':grants},capacity_bytes=32768,capacity_tiles=1)
        self.server_channel=Channel(sr,sw,config('new-client'),{'new-client':grants},capacity_bytes=32768,capacity_tiles=1)
        self.identity=Identity();self.authorize=Authorize()
        self.client=HistoryClient(self.client_channel)
        self.service=HistoryService(self.server_channel,self.journal,identity=self.identity,authorize=self.authorize)
        self.tasks=[]
        await asyncio.gather(self.client_channel.start(),self.server_channel.start())
    async def asyncTearDown(self):
        for task in getattr(self,'tasks',[]):
            if not task.done():task.cancel()
        if getattr(self,'tasks',None):await asyncio.gather(*self.tasks,return_exceptions=True)
        if hasattr(self,'service'):await self.service.close()
        if hasattr(self,'client'):await self.client.close()
    async def query(self,command,binding):
        server=asyncio.create_task(self.service.serve_once())
        client=asyncio.create_task(self.client.query(command,binding));self.tasks.extend((server,client))
        try:
            _,receipt=await asyncio.gather(server,client);return receipt
        except BaseException:
            for task in (server,client):
                if not task.done():task.cancel()
            await asyncio.gather(server,client,return_exceptions=True)
            raise
    def unchanged(self):
        self.assertEqual(tuple(self.journal._db.iterdump()),self.before)
        self.assertEqual(self.journal._db.total_changes,self.original_changes)
        self.assertEqual(self.service._history.total_changes,0)
        self.assertEqual(self.resources.calls,self.resource_calls)
    async def test_all_reopened_states_are_history_without_execution_qualification(self):
        for n,state in enumerate(('RESERVED','CONSUMED','COMMITTED','UNKNOWN')):
            result=await self.query('cmd'+str(n),self.bindings[n])
            self.assertEqual(result['state'],state)
            self.assertFalse(result['execution_allowed']);self.assertFalse(result['resource_verified'])
            self.assertEqual(result['binding']['peer'],'f'*64)
        self.assertEqual(self.identity.seen[0].node_id,'new-client')
        self.assertNotEqual(self.identity.seen[0].certificate_sha256,'f'*64)
        with self.assertRaises(Conflict):self.journal.consume('cmd0','rid0',self.bindings[0])
        self.unchanged()
    async def test_missing_id_requires_permission_and_creates_nothing(self):
        self.assertIsNone(await self.query('missing',self.bindings[0]))
        self.assertTrue(self.authorize.seen);self.unchanged()
    async def test_missing_identity_defaults_deny(self):
        self.service.identity=None
        with self.assertRaises((ValueError,PermissionError,ChannelError)):
            await self.query('cmd0',self.bindings[0])
        self.unchanged()
    async def test_different_current_principal_cannot_read_original_owner(self):
        self.identity.principal='other-owner'
        with self.assertRaises((PermissionError,ChannelError)):
            await self.query('cmd0',self.bindings[0])
        self.unchanged()
    async def test_wrong_original_binding_rejected(self):
        wrong=dict(self.bindings[0],peer='0'*64)
        with self.assertRaises((ValueError,ChannelError)):
            await self.query('cmd0',wrong)
        self.unchanged()
    async def test_query_service_uses_independent_sqlite_readonly_connection(self):
        with self.assertRaises(sqlite3.OperationalError):
            self.service._history.execute('delete from control_calls')
        with patch.object(self.journal,'query',side_effect=AssertionError('must use RO SELECT')):
            self.assertEqual((await self.query('cmd0',self.bindings[0]))['state'],'RESERVED')
        self.assertEqual(self.service._history.execute('pragma query_only').fetchone(),(1,))
        self.unchanged()
    async def test_execute_action_and_extra_fields_are_never_dispatched(self):
        original=self.client_channel.send
        async def send(envelope,**kw):
            request=json.loads(envelope.payload);request['action']='execute';request['operation']='forbidden'
            raw=json.dumps(request,sort_keys=True,separators=(',',':')).encode()
            return await original(replace(envelope,payload=raw,tensor_shape=(len(raw),)),**kw)
        with patch.object(self.client_channel,'send',side_effect=send):
            with self.assertRaises((ValueError,ChannelError)):await self.query('cmd0',self.bindings[0])
        self.unchanged()
    async def test_revoke_while_credit_release_waits_prevents_response(self):
        entered=asyncio.Event();resume=asyncio.Event();original=self.server_channel._release
        async def release(size):
            self.assertEqual((self.identity.active,self.authorize.active),(0,0))
            entered.set();await resume.wait();await original(size)
        with patch.object(self.server_channel,'_release',side_effect=release):
            task=asyncio.create_task(self.query('cmd0',self.bindings[0]));self.tasks.append(task)
            try:
                await asyncio.wait_for(entered.wait(),3);self.authorize.revoked=True
            finally:resume.set()
            with self.assertRaises((PermissionError,ChannelError)):await task
        self.assertFalse(any(raw[:1]==b'D' for raw in self.server_channel.w.frames));self.unchanged()
    async def test_duplicate_queries_do_not_create_execution_or_resource_calls(self):
        first=await self.query('cmd0',self.bindings[0]);second=await self.query('cmd0',self.bindings[0])
        self.assertEqual(first,second);self.unchanged()

    async def test_absent_id_and_missing_permission_still_deny(self):
        self.service.authorize=None
        with self.assertRaises((ValueError,ChannelError)):
            await self.query('missing',self.bindings[0])
        self.assertEqual(self.identity.seen,[]);self.unchanged()

    async def test_cancel_pending_service_receive_closes_without_history_mutation(self):
        entered=asyncio.Event();original=self.server_channel.receive
        async def receive():entered.set();return await original()
        with patch.object(self.server_channel,'receive',side_effect=receive):
            task=asyncio.create_task(self.service.serve_once());self.tasks.append(task)
            await asyncio.wait_for(entered.wait(),3);task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(self.server_channel._closed and self.server_channel._task.done())
        self.unchanged()

    async def test_concurrent_same_client_query_rejects_without_disturbing_first(self):
        entered=asyncio.Event();resume=asyncio.Event();original=self.server_channel._release
        async def release(size):entered.set();await resume.wait();await original(size)
        with patch.object(self.server_channel,'_release',side_effect=release):
            first=asyncio.create_task(self.query('cmd0',self.bindings[0]));self.tasks.append(first)
            try:
                await asyncio.wait_for(entered.wait(),3)
                with self.assertRaises(ValueError):await self.client.query('cmd0',self.bindings[0])
            finally:resume.set()
            self.assertEqual((await first)['state'],'RESERVED')
        self.assertEqual(sum(raw[:1]==b'D' for raw in self.client_channel.w.frames),1)
        self.unchanged()

    async def test_boolean_sequence_rejected(self):
        original=self.client_channel.send
        async def send(envelope,**kw):
            request=json.loads(envelope.payload);request['sequence']=False
            raw=json.dumps(request,sort_keys=True,separators=(',',':')).encode()
            return await original(replace(envelope,payload=raw,tensor_shape=(len(raw),)),**kw)
        with patch.object(self.client_channel,'send',side_effect=send):
            with self.assertRaises((ValueError,ChannelError)):await self.query('cmd0',self.bindings[0])
        self.assertEqual(self.identity.seen,[]);self.unchanged()


if __name__=='__main__':unittest.main()
