"""Ordinary SQLite/memory Channels; providers and TLS are explicit test doubles."""
import asyncio,hashlib,tempfile,unittest,sqlite3,os
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from cluster_p1.channel import Channel,Grant
from cluster_p1.c_receiving import ReceivingStore
from cluster_p1.c_receiving_history import ReceivingHistoryClient,ReceivingHistoryService
from validation.role_bridge.test_transport import streams,config

class Identity:
    def __init__(self):self.principal='current-user';self.active=0
    @contextmanager
    def hold(self,identity):
        self.active+=1
        try:yield self.principal
        finally:self.active-=1
class Authority:
    def __init__(self):self.revoked=False;self.active=0;self.seen=[]
    @contextmanager
    def hold(self,principal,identity,descriptor,action):
        if self.revoked or principal!='current-user' or identity.node_id!='new-client' or action!='query':raise PermissionError('no history access')
        self.active+=1;self.seen.append(descriptor.copy())
        try:yield None
        finally:self.active-=1

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.d=dict(version=1,purpose='result-receive',owner='old-owner',task='task',session='session',epoch=1,model='1'*64,layout='2'*64,plan_digest='3'*64,input_snapshot='4'*64,token_digest='5'*64,preprocessing='6'*64,hidden=24,vocab=100,consent_id='consent',consent_digest='7'*64,revision=1,c_node='c',b_node='b',resource_id='capacity',charge=33168,receiver=dict(version=1,node='c',purpose='result-store',max_charge_bytes=33168),b_placement=dict(node='b',stage=1),declaration_digest='8'*64,authority_incarnation='9'*32)
        self.store=ReceivingStore(self.root/'c.db');self.addCleanup(self.store.close)
        self.store.pending(self.d,'a'*64,'b'*64,self.store.incarnation)
        self.before=tuple(self.store.db.iterdump());self.changes=self.store.db.total_changes
        cr,cw,sr,sw=streams('new-client','c')
        g=Grant('task','session',1,1,(0,1),'1'*64,2)
        self.cc=Channel(cr,cw,config('c'),{'c':{g}},capacity_bytes=32768,capacity_tiles=1)
        self.sc=Channel(sr,sw,config('new-client'),{'new-client':{g}},capacity_bytes=32768,capacity_tiles=1)
        self.identity=Identity();self.authority=Authority();self.jobs=[]
        self.client=ReceivingHistoryClient(self.cc)
        self.service=ReceivingHistoryService(self.sc,self.store,identity=self.identity,authorize=self.authority)
        await asyncio.gather(self.cc.start(),self.sc.start())
    async def asyncTearDown(self):
        for task in self.jobs:
            if not task.done():task.cancel()
        await asyncio.gather(*self.jobs,return_exceptions=True)
        await asyncio.gather(self.client.close(),self.service.close(),return_exceptions=True)
    def unchanged(self):
        self.assertEqual(tuple(self.store.db.iterdump()),self.before);self.assertEqual(self.store.db.total_changes,self.changes)
    async def query(self,d=None):
        jobs=[asyncio.create_task(self.service.serve_once()),asyncio.create_task(self.client.query(d or self.d))];self.jobs+=jobs
        try:
            _,receipt=await asyncio.wait_for(asyncio.gather(*jobs),3);return receipt
        except BaseException:
            for task in jobs:
                if not task.done():task.cancel()
            await asyncio.gather(*jobs,return_exceptions=True);raise
    async def test_current_principal_reads_original_binding_no_qualification(self):
        result=await self.query();self.assertEqual(result['connection'],'a'*64)
        self.assertEqual(result['descriptor'],self.d)
        self.assertFalse(result['receiving_allowed']);self.assertFalse(result['execution_allowed']);self.assertFalse(result['resource_verified'])
        self.assertEqual(self.authority.seen[-1]['owner'],'old-owner');self.unchanged()
        with self.assertRaises(sqlite3.OperationalError):self.service._history.execute('DELETE FROM c_receiving')
    async def test_unknown_row_still_authorized(self):
        d=dict(self.d,owner='absent');self.assertIsNone(await self.query(d));self.assertEqual(self.authority.seen[-1],d);self.unchanged()
    async def test_missing_authorizer_denies_and_closes_readonly_connection(self):
        self.service.authorize=None
        with self.assertRaises(Exception):await self.query()
        self.assertIsNone(self.service._history);self.unchanged()
    async def test_wrong_principal_denies(self):
        self.identity.principal='intruder'
        with self.assertRaises(Exception):await self.query()
        self.unchanged()
    async def test_descriptor_substitution_denies(self):
        with self.assertRaises(Exception):await self.query(dict(self.d,consent_digest='f'*64))
        self.unchanged()
    async def test_repeated_queries_do_not_consume(self):
        for _ in range(3):self.assertEqual((await self.query())['state'],'PENDING')
        self.unchanged()
    async def test_reopened_store_only_history(self):
        self.service._close_db()
        reopened=ReceivingStore(self.root/'c.db');self.addCleanup(reopened.close)
        self.assertFalse(reopened.fresh)
        # Constructor's store locator is a capability, not a writable permission.
        self.service=ReceivingHistoryService(self.sc,reopened,identity=self.identity,authorize=self.authority)
        self.assertEqual((await self.query())['state'],'PENDING')
        self.assertEqual(self.service._history.execute('PRAGMA query_only').fetchone(),(1,))
        self.unchanged()
    async def test_schema_change_rejected(self):
        self.store.db.execute('CREATE TABLE changed(x)')
        with self.assertRaises(Exception):await self.query()
        self.assertIsNone(self.service._history)
    async def test_inode_replacement_rejected(self):
        path=self.store.path;path.rename(self.root/'old.db');sqlite3.connect(path).close()
        with self.assertRaises(Exception):await self.query()
    async def test_revoke_while_waiting_write_lock_blocks_reply(self):
        entered=asyncio.Event();original=self.sc.send
        async def delayed(*args,**kwargs):
            await self.sc._write.acquire();entered.set();return await original(*args,**kwargs)
        with patch.object(self.sc,'send',delayed):
            job=asyncio.create_task(self.query());self.jobs.append(job)
            await asyncio.wait_for(entered.wait(),3)
            self.assertEqual(self.identity.active,0);self.assertEqual(self.authority.active,0)
            self.authority.revoked=True;self.sc._write.release()
            with self.assertRaises(Exception):await job
        self.assertFalse(any(frame[:1]==b'D' for frame in self.sc.w.frames));self.unchanged()
    async def test_drain_never_holds_authorization(self):
        original=self.sc.w.drain
        async def checked():
            self.assertEqual(self.identity.active,0);self.assertEqual(self.authority.active,0);return await original()
        with patch.object(self.sc.w,'drain',checked):await self.query()
        self.unchanged()
    async def test_cancel_pending_receive_closes_ro_and_reader(self):
        event=asyncio.Event();original=self.sc.receive
        async def receive():event.set();return await original()
        with patch.object(self.sc,'receive',receive):
            task=asyncio.create_task(self.service.serve_once());self.jobs.append(task)
            await asyncio.wait_for(event.wait(),3);task.cancel()
            await asyncio.gather(task,return_exceptions=True)
        self.assertIsNone(self.service._history);self.assertTrue(self.sc._task.done());self.unchanged()

    async def test_unknown_row_denial_does_not_reveal_absence(self):
        self.authority.revoked=True
        with self.assertRaises(Exception):await self.query(dict(self.d,owner='absent'))
        self.unchanged()
    async def test_pid_change_rejected(self):
        with patch('cluster_p1.c_receiving_history.os.getpid',return_value=os.getpid()+1):
            with self.assertRaises(Exception):await self.query()
        self.unchanged()
    async def test_principal_changes_before_actual_write_denied(self):
        original=self.sc.send
        async def changed(*args,**kwargs):
            self.identity.principal='different-user';return await original(*args,**kwargs)
        with patch.object(self.sc,'send',changed):
            with self.assertRaises(Exception):await self.query()
        self.assertFalse(any(frame[:1]==b'D' for frame in self.sc.w.frames));self.unchanged()
    async def test_history_reader_never_calls_store_mutations_or_resources(self):
        def forbidden(*args,**kwargs):raise AssertionError('history called write path')
        with patch.object(self.store,'pending',forbidden),patch.object(self.store,'move',forbidden),patch.object(self.store,'unknown',forbidden),patch.object(self.store,'_tx',forbidden):
            await self.query()
        self.unchanged()

if __name__=='__main__':unittest.main()
