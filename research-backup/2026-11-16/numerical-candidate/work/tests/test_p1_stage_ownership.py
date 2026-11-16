import asyncio
import threading
import unittest
from cluster_p1.stage_ownership import StageOwner, StageBinding, OwnershipError

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls=[];self.commits=[];self.auths=[]
        self.binding=StageBinding('t','s',1,0,'a'*64,'b'*64)
        def operation(value,*,token_start,stage_id):
            self.calls.append((value,token_start,stage_id));return b'output'
        self.operation=operation
    def owner(self,operation=None,authorize=None,commit=None):
        def auth(spec):self.auths.append(spec);return True
        def write(spec,result):self.commits.append((spec,result))
        o=StageOwner(self.binding,operation or self.operation,authorize=authorize or auth,commit=commit or write)
        self.addAsyncCleanup(o.close)
        return o
    async def test_success_advances_only_after_commit(self):
        o=self.owner();self.assertEqual(await o.call(b'in',token_start=0,token_count=2),b'output')
        self.assertEqual(o.state.next_token,2);self.assertEqual(o.state.status,'idle')
        self.assertFalse(o.state.execution_allowed)
        await o.call(b'next',token_start=2,token_count=1)
        self.assertEqual(len(self.calls),2);self.assertEqual(len(self.commits),2)
    async def test_wrong_prefix_does_not_call(self):
        o=self.owner()
        with self.assertRaises(OwnershipError):await o.call(b'in',token_start=1,token_count=2)
        self.assertEqual(self.calls,[])
    async def test_auth_failure_retires(self):
        o=self.owner(authorize=lambda spec:False)
        with self.assertRaises(OwnershipError):await o.call(b'in',token_start=0,token_count=2)
        self.assertEqual(self.calls,[]);self.assertEqual(o.state.status,'retired')
    async def test_commit_failure_no_blind_recompute(self):
        e=RuntimeError('commit uncertain')
        def commit(*args):raise e
        o=self.owner(commit=commit)
        with self.assertRaises(RuntimeError) as caught:await o.call(b'in',token_start=0,token_count=2)
        self.assertIs(caught.exception,e);self.assertEqual(o.state.status,'retired')
        self.assertEqual(o.state.commit,'unconfirmed')
        with self.assertRaises(OwnershipError):await o.call(b'in',token_start=0,token_count=2)
        self.assertEqual(len(self.calls),1)
    def blocking(self):
        loop=asyncio.get_running_loop();entered=asyncio.Event();release=threading.Event()
        def operation(value,**kw):
            self.calls.append(value);loop.call_soon_threadsafe(entered.set)
            if not release.wait(3):raise RuntimeError('test barrier timeout')
            return b'late'
        return operation,entered,release
    async def test_cancel_waits_discards_and_no_reset(self):
        op,entered,release=self.blocking();o=self.owner(op)
        t=asyncio.create_task(o.call(b'in',token_start=0,token_count=2));await entered.wait()
        try:
            t.cancel();await asyncio.sleep(0);t.cancel();await asyncio.sleep(0)
            self.assertFalse(t.done());self.assertEqual(o.state.status,'retiring')
        finally:release.set()
        with self.assertRaises(asyncio.CancelledError):await t
        self.assertEqual(o.state.status,'retired');self.assertEqual(self.commits,[])
    async def test_close_and_exclusive_busy(self):
        op,entered,release=self.blocking();o=self.owner(op)
        t=asyncio.create_task(o.call(b'in',token_start=0,token_count=2));await entered.wait()
        try:
            with self.assertRaises(OwnershipError):await o.call(b'in',token_start=0,token_count=2)
            close=asyncio.create_task(o.close());await asyncio.sleep(0)
            self.assertFalse(close.done())
        finally:release.set()
        with self.assertRaises(OwnershipError):await t
        await close;await o.close()
        self.assertEqual(o.state.status,'retired');self.assertEqual(self.commits,[])
    async def test_compute_failure_retires(self):
        def operation(*a,**k):raise RuntimeError('operation')
        o=self.owner(operation)
        with self.assertRaises(RuntimeError):await o.call(b'in',token_start=0,token_count=1)
        self.assertEqual(o.state.status,'retired');self.assertEqual(self.commits,[])
    async def test_close_cancellation_still_waits_for_thread(self):
        op,entered,release=self.blocking();o=self.owner(op)
        t=asyncio.create_task(o.call(b'in',token_start=0,token_count=1));await entered.wait()
        c=asyncio.create_task(o.close());await asyncio.sleep(0);c.cancel();await asyncio.sleep(0)
        try:self.assertFalse(c.done());self.assertFalse(t.done())
        finally:release.set()
        with self.assertRaises(OwnershipError):await t
        with self.assertRaises(asyncio.CancelledError):await c
        self.assertEqual(o.state.status,'retired')
    async def test_authorize_and_commit_remain_on_owning_thread(self):
        own=threading.get_ident();seen=[]
        def auth(spec):seen.append(('auth',threading.get_ident()));return True
        def op(*args,**kwargs):seen.append(('op',threading.get_ident()));return b'ok'
        def commit(*args):seen.append(('commit',threading.get_ident()))
        o=self.owner(op,auth,commit);await o.call(b'in',token_start=0,token_count=1)
        self.assertEqual(seen[0],('auth',own));self.assertEqual(seen[-1],('commit',own))
        self.assertNotEqual(seen[1][1],own)
    async def test_coroutine_authorization_rejected_without_execution(self):
        executed=[]
        async def accidental():executed.append(True);return True
        o=self.owner(authorize=lambda spec:accidental())
        with self.assertRaises(OwnershipError):await o.call(b'in',token_start=0,token_count=1)
        self.assertEqual(executed,[]);self.assertEqual(self.calls,[])
    async def test_cancel_plus_worker_error_preserves_both(self):
        op,entered,release=self.blocking();error=RuntimeError('worker failed')
        def failing(*args,**kwargs):op(*args,**kwargs);raise error
        o=self.owner(failing);t=asyncio.create_task(o.call(b'in',token_start=0,token_count=1));await entered.wait()
        t.cancel();await asyncio.sleep(0);release.set()
        with self.assertRaises(asyncio.CancelledError) as caught:await t
        self.assertIs(caught.exception.__cause__,error)
        self.assertEqual(self.commits,[])
    async def test_started_coroutine_rejected_without_driving_finally(self):
        import inspect
        closed=[]
        class Pause:
            def __await__(self):yield 'pause'
        async def existing():
            try:await Pause()
            finally:closed.append(threading.get_ident())
        for boundary in ('authorize','operation','commit'):
            coroutine=existing();coroutine.send(None)
            self.assertEqual(inspect.getcoroutinestate(coroutine),inspect.CORO_SUSPENDED)
            kwargs={boundary:lambda *a,**k:coroutine}
            # owner helper names operation, authorize and commit match the boundary.
            o=self.owner(**kwargs)
            try:
                with self.assertRaises(OwnershipError):await o.call(b'in',token_start=0,token_count=1)
                self.assertEqual(inspect.getcoroutinestate(coroutine),inspect.CORO_SUSPENDED)
                self.assertEqual(closed,[])
            finally:
                coroutine.close();closed.clear()
