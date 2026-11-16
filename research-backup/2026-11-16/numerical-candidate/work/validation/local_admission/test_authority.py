"""Ordinary synthetic identities only; no genuine IdP or production integration."""
import asyncio
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import private_local_admission as m

class SyntheticAdapter(m.IdentityAdapter):
    async def observe(self, credential):
        if credential != 'synthetic-alice':
            raise m.AdmissionRejected()
        return m.Observation('synthetic-declaration-test','alice','session-a','test-audience',time.time()+60)

class Checks(unittest.IsolatedAsyncioTestCase):
    async def test_hold_first_detects_replacement_and_restore_cannot_revive(self):
        await self.prepare()
        old=self.path.with_suffix('.old')
        self.path.rename(old);self.path.write_bytes(old.read_bytes())
        with self.assertRaises(m.AdmissionRejected):
            with self.a.hold('synthetic-alice'):pass
        self.assertEqual(self.a.state,'POISONED')
        self.path.unlink();old.rename(self.path)
        self.assertIsNone(self.a.verify('synthetic-alice'))
        with self.assertRaises(m.AdmissionRejected):await self.prepare()

    async def test_missing_path_poison_does_not_acquire_writer_lock(self):
        await self.prepare();self.path.rename(self.path.with_suffix('.old'))
        class ForbiddenLock:
            def __enter__(self):raise AssertionError('verify acquired W')
            def __exit__(self,*args):pass
        with patch.object(self.a,'_w',ForbiddenLock()):
            self.assertIsNone(self.a.verify('synthetic-alice'))
        self.assertEqual(self.a.state,'POISONED')

    async def test_fence_exit_detects_path_replacement(self):
        await self.prepare()
        with self.assertRaises(m.AdmissionRejected):
            with self.a.hold('synthetic-alice'):
                old=self.path.with_suffix('.old')
                self.path.rename(old);self.path.write_bytes(old.read_bytes())
        self.assertEqual(self.a.state,'POISONED')

    async def test_expired_observation_requires_new_adapter_observation(self):
        await self.prepare();now=time.time()
        with patch.object(m.time,'time',return_value=now+120):
            with self.assertRaises(m.AdmissionRejected):
                with self.a.hold('synthetic-alice'):pass
        with patch.object(m.time,'time',return_value=now):
            self.assertIsNone(self.a.verify('synthetic-alice'))
            self.assertEqual(self.a.state,'READY')
            with patch.object(self.a._adapter,'observe',wraps=self.a._adapter.observe) as observed:
                await self.prepare()
                observed.assert_awaited_once_with('synthetic-alice')
            self.assertIsNotNone(self.a.verify('synthetic-alice'))

    async def test_cached_identity_denies_after_path_replacement(self):
        await self.prepare()
        old = self.path.with_suffix('.old')
        self.path.rename(old)
        self.path.write_bytes(old.read_bytes())
        self.assertIsNone(self.a.verify('synthetic-alice'))
        with self.assertRaises(m.AdmissionRejected):
            with self.a.hold('synthetic-alice'):
                pass

    async def test_observed_expiry_cannot_revive_after_clock_rollback(self):
        await self.prepare()
        now = time.time()
        with patch.object(m.time, 'time', return_value=now + 120):
            self.assertIsNone(self.a.verify('synthetic-alice'))
        with patch.object(m.time, 'time', return_value=now):
            self.assertIsNone(self.a.verify('synthetic-alice'))
            with self.assertRaises(m.AdmissionRejected):
                with self.a.hold('synthetic-alice'):
                    pass

    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'identity.sqlite'
        self.a=m.LocalAdmission.create(self.path,issuer='synthetic-declaration-test',audience='test-audience',adapter=SyntheticAdapter())
        async def cleanup():
            if self.a.state != 'CLOSE_FAILED':await self.a.close()
        self.addAsyncCleanup(cleanup)
    async def prepare(self):return await self.a.prepare_user('synthetic-alice')
    def rows(self):
        c=sqlite3.connect(self.path)
        try:return c.execute('select count(*) from events').fetchone()[0]
        finally:c.close()
    async def test_default_deny(self):
        self.a._adapter=None
        self.assertIsNone(self.a.verify('synthetic-alice'))
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),0)
    async def test_prepare_and_revoke(self):
        await self.prepare();self.assertEqual(self.a.verify('synthetic-alice').subject,'alice')
        await self.a.revoke_subject('alice');self.assertIsNone(self.a.verify('synthetic-alice'))
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),2)
    async def test_expiry(self):
        await self.prepare()
        with patch.object(m.time,'time',return_value=time.time()+120):self.assertIsNone(self.a.verify('synthetic-alice'))
    async def test_history_reopen_never_admits(self):
        await self.prepare();await self.a.close()
        reopened=m.LocalAdmission.open_existing(self.path);self.addAsyncCleanup(reopened.close)
        self.assertEqual(len(reopened.history()),1)
        self.assertIsNone(reopened.verify('synthetic-alice'))
        with self.assertRaises(m.AdmissionRejected):await reopened.prepare_user('synthetic-alice')
    async def test_single_live(self):
        with self.assertRaises(m.AdmissionRejected):m.LocalAdmission.open_existing(self.path)
    async def test_pid_mismatch(self):
        await self.prepare()
        with patch.object(m.os,'getpid',return_value=-1):
            self.assertIsNone(self.a.verify('synthetic-alice'))
            with self.assertRaises(m.AdmissionRejected):await self.prepare()
    async def test_old_observation_after_revoke(self):
        started=asyncio.Event();release=asyncio.Event();base=self.a._adapter
        class Delayed(m.IdentityAdapter):
            async def observe(self,credential):
                result=await base.observe(credential);started.set();await release.wait();return result
        self.a._adapter=Delayed();job=asyncio.create_task(self.prepare());await started.wait()
        await self.a.revoke_subject('alice');release.set()
        with self.assertRaises(m.AdmissionRejected):await job
        self.assertEqual(self.rows(),1)
    async def test_old_observation_after_policy(self):
        started=asyncio.Event();release=asyncio.Event();base=self.a._adapter
        class Delayed(m.IdentityAdapter):
            async def observe(self,c):
                result=await base.observe(c);started.set();await release.wait();return result
        self.a._adapter=Delayed();job=asyncio.create_task(self.prepare());await started.wait()
        await self.a.advance_policy();release.set()
        with self.assertRaises(m.AdmissionRejected):await job
    async def test_prepare_cannot_resurrect_closed(self):
        started=asyncio.Event();release=asyncio.Event();base=self.a._adapter
        class Delayed(m.IdentityAdapter):
            async def observe(self,c):
                result=await base.observe(c);started.set();await release.wait();return result
        self.a._adapter=Delayed();job=asyncio.create_task(self.prepare());await started.wait()
        await self.a.close();release.set()
        with self.assertRaises(m.AdmissionRejected):await job
        self.assertEqual(self.a.state,'CLOSED');self.assertEqual(self.rows(),0)
    async def blocked_write(self, action):
        entered=threading.Event();release=threading.Event();original=self.a._commit
        def block(c):
            entered.set()
            if not release.wait(5):raise RuntimeError('barrier timeout')
            original(c)
        self.a._commit=block
        job=asyncio.create_task(action())
        self.assertTrue(await asyncio.to_thread(entered.wait,3))
        return job,release
    async def test_busy_denies_and_close_drains_before_unlock(self):
        job,release=await self.blocked_write(self.prepare)
        self.assertEqual(self.a.state,'BUSY');self.assertIsNone(self.a.verify('synthetic-alice'))
        closing=asyncio.create_task(self.a.close());await asyncio.sleep(0)
        with self.assertRaises(m.AdmissionRejected):m.LocalAdmission.open_existing(self.path)
        release.set()
        with self.assertRaises(m.AdmissionRejected):await asyncio.wait_for(job,3)
        await asyncio.wait_for(closing,3);self.assertEqual(self.a.state,'CLOSED')
        self.assertEqual(self.rows(),1)
    async def test_cancel_drains_and_poison(self):
        job,release=await self.blocked_write(self.prepare);job.cancel();await asyncio.sleep(0)
        self.assertFalse(job.done());release.set()
        with self.assertRaises(asyncio.CancelledError):await job
        self.assertEqual(self.a.state,'POISONED');self.assertIsNone(self.a.verify('synthetic-alice'))
    async def test_before_commit_fault(self):
        def fail(c):raise RuntimeError('injected before commit')
        self.a._commit=fail
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),0);self.assertEqual(self.a.state,'POISONED')
    async def test_after_commit_fault_preserves_history(self):
        original=self.a._commit
        def fail(c):original(c);raise RuntimeError('injected after commit')
        self.a._commit=fail
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),1);self.assertEqual(self.a.state,'POISONED')
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
    async def test_session_revocation(self):
        await self.prepare();await self.a.revoke_session('alice','session-a')
        self.assertIsNone(self.a.verify('synthetic-alice'))
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
    async def test_fence_propagates_body_failure(self):
        await self.prepare()
        with self.assertRaisesRegex(RuntimeError,'body'):
            with self.a.hold('synthetic-alice'):raise RuntimeError('body')
    async def test_adapter_wrong_pins(self):
        class Wrong(m.IdentityAdapter):
            async def observe(self,c):return m.Observation('wrong','alice','s','test-audience',time.time()+60)
        self.a._adapter=Wrong()
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),0)
    async def test_close_cancellation_waits_for_writer(self):
        job,release=await self.blocked_write(self.prepare)
        closing=asyncio.create_task(self.a.close());await asyncio.sleep(0)
        closing.cancel();await asyncio.sleep(0);closing.cancel();await asyncio.sleep(0)
        self.assertFalse(closing.done());release.set()
        with self.assertRaises(m.AdmissionRejected):await job
        with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(closing,3)
        self.assertEqual(self.a.state,'CLOSED')
    async def test_local_fence_orders_revoke_entry(self):
        await self.prepare();entered=threading.Event();release=threading.Event()
        def held():
            with self.a.hold('synthetic-alice'):
                entered.set()
                if not release.wait(3):raise RuntimeError('timeout')
        job=asyncio.create_task(asyncio.to_thread(held))
        self.assertTrue(await asyncio.to_thread(entered.wait,2))
        # Start update in another event-loop thread: otherwise its sync F wait
        # blocks this loop and cannot service our release (documented boundary).
        def revoke():asyncio.run(self.a.revoke_subject('alice'))
        update=asyncio.create_task(asyncio.to_thread(revoke))
        await asyncio.sleep(0);self.assertFalse(update.done());release.set()
        await job;await update;self.assertIsNone(self.a.verify('synthetic-alice'))
    async def test_post_commit_generation_change_denies_publication(self):
        original=self.a._commit
        def expire(c):
            original(c)
            self.a._generation+=1 # explicit publication-generation fault injection
        self.a._commit=expire
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),1);self.assertEqual(self.a.state,'POISONED')
    async def test_bad_credential_is_not_a_poison_event(self):
        await self.prepare()
        with self.assertRaises(m.AdmissionRejected):await self.a.prepare_user('bad')
        self.assertEqual(self.a.state,'READY');self.assertIsNotNone(self.a.verify('synthetic-alice'))
    async def test_pid_rejection_precedes_lock_access(self):
        class ForbiddenLock:
            def __enter__(self):raise AssertionError('inherited lock touched')
            def __exit__(self,*a):pass
        with patch.object(self.a,'_f',ForbiddenLock()),patch.object(self.a,'_w',ForbiddenLock()),patch.object(m.os,'getpid',return_value=-1):
            self.assertIsNone(self.a.verify('synthetic-alice'))
            with self.assertRaises(m.AdmissionRejected):
                with self.a.hold('synthetic-alice'):pass
            with self.assertRaises(m.AdmissionRejected):await self.a.close()
            with self.assertRaises(m.AdmissionRejected):self.a.history()
    async def test_replaced_path_never_writes_unlocked_file(self):
        original=self.path.with_suffix('.original')
        self.path.rename(original);self.path.write_bytes(original.read_bytes())
        with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),0);self.assertIsNone(self.a.verify('synthetic-alice'))
    async def test_close_wrapper_failure_consumes_fd_once(self):
        old_fd=self.a._fd;real_close=m.os.close;calls=[]
        def fault(fd):
            calls.append(fd);real_close(fd)
            raise OSError('injected after real close')
        with patch.object(m.os,'close',side_effect=fault):
            with self.assertRaises(m.AdmissionRejected):await self.a.close()
            self.assertIsNone(self.a._fd);self.assertEqual(self.a.state,'CLOSE_FAILED')
            with self.assertRaises(m.AdmissionRejected):await self.a.close()
        self.assertEqual(calls,[old_fd]);self.assertIsNone(self.a.verify('synthetic-alice'))
    async def test_prepare_final_verify_only_once(self):
        original=self.a.verify;calls=[]
        def verify(c):
            calls.append(c)
            return original(c) if len(calls)==1 else None
        with patch.object(self.a,'verify',side_effect=verify):
            result=await self.prepare()
        self.assertIsNotNone(result);self.assertEqual(len(calls),1)
    async def test_connection_close_wrapper_after_commit(self):
        real=self.a._connect
        class Proxy:
            def __init__(self,c):self.c=c
            def __getattr__(self,name):return getattr(self.c,name)
            def close(self):
                self.c.close();raise RuntimeError('injected after sqlite connection close')
        with patch.object(self.a,'_connect',side_effect=lambda mode:Proxy(real(mode))):
            with self.assertRaises(m.AdmissionRejected):await self.prepare()
        self.assertEqual(self.rows(),1);self.assertEqual(self.a.state,'POISONED')
    async def test_pid_change_during_adapter_precedes_lock(self):
        entered=asyncio.Event();release=asyncio.Event();base=self.a._adapter
        class Delayed(m.IdentityAdapter):
            async def observe(self,c):
                result=await base.observe(c);entered.set();await release.wait();return result
        class ForbiddenLock:
            def __enter__(self):raise AssertionError('post-await inherited lock touched')
            def __exit__(self,*args):pass
        self.a._adapter=Delayed();job=asyncio.create_task(self.prepare());await entered.wait()
        with patch.object(self.a,'_f',ForbiddenLock()),patch.object(m.os,'getpid',return_value=-1):
            release.set()
            with self.assertRaises(m.AdmissionRejected):await job

from validation.input_declaration.test_declaration import Checks as SourceFixture, MODEL, CONTRACT
import private_source_bridge as bridge

class BridgeChecks(SourceFixture):
    async def test_bridge_denies_after_identity_path_replacement(self):
        p = Path(self.local._path)
        old = p.with_suffix('.old')
        p.rename(old)
        p.write_bytes(old.read_bytes())
        with self.assertRaises(bridge.SourceBridgeRejected):
            await self.register()
        self.assertEqual(self.claims(), 0)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.local=m.LocalAdmission.create(self.root/'local-identity',issuer='synthetic-declaration-test',audience='test-audience',adapter=SyntheticAdapter())
        self.service._verifier=self.local
        await self.local.prepare_user('synthetic-alice')
        self.b=await bridge._open_source_bridge(service=self.service,model=MODEL,
            preprocessing_version='declared-test-v1',contract_sha256=CONTRACT,
            source_namespace='e'*64,coordinator_path=self.root/'co',identity_fence=self.local,credential='synthetic-alice')
    async def asyncTearDown(self):
        await self.b.close();await self.local.close();await super().asyncTearDown()
    async def register(self):return await self.b.register_current_source('synthetic-alice',self.task.task_id)
    def claims(self):
        c=sqlite3.connect(self.root/'co')
        try:return c.execute('select count(*) from source_claims').fetchone()[0]
        finally:c.close()
    async def test_actual_bridge_success_remains_inert(self):
        result=await self.register();self.assertFalse(result.execution_allowed)
        self.assertEqual(self.claims(),1);self.assertEqual(self.b._co._reservations,{})
    async def test_actual_bridge_busy_after_preflight_permanently_retires(self):
        await self.a._lock.acquire()
        pending=asyncio.create_task(self.register());await asyncio.sleep(0)
        entered=threading.Event();release=threading.Event();original=self.local._commit
        def blocked(c):
            entered.set()
            if not release.wait(5):raise RuntimeError('test timeout')
            original(c)
        self.local._commit=blocked
        update=asyncio.create_task(self.local.prepare_user('synthetic-alice'))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,3))
            self.assertEqual(self.local.state,'BUSY');self.a._lock.release()
            with self.assertRaises(bridge.SourceBridgeRejected):await pending
            self.assertTrue(self.b._failed);self.assertTrue(self.b._co._closed)
            self.assertEqual(self.claims(),0)
        finally:release.set()
        await update;self.assertEqual(self.local.state,'READY')
        with self.assertRaises(bridge.SourceBridgeRejected):await self.register()
    async def test_source_commit_then_identity_failure_has_side_effect(self):
        original=self.b._co._commit
        def fail(c):
            original(c);self.local._poison()
        self.b._co._commit=fail
        with self.assertRaises(bridge.SourceBridgeRejected):await self.register()
        self.assertEqual(self.claims(),1);self.assertTrue(self.b._failed)
        self.assertEqual(self.b._co._reservations,{})

if __name__=='__main__':
    suite=unittest.TestSuite()
    for cls in (Checks,BridgeChecks):
        suite.addTests(cls(name) for name in sorted(cls.__dict__) if name.startswith('test_'))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
