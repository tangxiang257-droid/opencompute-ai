"""Ordinary lifecycle tests; no production providers, signals or networking."""
import asyncio,contextlib,hashlib,io,json,os,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from cluster_p1 import thin_host as host
from cluster_p1.online_issuance_transport import _json
class Tests(unittest.IsolatedAsyncioTestCase):
 def config(self,path):
  raw=_json(dict(version=1,role='A',node='a',issuer_node='issuer',issuer_certificate='a'*64,coordinator_node='coord',descriptor_sha256='b'*64,root=str(path),charge=1,control_budget=2));return raw,hashlib.sha256(raw).hexdigest()
 async def test_complete_host_config_still_refuses_real_execution(self):
  with tempfile.TemporaryDirectory() as d:
   raw,pin=self.config(Path(d));r=host.preflight(raw,pin)
   self.assertTrue(r['host_directory_valid']);self.assertFalse(r['execution_allowed']);self.assertEqual(r['status'],'REFUSED')
   with self.assertRaises(host.Refused):await host.run_once(raw,pin)
   self.assertEqual(list(Path(d).iterdir()),[])
 async def test_world_readable_directory_refuses(self):
  with tempfile.TemporaryDirectory() as d:
   os.chmod(d,0o755);r=host.preflight(*self.config(Path(d)));self.assertFalse(r['host_directory_valid'])
 async def test_provider_field_and_wrong_pin_refuse(self):
  with tempfile.TemporaryDirectory() as d:
   raw,pin=self.config(Path(d));obj=json.loads(raw);obj['provider']='validation.Holds';raw=_json(obj)
   self.assertTrue(host.preflight(raw,hashlib.sha256(raw).hexdigest())['invalid'])
   self.assertTrue(host.preflight(raw,'0'*64)['invalid'])
 async def test_cli_run_once_no_claim_no_test_option_no_echo(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'config';raw,pin=self.config(Path(d));p.write_bytes(raw);out=io.StringIO()
   with contextlib.redirect_stdout(out):code=host.main(['run-once','--config',str(p),'--sha256',pin])
   self.assertEqual(code,2);self.assertFalse(json.loads(out.getvalue())['execution_allowed']);self.assertEqual(list(Path(d).iterdir()),[p])
 async def test_success_cleanup_order_and_no_quota_release(self):
  h=host._OwnedRun(work_seconds=1,close_seconds=1);events=[]
  async def close():events.append('close')
  async def body(owner):owner.borrow(close);events.append('body')
  r=await h._synthetic_run(body,asyncio.Event());self.assertEqual(r['status'],'COMPLETED');self.assertEqual(events,['body','close']);self.assertFalse(r['quota_release_allowed'])
  with self.assertRaises(host.Refused):await h._synthetic_run(body,asyncio.Event())
 async def test_preexisting_stop_no_body(self):
  stop=asyncio.Event();stop.set();calls=[]
  async def body(h):calls.append(1)
  self.assertEqual((await host._OwnedRun(work_seconds=1,close_seconds=1)._synthetic_run(body,stop))['status'],'STOPPED');self.assertEqual(calls,[])
 async def test_stop_waits_body_exit_before_borrowed_close(self):
  h=host._OwnedRun(work_seconds=1,close_seconds=1);started=asyncio.Event();stop=asyncio.Event();events=[]
  async def close():events.append('close')
  async def body(owner):
   owner.borrow(close);started.set()
   try:await asyncio.Event().wait()
   finally:events.append('body-exit')
  task=asyncio.create_task(h._synthetic_run(body,stop));await started.wait();stop.set();r=await task
  self.assertEqual(r['status'],'STOPPED');self.assertEqual(events,['body-exit','close'])
 async def test_work_timeout_unknown_not_retried(self):
  h=host._OwnedRun(work_seconds=.01,close_seconds=1);calls=[]
  async def body(owner):calls.append(1);await asyncio.Event().wait()
  r=await h._synthetic_run(body,asyncio.Event());self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(calls,[1])
 async def test_cancellation_resistant_body_retained_no_close(self):
  h=host._OwnedRun(work_seconds=1,close_seconds=.01);entered=asyncio.Event();release=asyncio.Event();stop=asyncio.Event();closed=[]
  async def close():closed.append(True)
  async def body(owner):
   owner.borrow(close);entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:await release.wait()
  task=asyncio.create_task(h._synthetic_run(body,stop));await entered.wait();stop.set();r=await task
  self.assertEqual(r['status'],'UNKNOWN');self.assertTrue(r['retained_work']);self.assertFalse(closed);self.assertFalse(r['physical_stop_verified'])
  release.set();await h.cleanup;self.assertEqual(closed,[True]);self.assertFalse(h.quota_release_allowed)
 async def test_body_error_keeps_borrowed_services_owned(self):
  h=host._OwnedRun(work_seconds=1,close_seconds=1);closed=[]
  async def close():closed.append(1)
  async def body(owner):owner.borrow(close);raise ValueError('secret not echoed')
  r=await h._synthetic_run(body,asyncio.Event());self.assertEqual(r['status'],'UNKNOWN');self.assertFalse(closed);self.assertNotIn('secret',str(r))
 async def test_close_failure_unknown(self):
  h=host._OwnedRun(work_seconds=1,close_seconds=1)
  async def close():raise OSError('fsync')
  async def body(owner):owner.borrow(close)
  self.assertEqual((await h._synthetic_run(body,asyncio.Event()))['status'],'UNKNOWN')
 async def test_wrong_launcher_and_deadline_refused(self):
  with self.assertRaises(host.Refused):host._OwnedRun(work_seconds=float('nan'),close_seconds=1)
  with self.assertRaises(host.Refused):host._OwnedRun(work_seconds=1,close_seconds=1).own_launcher(object())
 async def test_parent_cancel_retains_cleanup(self):
  h=host._OwnedRun(work_seconds=1,close_seconds=.01);entered=asyncio.Event();release=asyncio.Event()
  async def body(owner):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:await release.wait()
  task=asyncio.create_task(h._synthetic_run(body,asyncio.Event()));await entered.wait();task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task
  self.assertTrue(h.retained);release.set();await h.cleanup
 async def test_cli_rejects_symlink_and_oversize(self):
  with tempfile.TemporaryDirectory() as d:
   target=Path(d)/'public';target.write_bytes(b'x'*32769);link=Path(d)/'link';link.symlink_to(target)
   for p in (target,link):
    out=io.StringIO()
    with contextlib.redirect_stdout(out):code=host.main(['preflight','--config',str(p),'--sha256','a'*64])
    self.assertEqual(code,2);self.assertEqual(json.loads(out.getvalue())['invalid'],['public_config_unavailable'])
 async def test_sync_callback_rejected_without_call(self):
  calls=[];h=host._OwnedRun(work_seconds=1,close_seconds=1)
  def body(owner):calls.append(1);raise ValueError('sync')
  with self.assertRaises(host.Refused):await h._synthetic_run(body,asyncio.Event())
  self.assertEqual(calls,[]);self.assertFalse(h.used)
if __name__=='__main__':unittest.main()
