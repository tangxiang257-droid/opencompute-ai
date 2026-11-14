"""Raw-dial LAB boundary; no socket, TLS handshake, model or actual device."""
import asyncio,ssl,unittest,importlib.util
from unittest.mock import patch
from cluster_p1.channel import TLSConfig
from cluster_p1.role_launcher import StageLauncher
from validation.role_launcher.test_pipeline import Tests as Base,configuration
from validation.role_entry.test_entry import Flow as Chain

async def lab_dial(*args,**kwargs):
 from cluster_p1.role_dial import dial_stage
 kwargs.setdefault('material',b'LAB loader boundary');kwargs.setdefault('material_pin','LAB')
 return await dial_stage(*args,**kwargs)

class Entry(unittest.TestCase):
 def test_actual_dial_entry_exists(self):self.assertIsNotNone(importlib.util.find_spec('cluster_p1.role_dial'),'raw role stream dial missing')
def context():
 c=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);c.minimum_version=ssl.TLSVersion.TLSv1_2;return c
class Stop(Exception):pass
class Cases(Base):
 async def create_startup(self,r,w,tls,grants,store,d,**opts):
  opts.pop('issuer_node');opts.pop('issuer_certificate');c=configuration(self.root,('A','B')[d['stage']],d)
  await self.probe(c,r,w,tls,grants,store,d,opts);raise Stop()
 async def case(self,probe):
  self.probe=probe
  with self.assertRaises(Stop):await self.online_bootstrap()
 async def test_default_missing_providers_and_hostname_no_dial(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   calls=[]
   async def opened(*a,**kw):calls.append(1);return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    for change in ({'enabled':False},{'identity':None},{'authorize':None},{'resources':None},{'completion':None},{'runtime':None},{'runtime':dict(opts['runtime'],layer_range=(-1,-1))},{'runtime':dict(opts['runtime'],factories='invalid')},{'server_hostname':''}):
     kw=dict(opts,enabled=True,server_hostname='issuer.example');kw.update(change)
     with self.assertRaises(ValueError):await dial_stage(c,'issuer.example',443,tls,grants,store,d,**kw)
   self.assertEqual(calls,[]);self.assertFalse(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_raw_dial_one_reader_no_H_then_owner_close(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   calls=[]
   async def opened(host,port,**kw):calls.append((host,port,kw));return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    launch=await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.addAsyncCleanup(launch.close)
   self.assertEqual(len(calls),1);self.assertEqual(calls[0][2]['server_hostname'],'issuer.example')
   self.assertTrue(calls[0][2]['ssl'].check_hostname);self.assertEqual(w.frames,[])
   self.assertIs(launch.startup.channel.r,r);self.assertIs(launch.startup.channel.w,w)
   self.assertFalse(launch.startup.channel._start_attempted);await launch.close();self.assertTrue(launch.startup.channel._task.done())
  await self.case(probe)
 async def test_cancelled_dial_late_stream_is_closed_without_claim(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event()
   async def opened(*a,**kw):
    entered.set()
    try:await asyncio.Event().wait()
    except asyncio.CancelledError:cancelled.set();await release.wait();return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    task=asyncio.create_task(dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts))
    await entered.wait();task.cancel();await cancelled.wait();task.cancel();release.set()
    with self.assertRaises(BaseException):await task
   self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists());self.assertEqual(w.frames,[])
  await self.case(probe)
 async def test_claim_conflict_after_connect_closes_writer(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):c.claim();return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertTrue(c.path('claim').exists());self.assertEqual(w.frames,[])
  await self.case(probe)
 async def test_timeout_late_stream_closes_without_claim(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   entered=asyncio.Event();cancelled=asyncio.Event()
   async def opened(*a,**kw):
    entered.set()
    try:await asyncio.Event().wait()
    except asyncio.CancelledError:cancelled.set();return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(TimeoutError):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,timeout=.01,**opts)
   self.assertTrue(entered.is_set());self.assertTrue(cancelled.is_set());self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_wrong_peer_certificate_closes_after_claim(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):w.cert=b'wrong';return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertTrue(c.path('claim').exists());self.assertEqual(w.frames,[])
  await self.case(probe)
 async def test_config_drift_closes_before_claim(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):c._sha='0'*64;return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertFalse((c.root/'launch-A-claim.sqlite').exists())
  await self.case(probe)
 async def test_live_capacity_denial_prevents_dial_keeps_charge(self):
  dial_stage=lab_dial
  from contextlib import contextmanager
  class Deny:
   @contextmanager
   def hold(self,*args):raise PermissionError('capacity unavailable');yield
  async def probe(c,r,w,tls,grants,store,d,opts):
   opts['resources'].capacity=Deny();calls=[]
   async def opened(*a,**kw):calls.append(1);return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(PermissionError):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertEqual(calls,[]);row=opts['resources'].query(d['resource_id']);self.assertEqual(row['state'],'UNKNOWN');self.assertGreater(row['charge'],0)
  await self.case(probe)
 async def test_last_resource_callback_poison_closes_before_claim(self):
  dial_stage=lab_dial
  from contextlib import contextmanager
  async def probe(c,r,w,tls,grants,store,d,opts):
   resource=opts['resources'];old=resource.hold;holds=[];calls=[]
   @contextmanager
   def hold(*a,**kw):
    with old(*a,**kw):yield
    holds.append(1)
    if len(holds)==3:resource.poisoned.add(d['resource_id'])
   resource.hold=hold
   async def opened(*a,**kw):calls.append(1);return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertEqual(len(holds),3);self.assertEqual(calls,[1]);self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_TLS_context_downgrade_during_dial_closes(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   ctx=context()
   async def opened(*a,**kw):ctx.check_hostname=False;ctx.verify_mode=ssl.CERT_NONE;return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:ctx):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_existing_claim_refuses_before_dial(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   c.claim();calls=[]
   async def opened(*a,**kw):calls.append(1);return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertEqual(calls,[]);self.assertFalse(w.is_closing())
  await self.case(probe)
 async def test_resource_method_swap_during_dial_refuses(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):opts['resources'].hold_control=lambda *a:None;return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_store_authority_swap_during_dial_refuses(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):store.authorize=type(store.authorize)();return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_config_claim_method_swap_during_dial_refuses(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):c.claim=lambda:None;return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    with self.assertRaises(Exception):await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
   self.assertTrue(w.is_closing());self.assertFalse(c.path('claim').exists())
  await self.case(probe)
 async def test_late_dial_error_preserved_with_cancellation(self):
  dial_stage=lab_dial
  async def probe(c,r,w,tls,grants,store,d,opts):
   entered=asyncio.Event()
   async def opened(*a,**kw):
    entered.set()
    try:await asyncio.Event().wait()
    except asyncio.CancelledError:raise RuntimeError('late dial failure')
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    task=asyncio.create_task(dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts))
    await entered.wait();task.cancel()
    with self.assertRaises(BaseException) as caught:await task
   def contains(e):
    pending=[e];seen=set()
    while pending:
     value=pending.pop()
     if id(value) in seen:continue
     seen.add(id(value))
     if isinstance(value,RuntimeError):return True
     pending.extend(getattr(value,'exceptions',()))
     pending.extend(x for x in (value.__cause__,value.__context__) if x is not None)
    return False
   self.assertTrue(contains(caught.exception),'late cleanup error was discarded');self.assertFalse(c.path('claim').exists())
  await self.case(probe)
class Pipeline(Chain):
 async def create_stage(self,c,r,w,tls,grants,store,d,**opts):
  dial_stage=lab_dial
  async def opened(*args,**kw):return r,w
  with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
   launch=await dial_stage(c,'issuer.example',443,tls,grants,store,d,server_hostname='issuer.example',enabled=True,**opts)
  self.dial_stages=getattr(self,'dial_stages',[])+[d['stage']]
  self.assertEqual(w.frames,[]);return launch
 async def test_A_B_C_via_raw_dial_and_control_entry(self):
  await self.run_chain();self.assertEqual(self.dial_stages,[0,1])
def load_tests(loader,tests,pattern):return unittest.TestSuite([Entry('test_actual_dial_entry_exists'),*(Cases(n) for n in Cases.__dict__ if n.startswith('test_')),Pipeline('test_A_B_C_via_raw_dial_and_control_entry')])
if __name__=='__main__':unittest.main()
