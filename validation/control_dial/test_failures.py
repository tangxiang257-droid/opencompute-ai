import asyncio,hashlib,ssl,struct,unittest
from unittest.mock import patch
from validation.control_dial.test_dial import Flow
from cluster_p1.control_exchange import _json,_parse
import cluster_p1.role_dial as dial
import private_experiment_topology as topology

def context(*args):
 c=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);c.minimum_version=ssl.TLSVersion.TLSv1_2;return c
class Cases(Flow):
 async def prepare_dial(self):
  l=self.launchers[0];l.build_operation(self.spec,self.pipe_policy,tokens=struct.pack('<q',1))
  await self.endpoints(0,self.spec);return l
 def streams(self,index):
  sr,sw,ar,aw=self.raw_streams;return (sr,sw) if index==0 else (ar,aw)
 async def test_missing_or_wrong_config_does_not_dial(self):
  l=await self.prepare_dial();calls=[]
  async def opened(*a,**k):calls.append(1);return self.streams(0)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   for change in ({'enabled':False},{'identity':None},{'authorize':None},{'action_identity':None},{'clock':None}):
    with self.assertRaises(Exception):await dial.dial_control_ready(*self.dial_args,**dict(self.dial_kw,**change))
   for kind,key,value in [('control','peer','issuer'),('actions','certificate','0'*64),('control','port',True)]:
    d=_parse(self.dial_args[1]);d[kind][key]=value;raw=_json(d);args=(l,raw,hashlib.sha256(raw).hexdigest(),*self.dial_args[3:])
    with self.assertRaises(Exception):await dial.dial_control_ready(*args,**self.dial_kw)
  self.assertFalse(calls);self.assertFalse(getattr(l,'_control_dial_attempted',False));self.assertFalse(l._closing)
 async def test_invalid_control_certificate_refuses_before_material_load(self):
  l=await self.prepare_dial();d=_parse(self.dial_args[1]);d['control']['certificate']='not-a-pin'
  self.dial_args[3]['control'].peers['controller']='not-a-pin'
  raw=_json(d);args=(l,raw,hashlib.sha256(raw).hexdigest(),*self.dial_args[3:]);loaded=[]
  def material(*args):loaded.append(1);raise RuntimeError('LAB stop before network')
  with patch.object(dial,'load_context',material):
   with self.assertRaises(Exception):await dial.dial_control_ready(*args,**self.dial_kw)
  self.assertEqual(loaded,[],'malformed remote pin reached material/network preparation')
 async def test_second_dial_failure_keeps_responsibility(self):
  l=await self.prepare_dial();calls=[]
  async def opened(*a,**k):
   calls.append(1)
   if len(calls)==2:raise OSError('LAB second dial failed')
   return self.streams(0)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   with self.assertRaises(OSError):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
   with self.assertRaises(Exception):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
  self.assertEqual(len(calls),2);self.assertTrue(self.streams(0)[1].is_closing());self.assertTrue(l.operation.retired)
  r=l.startup.runtime;self.assertEqual(r['resources'].query(r['ticket'].reservation_id)['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]])
 async def test_cleanup_error_not_suppressed(self):
  l=await self.prepare_dial();marker=OSError('LAB wait_closed unknown');calls=[];w=self.streams(0)[1]
  async def closed():raise marker
  async def opened(*a,**k):
   calls.append(1)
   if len(calls)==2:raise ValueError('LAB second dial')
   return self.streams(0)
  with patch.object(w,'wait_closed',closed),patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   with self.assertRaises(BaseException) as caught:await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
  def graph(e):
   pending=[e];seen=set()
   while pending:
    item=pending.pop()
    if id(item) in seen:continue
    seen.add(id(item));yield item
    if isinstance(item,BaseExceptionGroup):pending.extend(item.exceptions)
    pending.extend(v for v in (item.__cause__,item.__context__) if v is not None)
  self.assertIn(marker,list(graph(caught.exception)));self.assertTrue(w.is_closing());self.assertTrue(l.operation.retired)
 async def external_close(self,index):
  l=await self.prepare_dial();entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event();calls=[]
  async def opened(*a,**k):
   i=len(calls);calls.append(i)
   if i==index:
    entered.set()
    try:await asyncio.Event().wait()
    except asyncio.CancelledError:cancelled.set();await release.wait()
   return self.streams(i)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   task=asyncio.create_task(dial.dial_control_ready(*self.dial_args,**self.dial_kw));await entered.wait()
   close=asyncio.create_task(l.close());await asyncio.wait_for(cancelled.wait(),3);task.cancel();release.set()
   done,pending=await asyncio.wait((task,close),timeout=5)
   self.assertFalse(pending,'external close cycle or lost late stream')
   results=await asyncio.gather(task,close,return_exceptions=True)
   self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[1])
  for i in calls:self.assertTrue(self.streams(i)[1].is_closing())
  self.assertTrue(l._close_task.done());self.assertTrue(l.operation.retired)
 async def test_external_close_first_dial(self):await self.external_close(0)
 async def test_external_close_second_dial(self):await self.external_close(1)
 async def test_competing_entry_cannot_take_or_close_owner(self):
  l=await self.prepare_dial();entered=asyncio.Event();release=asyncio.Event()
  async def opened(*a,**k):entered.set();await release.wait();return self.streams(0)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   task=asyncio.create_task(dial.dial_control_ready(*self.dial_args,**self.dial_kw));await entered.wait()
   with self.assertRaises(Exception):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
   with self.assertRaises(Exception):await l.attach(*self.streams(0),None,None)
   with self.assertRaises(Exception):await l.attach_actions(None)
   with self.assertRaises(Exception):await topology.bind_control_ready(l,*self.streams(0),None,None,None,enabled=True)
   self.assertFalse(l._closing);self.assertFalse(task.done());task.cancel()
   with self.assertRaises(asyncio.CancelledError):await task
  self.assertTrue(l._close_task.done());self.assertEqual(self.calls,[[],[]])
 async def test_operation_provider_drift_stops_before_second_dial(self):
  l=await self.prepare_dial();calls=[];original=l.operation.loader
  class Foreign:
   def load(self,*args):raise AssertionError('no computation')
   def quarantine(self,*args):pass
  async def opened(*a,**k):
   calls.append(1)
   if len(calls)==2:raise ValueError('LAB stop second dial')
   l.operation.loader=Foreign();return self.streams(0)
  try:
   with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
    with self.assertRaises(Exception):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
   self.assertEqual(len(calls),1,'operation provider drift reached second dial')
   self.assertEqual(self.calls,[[],[]]);self.assertTrue(self.streams(0)[1].is_closing())
  finally:l.operation.loader=original # fixture teardown only; no retry
 async def test_material_drift_after_first_dial_refuses_second(self):
  l=await self.prepare_dial();calls=[]
  async def opened(*a,**k):calls.append(1);self.dial_args[4]['actions']=b'changed';return self.streams(0)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   with self.assertRaises(Exception):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
  self.assertEqual(len(calls),1);self.assertTrue(self.streams(0)[1].is_closing());self.assertEqual(self.calls,[[],[]])
def load_tests(*args):return unittest.TestSuite(Cases(n) for n in Cases.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
