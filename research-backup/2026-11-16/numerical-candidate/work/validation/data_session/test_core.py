import asyncio,hashlib,unittest
from contextlib import contextmanager
from validation.role_bridge.test_transport import streams,config
from cluster_p1.channel import Channel
try:
 from cluster_p1.data_session import DataSession,DataSessionPolicy
except ImportError:DataSession=None
class Provider:
 def __init__(self):self.live=True;self.count=0;self.pulses=asyncio.Event()
 @contextmanager
 def hold(self,*args):
  if not self.live:raise PermissionError('revoked')
  self.count+=1
  if self.count>=20:self.pulses.set()
  yield args[0].node_id if len(args)==1 else None
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(DataSession,'missing bounded data session')
  ar,aw,br,bw=streams('a','b');self.a=Channel(ar,aw,config('b'),{},timeout=.1);self.b=Channel(br,bw,config('a'),{},timeout=.1,_gate_receipt=True)
  self.sessions=[];self.p=Provider()
  self.binding={'transport_scope':{'owner':'o','request':'r','session':'s','epoch':1,'model':'m','layout':2,'plan_digest':'p','input_snapshot':'i','token_digest':'t','preprocessing':'x','hidden':1,'vocab':1},'a':{'node':'a','certificate':hashlib.sha256(b'a').hexdigest()},'b':{'node':'b','certificate':hashlib.sha256(b'b').hexdigest()},'installation':{'registration_id':'r','issuer_incarnation':'i','source_namespace':'n','source_id':'s','declaration_digest':'d','source_binding':{'x':'y'},'issuer':{'node':'issuer','certificate':'a'*64}}}
 async def asyncTearDown(self):
  await asyncio.gather(*(s.close() for s in self.sessions),return_exceptions=True)
  await asyncio.gather(self.a.close(),self.b.close())
 def attach(self,c,binding=None):
  s=DataSession.attach(c,binding or self.binding,DataSessionPolicy(.02,.15,2),identity=self.p,authorize=self.p,check=lambda:None,enabled=True);self.sessions.append(s);return s
 async def both(self):
  a=self.attach(self.a);b=self.attach(self.b);await asyncio.gather(a.ready(),b.ready());return a,b
 async def test_bilateral_no_credit(self):
  a,b=await self.both();await asyncio.wait_for(self.p.pulses.wait(),1)
  self.assertTrue(a.active and b.active);self.assertEqual(self.b._receipt_state,'unprepared');self.assertEqual(self.a._credits_bytes,0);self.assertTrue(all(f[:1]==b'P' for f in self.a.w.frames))
 async def test_disabled(self):
  with self.assertRaises(Exception):DataSession.attach(self.a,self.binding,DataSessionPolicy(),identity=self.p,authorize=self.p,check=lambda:None)
  self.assertEqual(self.a.w.frames,[])
 async def test_revoke(self):
  a,b=await self.both();self.p.live=False;await asyncio.wait_for(self.a._task,1);self.assertFalse(a.active)
 async def test_wrong_scope(self):
  import copy
  other=copy.deepcopy(self.binding);other['installation']['source_id']='other';a=self.attach(self.a);b=self.attach(self.b,other)
  result=await asyncio.gather(a.ready(),b.ready(),return_exceptions=True);self.assertTrue(all(isinstance(x,BaseException) for x in result))
 async def test_replay(self):
  a,b=await self.both();frame=next(f for f in self.b.w.frames if f[6]==2);self.a.r.feed_data(frame);await asyncio.wait_for(self.a._task,1);self.assertFalse(a.active)
 async def test_partial_header(self):
  ar,aw,_,_=streams('a','b');c=Channel(ar,aw,config('b'),{},timeout=.05)
  a=self.attach(c);c.r.feed_data(b'P');await asyncio.wait_for(c._task,1);self.assertIsInstance(c._error,TimeoutError)
 async def test_close_joins(self):
  a,b=await self.both();await self.a.close();self.assertTrue(all(t.done() for t in a.tasks))
class Boundaries(Tests):
 async def test_unilateral_opening_cannot_extend(self):
  import struct
  ar,aw,_,_=streams('a','b');c=Channel(ar,aw,config('b'),{},timeout=.3)
  original=aw.write
  def write(frame):
   original(frame)
   if frame[:1]==b'P' and frame[6]==1:ar.feed_data(frame[:6]+b'\x02'+frame[7:])
  aw.write=write;s=self.attach(c)
  with self.assertRaises(Exception):await s.ready()
  self.assertFalse(s.active);self.assertGreater(len(aw.frames),2)
 async def test_absolute_lifetime(self):
  policy=DataSessionPolicy(.02,.15,.3)
  a=DataSession.attach(self.a,self.binding,policy,identity=self.p,authorize=self.p,check=lambda:None,enabled=True)
  b=DataSession.attach(self.b,self.binding,policy,identity=self.p,authorize=self.p,check=lambda:None,enabled=True);self.sessions.extend((a,b))
  await asyncio.gather(a.ready(),b.ready());await asyncio.wait_for(self.a._task,1);self.assertFalse(a.active)
 async def test_writer_cleanup_error_preserved(self):
  a,b=await self.both();error=RuntimeError('synthetic-close')
  async def fail():raise error
  self.a.w.wait_closed=fail
  with self.assertRaises(RuntimeError) as seen:await a.close()
  self.assertIs(seen.exception,error)
 async def test_close_throw_joins(self):
  a,b=await self.both();error=RuntimeError('close-throw');original=self.a.w.close
  def fail():original();raise error
  self.a.w.close=fail
  with self.assertRaises(RuntimeError) as caught:await a.close()
  self.assertIs(caught.exception,error);self.assertTrue(all(t.done() for t in a.tasks));self.assertTrue(self.a._task.done())
  self.a._close_error=None
 async def test_wrong_principal(self):
  @contextmanager
  def wrong(*args):yield 'foreign' if len(args)==1 else None
  self.p.hold=wrong
  a=self.attach(self.a);b=self.attach(self.b)
  out=await asyncio.gather(a.ready(),b.ready(),return_exceptions=True)
  self.assertTrue(all(isinstance(e,BaseException) for e in out));self.assertEqual(self.a.w.frames,[])
 async def test_generation_mismatch(self):
  self.b.w.unique=b'other-generation';a=self.attach(self.a);b=self.attach(self.b)
  out=await asyncio.gather(a.ready(),b.ready(),return_exceptions=True)
  self.assertTrue(all(isinstance(e,BaseException) for e in out))
 async def test_provider_exit_mutation(self):
  @contextmanager
  def mutation(*args):
   yield args[0].node_id if len(args)==1 else None
   self.binding['installation']['source_id']='changed'
   if self.sessions:self.sessions[0].binding['scope']['installation']['source_id']='changed'
  self.p.hold=mutation
  a=self.attach(self.a);b=self.attach(self.b)
  out=await asyncio.gather(a.ready(),b.ready(),return_exceptions=True)
  self.assertTrue(any(isinstance(e,BaseException) for e in out));self.assertFalse(a.active)
 async def test_extended_deadline_responder_wait(self):
  import struct
  ar,aw,_,_=streams('a','b');c=Channel(ar,aw,config('b'),{},timeout=.1);enough=asyncio.Event();original=aw.write
  def peer(frame):
   original(frame)
   if frame[:1]==b'P' and frame[6]==1:
    seq=struct.unpack('!Q',frame[7:15])[0]
    ar.feed_data(frame[:6]+b'\x02'+frame[7:])
    if seq==1:ar.feed_data(frame[:47]+b'z'*32)
    if seq>=12:enough.set()
  aw.write=peer;a=self.attach(c);await a.ready()
  failed=asyncio.Event();original_fail=a._fail;observed=[]
  def capture(error=None,**kw):
   if a.phase!='CLOSED':observed.append((type(error).__name__,a.deadline-a.loop.time()));failed.set()
   return original_fail(error,**kw)
  a._fail=capture
  yes=asyncio.create_task(enough.wait());no=asyncio.create_task(failed.wait())
  try:
   await asyncio.wait((yes,no),timeout=.6,return_when=asyncio.FIRST_COMPLETED)
   self.assertTrue(enough.is_set(),repr(observed));self.assertTrue(a.active)
  finally:
   yes.cancel();no.cancel();await asyncio.gather(yes,no,return_exceptions=True)
 async def test_partial_body(self):
  import struct
  ar,aw,_,_=streams('a','b');c=Channel(ar,aw,config('b'),{},timeout=.05)
  a=self.attach(c);c.r.feed_data(b'P'+struct.pack('!I',74)+b'x');await asyncio.wait_for(c._task,1);self.assertIsInstance(c._error,TimeoutError)

def load_tests(loader,tests,pattern):
 return unittest.TestSuite([Tests(n) for n in Tests.__dict__ if n.startswith('test_')]+[Boundaries(n) for n in Boundaries.__dict__ if n.startswith('test_')])
if __name__=='__main__':unittest.main()
