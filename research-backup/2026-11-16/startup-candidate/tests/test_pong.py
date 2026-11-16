import sys,importlib.abc,resource
class Guard(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if any(x in fullname.lower().split('.') for x in ('torch','transformers','safetensors','tensorflow','worker')):raise RuntimeError('blocked')
def audit(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system','os.fork','os.posix_spawn'):raise RuntimeError('blocked')
sys.meta_path.insert(0,Guard());sys.addaudithook(audit)
resource.setrlimit(resource.RLIMIT_CPU,(90,95));resource.setrlimit(resource.RLIMIT_AS,(1536*1024*1024,1536*1024*1024))
import asyncio,unittest,json,logging,struct
from pathlib import Path
from validation.control_idle.test_control_core import Control
logging.disable(logging.CRITICAL)
OUT=Path(sys.argv[1]);OUT.mkdir(exist_ok=False)
def category(e):
 if e is None:return 'none'
 if type(e)is asyncio.CancelledError:return 'cancelled'
 if type(e)is TimeoutError:return 'deadline'
 if type(e)is AssertionError:return 'assertion'
 if isinstance(e,BaseExceptionGroup):return 'group'
 if isinstance(e,(ValueError,PermissionError)):return 'rejected'
 return 'other'
class Probe(Control):
 async def host_inputs(self,stage,spec):
  result=await super().host_inputs(stage,spec)
  if self.mode=='normal':return result
  writer=self.central_streams['control'][1];original=writer.write
  self.held=[];self.held_size=0;self.gate=asyncio.Event();self.gate_writer=writer
  def gated(data):
   if writer.is_closing():return original(data)
   pong=type(data)is bytes and len(data)==79 and data[:1]==b'P' and data[1:5]==struct.pack('!I',74) and data[5:7]==b'\x01\x02'
   if self.gate.is_set() or pong:
    if self.held_size+len(data)>65536:
     self.record['fixture_buffer_exceeded']=True;raise AssertionError('fixture buffer limit')
    self.held.append(data);self.held_size+=len(data);self.gate.set();return None
   return original(data)
  self.restore_writer=(writer,original)
  writer.write=gated
  return result
 def snapshot(self):
  hosts=getattr(self,'control_hosts',[]);host=hosts[-1] if hosts else None;l=self.launchers.get(0)
  channels={}
  if host:
   for key,slot in host.endpoints.items():
    c=slot.channel
    if c:
     owner=getattr(c,'_data_session_owner',None)
     channels['host_'+key]=dict(reader_done=c._task.done(),closed=c._closed,first=getattr(c,'_first_failure',None),owner_present=owner is not None,session_state=getattr(owner,'phase',None),reader_deadline_present=getattr(c,'_reader_idle_deadline',None) is not None,reader_deadline_reached=(asyncio.get_running_loop().time()>=c._reader_idle_deadline) if getattr(c,'_reader_idle_deadline',None) is not None else None)
  if l:
   for key,c in [('control',getattr(l,'control',None)),('actions',getattr(getattr(l,'actions',None),'channel',None))]:
    if c:
     owner=getattr(c,'_data_session_owner',None)
     channels['role_'+key]=dict(reader_done=c._task.done(),closed=c._closed,first=getattr(c,'_first_failure',None),owner_present=owner is not None,session_state=getattr(owner,'phase',None),reader_deadline_present=getattr(c,'_reader_idle_deadline',None) is not None,reader_deadline_reached=(asyncio.get_running_loop().time()>=c._reader_idle_deadline) if getattr(c,'_reader_idle_deadline',None) is not None else None)
  return dict(host_state=getattr(host,'state',None),host_work_done=host.work.done() if host and host.work else None,host_cleanup_done=host.cleanup.done() if host and host.cleanup else None,ledger_state=self.adm._row(self.owner,self.receipt.proposal_id)[3],gate_reached=hasattr(self,'gate') and self.gate.is_set(),channels=channels,compute_called=any(bool(x) for x in self.calls),action_count=getattr(host,'action_count',0))
 async def scenario(self,mode):
  self.mode=mode;record={'mode':mode};self.record=record;work=None;waiter=None
  try:
   work=asyncio.create_task(self.prepare_a())
   if mode=='normal':
    await asyncio.wait_for(asyncio.shield(work),30)
    record['outcome']='returned';record['observed']=self.snapshot()
    self.assertEqual(record['observed']['host_state'],'READY')
   else:
    # host_inputs is reached within prepare_a; don't poll or add sleeps.
    # Signal below is set by the subclass once the transport gate is installed.
    await asyncio.wait_for(self.inputs_ready.wait(),30)
    waiter=asyncio.create_task(self.gate.wait())
    done,_=await asyncio.wait((work,waiter),timeout=30,return_when=asyncio.FIRST_COMPLETED)
    if not self.gate.is_set():
     record['gate_outcome']='NOT_REACHED'
     if work in done:await work
     raise AssertionError('gate not reached')
    record['at_gate']=self.snapshot()
    self.assertFalse(record['at_gate']['compute_called']);self.assertEqual(record['at_gate']['action_count'],0)
    if mode=='cancel':
     work.cancel()
     with self.assertRaises(asyncio.CancelledError):await work
     record['outcome']='cancelled'
    else:
     host=self.control_hosts[-1];action=host.endpoints['actions'].channel
     self.assertIsNotNone(action)
     try:await asyncio.wait_for(asyncio.shield(action._task),30)
     except BaseException as e:
      record['reader_outcome']=category(e) if action._task.done() else 'harness_cap'
     else:record['reader_outcome']='returned'
     record['at_action_end']=self.snapshot()
     try:await asyncio.wait_for(asyncio.shield(work),30)
     except BaseException as e:record['work_outcome']='harness_cap' if type(e)is TimeoutError and not work.done() else category(e)
     else:record['work_outcome']='returned'
     record['outcome']='action_reader_ended'
     self.assertTrue(action._task.done())
   record['after']=self.snapshot();self.assertFalse(record['after']['compute_called']);self.assertEqual(record['after']['action_count'],0)
  except BaseException as e:
   record['scenario_error']='harness_cap' if type(e)is TimeoutError and work is not None and not work.done() else category(e);raise
  finally:
   for task in (waiter,work):
    if task is not None and not task.done():task.cancel()
   outcomes=await asyncio.gather(*(t for t in (waiter,work) if t is not None),return_exceptions=True)
   record['settled_categories']=[category(x) if isinstance(x,BaseException) else 'returned' for x in outcomes]
   if hasattr(self,'held'):self.held.clear()
   record['before_cleanup']=self.snapshot()
   try:await self._cleanup_runtime()
   except BaseException as e:record['cleanup_error']=category(e);raise
   finally:
    record['after_cleanup']=self.snapshot()
    if hasattr(self,'restore_writer'):
     writer,original=self.restore_writer;writer.write=original
    if hasattr(self,'held'):self.held.clear()
    (OUT/(mode+'.json')).write_text(json.dumps(record,sort_keys=True))
 async def asyncSetUp(self):
  self.inputs_ready=asyncio.Event();await super().asyncSetUp()
 async def test_normal(self):await self.scenario('normal')
 async def test_hold(self):await self.scenario('hold')
 async def test_cancel(self):await self.scenario('cancel')
# Notify gate installation without changing the product transport or providers.
_original_inputs=Probe.host_inputs
async def inputs(self,*a,**kw):
 result=await _original_inputs(self,*a,**kw);self.inputs_ready.set();return result
Probe.host_inputs=inputs
class Finite(unittest.TestResult):
 def addFailure(self,test,err):self.failures.append((test._testMethodName,category(err[1])))
 def addError(self,test,err):self.errors.append((test._testMethodName,category(err[1])))
r=Finite();unittest.TestSuite(Probe(n) for n in ('test_normal','test_hold','test_cancel')).run(r)
result=dict(tests=r.testsRun,failures=r.failures,errors=r.errors,successful=r.wasSuccessful());(OUT/'result.json').write_text(json.dumps(result));print(json.dumps(result));sys.exit(not r.wasSuccessful())
