import sys,importlib.abc,resource
class Guard(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if any(x in fullname.lower().split('.') for x in ('torch','transformers','safetensors','tensorflow','worker')):raise RuntimeError('blocked')
def audit(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system','os.fork','os.posix_spawn'):raise RuntimeError('blocked')
sys.meta_path.insert(0,Guard());sys.addaudithook(audit)
resource.setrlimit(resource.RLIMIT_CPU,(90,95));resource.setrlimit(resource.RLIMIT_AS,(1536*1024*1024,1536*1024*1024))
import asyncio,unittest,json,logging,struct,ssl
from pathlib import Path
from unittest.mock import patch
from validation.control_idle.test_control_core import Control
from validation.install_host.test_host import Socket,context
import private_control_action_host as hm
import cluster_p1.role_dial as dial
import cluster_p1.control_session as sessions
logging.disable(logging.CRITICAL)
OUT=Path(sys.argv[1]);OUT.mkdir(exist_ok=False)
def category(e):
 if e is None:return 'none'
 if type(e)is asyncio.CancelledError:return 'cancelled'
 if type(e)is TimeoutError:return 'test-cap-or-deadline'
 if type(e)is AssertionError:return 'assertion'
 if isinstance(e,BaseExceptionGroup):return 'group'
 if type(e).__name__=='DependencyEnded':return 'dependency-ended'
 return 'other'
class Probe(Control):
 async def test_required_reader_cancels_assigned_bootstrap(self):
  self.launchers[0].build_operation(self.spec,self.pipe_policy,tokens=struct.pack('<q',1))
  launch=await self.host_inputs(0,self.spec)
  host=hm.ControlActionHost(*self.host_args,**self.host_kw);self.owned.append(host)
  sockets=[Socket(),Socket()];accepted=[Socket(),Socket()];pairs=dict(self.central_streams)
  entered=asyncio.Event();release=asyncio.Event();settled=asyncio.Event();record={};tasks=[]
  async def accept(sock):
   self.assertTrue(all(s.bound is not None and s.backlog==1 for s in sockets))
   i=sockets.index(sock);return accepted[i],('127.0.0.1',30000+i)
  async def handshake(slot,sock,ctx,*args):return pairs[('control','actions')[accepted.index(sock)]]
  async def opened(hostname,port,**opts):return self.raw_streams[:2] if hostname=='control.example' else self.raw_streams[2:]
  def client_context(*args):
   c=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);c.minimum_version=ssl.TLSVersion.TLSv1_2;return c
  original=sessions.attach_control_session
  async def stalled(endpoint,*args,**kw):
   if endpoint is host:
    self.assertIsNotNone(host.control)
    self.assertFalse(self.consent.authority._lock.locked())
    entered.set()
    try:await release.wait()
    finally:settled.set()
   return await original(endpoint,*args,**kw)
  try:
   with patch.object(hm.socket,'socket',side_effect=sockets),patch.object(asyncio.get_running_loop(),'sock_accept',accept),patch.object(hm,'load_context',lambda *a,**k:context()),patch.object(hm,'_accepted_tls_streams',handshake),patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',client_context),patch.object(sessions,'attach_control_session',stalled):
    start=asyncio.create_task(host.start(enabled=True));role=asyncio.create_task(dial.dial_control_ready(*self.dial_args,**self.dial_kw));tasks=[start,role]
    await asyncio.wait_for(entered.wait(),30);record['gate_entered']=True
    action=host.endpoints['actions'].channel;self.assertIsNotNone(action)
    self.assertFalse(action._task.done());self.assertFalse(action._closed)
    self.raw_streams[3].close() # Actual memory peer close, never task/state assignment.
    await asyncio.wait_for(asyncio.shield(action._task),5)
    record['action_reader_done']=action._task.done();record['action_first']=action._first_failure
    try:await asyncio.wait_for(asyncio.shield(start),3)
    except TimeoutError:
     if not start.done():record['harness_cap']=True
     raise
    except BaseException as error:
     record['startup_outcome']=category(error)
     self.assertEqual(type(error).__name__,'DependencyEnded')
    else:self.fail('startup succeeded after required reader ended')
    self.assertTrue(settled.is_set());self.assertFalse(release.is_set())
    self.assertEqual(host.state,'UNKNOWN');self.assertEqual(self.adm._row(self.owner,self.receipt.proposal_id)[3],'UNKNOWN')
    self.assertEqual(host.action_count,0);self.assertEqual(self.calls,[[],[]]);self.assertTrue(host.work.done());self.assertTrue(host.cleanup.done())
    record['dependency_failure_settled']=True
  except BaseException as error:record['test_error']=category(error);raise
  finally:
   # Cancellation, never gate release, must terminate unfinished product work.
   for task in tasks:
    if not task.done():task.cancel()
   outcomes=await asyncio.gather(*tasks,return_exceptions=True)
   record['outcomes']=[category(e) if isinstance(e,BaseException) else 'returned' for e in outcomes]
   record['bootstrap_settled']=settled.is_set();record['gate_released']=release.is_set()
   try:await self._cleanup_runtime()
   except BaseException as error:record['cleanup_error']=category(error);raise
   finally:
    record['host_state']=host.state;record['ledger_state']=self.adm._row(self.owner,self.receipt.proposal_id)[3];record['compute_called']=any(bool(x) for x in self.calls)
    record['host_work_done']=host.work.done() if host.work else None;record['host_cleanup_done']=host.cleanup.done() if host.cleanup else None
    (OUT/'case.json').write_text(json.dumps(record,sort_keys=True))
class Finite(unittest.TestResult):
 def addFailure(self,test,err):self.failures.append((test._testMethodName,category(err[1])))
 def addError(self,test,err):self.errors.append((test._testMethodName,category(err[1])))
r=Finite();unittest.TestSuite([Probe('test_required_reader_cancels_assigned_bootstrap')]).run(r)
result=dict(tests=r.testsRun,failures=r.failures,errors=r.errors,successful=r.wasSuccessful());(OUT/'result.json').write_text(json.dumps(result));print(json.dumps(result));sys.exit(not r.wasSuccessful())
