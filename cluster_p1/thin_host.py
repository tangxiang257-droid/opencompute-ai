"""Private inert host preflight and bounded lifecycle supervision.

Production run-once is deliberately unavailable until a reviewed concrete trusted
assembly exists. There is NO provider registry, import string or test-mode CLI.
The synthetic driver is private, separate and never reachable from main/run_once.
Timeout means UNKNOWN with ownership retained, never permission to release quota.
"""
import argparse,asyncio,inspect,json,math,os,stat
from pathlib import Path
from .role_launcher import LaunchConfig,StageLauncher,ReceivingLauncher

MISSING=('experiment_execution_admission','remote_current_task_permission',
 'trusted_identity_and_node_configuration','live_platform_capacity_and_exclusivity',
 'device_action_authority_and_completion_assembly','trusted_role_connection_assembly',
 'c_authenticated_result_task_import','monetary_budget_enforcement')
class Refused(ValueError):pass

def preflight(raw,pin):
 """Host-local read only. Does not create a claim, key, provider, socket or GPU."""
 result=dict(status='REFUSED',execution_allowed=False,configuration_valid=False,
             host_directory_valid=False,missing=list(MISSING),invalid=[])
 try:
  config=LaunchConfig(raw,pin);result['configuration_valid']=True
  info=config.root.lstat()
  if info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)&0o077:raise Refused()
  config.check();result['host_directory_valid']=True
 except Exception:result['invalid'].append('configuration_or_private_directory')
 return result

async def run_once(raw,pin):
 # No override parameter and no environment-dependent factory. Method shape or
 # caller-declared verified=True cannot establish trusted execution authority.
 preflight(raw,pin)
 raise Refused('trusted production assembly unavailable')

def _seconds(value):
 if type(value) not in (int,float) or not math.isfinite(value) or not 0<value<=3600:raise Refused()
 return value

class _OwnedRun:
 """One in-process supervision attempt, not a durable execution state machine.

The assembly registers each created launcher immediately, before further awaits.
Borrowed stores/resources are closed only after all owned work/launchers settle.
On timeout/errors they remain owned by this object and require external isolation.
"""
 def __init__(self,*,work_seconds,close_seconds):
  self.work_seconds=_seconds(work_seconds);self.close_seconds=_seconds(close_seconds)
  self.used=False;self.sealed=False;self.launchers=[];self.borrowed=[];self.retained=[]
  self.work=None;self.cleanup=None;self.quota_release_allowed=False
 def own_launcher(self,launcher):
  if self.sealed or type(launcher) not in (StageLauncher,ReceivingLauncher) or any(x is launcher for x in self.launchers):raise Refused()
  self.launchers.append(launcher)
 def borrow(self,closer):
  if self.sealed or not callable(closer):raise Refused()
  self.borrowed.append(closer)
 def _seal(self):
  self.sealed=True
  for launcher in self.launchers:launcher._seal()
 def _retain(self,task):
  self.retained.append(task)
  # Observe eventual error without reporting success or releasing ownership.
  task.add_done_callback(lambda t:t.exception() if not t.cancelled() else None)
 async def _close(self,waiter):
  await asyncio.gather(waiter,return_exceptions=True)
  errors=[]
  if self.work is not None:
   try:await self.work
   except asyncio.CancelledError:pass
   except BaseException as e:errors.append(e)
  for launcher in reversed(self.launchers):
   try:await launcher.close()
   except BaseException as e:errors.append(e)
  if errors:raise BaseExceptionGroup('owned work or cleanup uncertain',errors)
  # Closers must be nonblocking async adapters; no quota release or qualification
  # reset is part of host cleanup. A stuck synchronous closer is not preemptible.
  for closer in reversed(self.borrowed):await closer()
 async def _synthetic_run(self,body,stop):
  """Test harness only; public run_once NEVER calls this method."""
  if self.used or type(stop)is not asyncio.Event or not inspect.iscoroutinefunction(body):raise Refused()
  self.used=True
  if stop.is_set():self.sealed=True;return self._report('STOPPED')
  self.work=asyncio.create_task(body(self));waiter=asyncio.create_task(stop.wait())
  status='UNKNOWN';cancelled=None
  try:
   done,_=await asyncio.wait((self.work,waiter),timeout=self.work_seconds,return_when=asyncio.FIRST_COMPLETED)
   if self.work in done and not self.work.cancelled() and self.work.exception() is None and not stop.is_set():status='COMPLETED'
   elif waiter in done:status='STOPPED'
  except asyncio.CancelledError as e:cancelled=e
  finally:
   self._seal();waiter.cancel()
   if not self.work.done():self.work.cancel()
   # Acquire strong cleanup ownership before any cancellation-sensitive await.
   self.cleanup=asyncio.create_task(self._close(waiter))
   try:
    done,_=await asyncio.wait((self.cleanup,),timeout=self.close_seconds)
    if self.cleanup not in done:self._retain(self.cleanup);status='UNKNOWN'
    elif self.cleanup.cancelled() or self.cleanup.exception() is not None:status='UNKNOWN'
   except asyncio.CancelledError as e:
    cancelled=e;self._retain(self.cleanup);status='UNKNOWN'
  if cancelled is not None:raise cancelled
  return self._report(status)
 def _report(self,status):return dict(status=status,synthetic_only=True,execution_allowed=False,
    physical_stop_verified=False,quota_release_allowed=False,retained_work=bool(self.retained))

def main(argv=None):
 parser=argparse.ArgumentParser(description='Private host preparation; real execution unavailable')
 parser.add_argument('action',choices=('preflight','run-once'));parser.add_argument('--config',required=True)
 parser.add_argument('--sha256',required=True);args=parser.parse_args(argv)
 try:
  # Bounded read, no arbitrary code/provider imports. Only the public config file.
  fd=os.open(args.config,os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW|os.O_NONBLOCK)
  try:
   info=os.fstat(fd)
   if not stat.S_ISREG(info.st_mode) or not 0<info.st_size<=32768:raise Refused()
   raw=os.read(fd,32769)
   if len(raw)!=info.st_size or (lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns))(os.fstat(fd))!=(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns):raise Refused()
  finally:os.close(fd)
  result=preflight(raw,args.sha256)
  if args.action=='run-once':result['invalid'].append('trusted_production_assembly_unavailable')
 except Exception:result=dict(status='REFUSED',execution_allowed=False,invalid=['public_config_unavailable'])
 print(json.dumps(result,sort_keys=True));return 2

if __name__=='__main__':raise SystemExit(main())
