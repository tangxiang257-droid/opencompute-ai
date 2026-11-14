"""Private single-stage CUDA completion, never an authorization source.

No eager torch import/device access. Requires trusted authority and exclusive
capacity providers; tests replace the private runtime only. Device-wide fences
are deliberately conservative. They do not isolate another process or release
reservations. One scope/one load/one worker; failure is permanently UNKNOWN.
"""
from contextlib import contextmanager
import os,re,struct,threading
from .projection_driver import Spec,need,nothing
from .local_attempt import _non_suppressing

class _TorchRuntime:
    def __init__(self):
        import torch
        self.torch=torch
        self.retained=[]
    def check(self,device,version):
        t=self.torch
        need(t.__version__==version and t.version.cuda is not None)
        need(t.cuda.is_available() and int(device.split(':')[1])<t.cuda.device_count())
    def current_stream(self,device):return self.torch.cuda.current_stream(device).cuda_stream
    def synchronize(self,device):self.torch.cuda.synchronize(device)
    def copy(self,tensor,device,count):
        t=self.torch
        need(type(tensor)is t.Tensor and tensor.device==t.device(device)
             and tensor.dtype==t.float32 and tuple(tensor.shape)==(1,1,count)
             and tensor.is_contiguous() and tensor.numel()==count
             and tensor.storage_offset()==0 and tensor.untyped_storage().nbytes()==count*4)
        # Caller fenced production before copying. Keep both references until
        # the post-copy fence succeeds, including a raised asynchronous error.
        self.retained.append(tensor)
        host=tensor.detach().to(device='cpu',non_blocking=False,copy=True)
        self.retained.append(host)
        return host
    def encode(self,host,count):
        need(host.device.type=='cpu' and host.dtype==self.torch.float32 and host.numel()==count)
        # No Python list of the whole block (float objects exceed byte budget).
        raw=bytearray(count*4);flat=host.reshape(-1)
        for index in range(count):struct.pack_into('<f',raw,index*4,flat[index].item())
        return bytes(raw)

class DeviceCompletion:
    def __init__(self,spec,*,stage,device,torch_version,authority=None,capacity=None,_runtime=None):
        need(type(spec)is Spec and len(spec.inputs)==1)
        need(type(stage)is int and stage in (0,1))
        need(type(device)is str and re.fullmatch(r'cuda:(0|[1-9][0-9]*)',device) is not None)
        need(type(torch_version)is str and bool(torch_version))
        self.spec=spec;self.stage=stage;self.device=device;self.version=torch_version
        self.authority=authority;self.capacity=capacity;self.runtime=_runtime
        self.pid=os.getpid();self.lock=threading.RLock();self.stopped=threading.Event()
        self.state='NEW';self.scope=None;self.owner=None;self.worker=None;self.stream=None
        self.pending=None;self.host=None;self.load_started=False;self.loaded=False
    @property
    def state(self):return 'UNKNOWN' if self.stopped.is_set() else self._state
    @state.setter
    def state(self,value):self._state=value
    def check_loader(self,loader,stage):
        from .private_one_token import PinnedStageLoader
        need(type(loader)is PinnedStageLoader and stage==self.stage and not loader.retired)
        need(str(loader.args[1][stage])==self.device)
    def _live(self):need(self.pid==os.getpid() and not self.stopped.is_set() and self.state!='UNKNOWN')
    @contextmanager
    def _gates(self,action):
        self._live()
        for p in (self.authority,self.capacity):need(p is not None and callable(getattr(p,'hold_device',None)))
        with _non_suppressing(self.authority.hold_device(self.spec,self.device,self.stage,action)) as a,\
             _non_suppressing(self.capacity.hold_device(self.spec,self.device,self.stage,action)) as c:
            nothing(a);nothing(c);self._live();yield;self._live()
    @contextmanager
    def _guard(self):
        if not self.lock.acquire(blocking=False):
            self.stopped.set();raise ValueError('device completion busy')
        try:
            self._live();yield
        except BaseException:
            self.stopped.set();self.state='UNKNOWN';raise
        finally:self.lock.release()
    def open_scope(self,spec,token,scope):
        with self._guard(),self._gates('open'):
            need(spec==self.spec and type(token)is int and token==0 and self.state=='NEW')
            need(type(scope)is str and re.fullmatch('[A-Za-z0-9_-]{1,128}',scope) is not None)
            self.scope=scope;self.state='OPEN'
    @contextmanager
    def hold_load(self,spec,stage):
        with self._guard(),self._gates('load'):
            need(spec==self.spec and stage==self.stage and self.state=='OPEN' and not self.load_started)
            self.load_started=True
            if self.runtime is None:self.runtime=_TorchRuntime()
            self.runtime.check(self.device,self.version)
            self.runtime.synchronize(self.device)
            try:yield None
            except BaseException as primary:
                self.stopped.set();self.state='UNKNOWN'
                # Fence partial enqueue, but never recover eligibility.
                try:self.runtime.synchronize(self.device)
                except BaseException as cleanup:raise BaseExceptionGroup('load and completion failed',[primary,cleanup])
                raise
            self.runtime.synchronize(self.device);self._live();self.loaded=True
    def _execution(self,scope):
        need(scope==self.scope and self.loaded and self.state=='OPEN')
        need(self.worker==threading.get_ident() and self.stream==self.runtime.current_stream(self.device))
    def require_open(self,scope,spec,adapter):
        with self._guard(),self._gates('execute'):
            need(scope==self.scope and spec==self.spec and self.loaded and self.state=='OPEN')
            need(getattr(adapter,'stage_id',None)==self.stage and str(getattr(adapter,'_device',None))==self.device)
            need(self.owner is None or self.owner is adapter)
            if self.worker is None:
                self.worker=threading.get_ident();self.stream=self.runtime.current_stream(self.device);self.owner=adapter
            self._execution(scope)
    def materialize(self,scope,pending,size):
        with self._guard():
            with self._gates('materialize'):
                self._execution(scope)
                need(self.pending is None and pending.scope==scope and pending.owner is self.owner)
                need(type(pending.count)is int and type(size)is int and size==pending.count*4 and 0<size<=(65536 if self.stage==0 else 32768))
                self.pending=pending
                self.runtime.synchronize(self.device)
                self.host=self.runtime.copy(pending.tensor,self.device,pending.count)
                self.runtime.synchronize(self.device);self._live()
                raw=self.runtime.encode(self.host,pending.count)
                need(type(raw)is bytes and len(raw)==size)
                self._live()
            # Both trusted fences exited successfully; keep the operation lock.
            self._live();self.pending=None;self.host=None
            if isinstance(self.runtime,_TorchRuntime):self.runtime.retained.clear()
            return raw
    def finish_scope(self,scope):
        with self._guard(),self._gates('finish'):
            self._execution(scope);need(self.pending is None)
            self.runtime.synchronize(self.device);self._live();self.state='FINISHED'
    def require_finished(self,scope,adapter):
        # Role completion runs on the event-loop thread after A's worker exits.
        with self._guard(),self._gates('inspect-finished'):
            need(scope==self.scope and self.owner is adapter and self.state=='FINISHED' and self.pending is None)
    def quarantine(self,scope):
        # No waiting on a worker lock and no assertion of physical stopping.
        self.stopped.set()
        self.state='UNKNOWN'
        need(scope==self.scope)
