"""Ordinary-byte stage-call lifecycle experiment, NOT a Tensor/prefill adapter."""
import asyncio
from dataclasses import dataclass
import hashlib
import inspect
import threading
from concurrent.futures import ThreadPoolExecutor
from pydantic import TypeAdapter
from .contracts import Identifier, Digest

class OwnershipError(ValueError):pass

@dataclass(frozen=True,slots=True)
class StageBinding:
    task_id:str
    session_id:str
    epoch:int
    stage_id:int
    model_hash:str
    layout_hash:str
    def __post_init__(self):
        for x in (self.task_id,self.session_id):TypeAdapter(Identifier).validate_python(x,strict=True)
        for x in (self.model_hash,self.layout_hash):TypeAdapter(Digest).validate_python(x,strict=True)
        if type(self.epoch) is not int or not 1<=self.epoch<2**31:raise OwnershipError('epoch')
        if type(self.stage_id) is not int or self.stage_id not in (0,1):raise OwnershipError('stage')

@dataclass(frozen=True,slots=True)
class CallSpec:
    binding:StageBinding
    token_start:int
    token_count:int
    input_digest:str

@dataclass(frozen=True,slots=True)
class OwnerState:
    status:str
    next_token:int
    commit:str='not_attempted'
    execution_allowed:bool=False


def _sync(value):
    if inspect.isawaitable(value):
        # Callback contract transfers only a freshly-created native coroutine.
        # Started coroutines may own resources or run finally code on close;
        # their creator, not this boundary (possibly a worker thread), owns them.
        if inspect.iscoroutine(value) and inspect.getcoroutinestate(value)==inspect.CORO_CREATED:
            value.close()
        raise OwnershipError('synchronous callbacks required')
    return value

class StageOwner:
    def __init__(self,binding,operation,*,authorize,commit):
        if type(binding) is not StageBinding:raise TypeError('StageBinding required')
        binding=StageBinding(binding.task_id,binding.session_id,binding.epoch,binding.stage_id,binding.model_hash,binding.layout_hash)
        if not all(callable(f) and not inspect.iscoroutinefunction(f) for f in (operation,authorize,commit)):
            raise TypeError('synchronous trusted callbacks required')
        self._binding=binding;self._operation=operation;self._authorize=authorize;self._commit=commit
        self._loop=asyncio.get_running_loop();self._state=OwnerState('idle',0)
        self._stop=threading.Event();self._done=None
        self._executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix='p1-ordinary-call')
    @property
    def state(self):return self._state
    def _loop_check(self):
        if asyncio.get_running_loop() is not self._loop:raise OwnershipError('single owning loop required')
    def _retire(self):
        self._stop.set()
        self._state=OwnerState('retiring' if self._done is not None else 'retired',self._state.next_token,self._state.commit)
    def _invoke(self,value,spec):
        # This is an admission fence, not an atomic stop of a running function.
        if self._stop.is_set():raise OwnershipError('admission cancelled')
        result=_sync(self._operation(value,token_start=spec.token_start,stage_id=spec.binding.stage_id))
        if type(result) is not bytes or len(result)>65536:raise OwnershipError('ordinary bounded bytes result required')
        return result
    async def call(self,value,*,token_start,token_count):
        self._loop_check()
        if self._state.status!='idle':raise OwnershipError('instance unavailable')
        if (type(value) is not bytes or not 1<=len(value)<=65536
            or type(token_start) is not int or token_start!=self._state.next_token
            or type(token_count) is not int or not 1<=token_count<=1048576
            or token_start+token_count>2**31-1):raise OwnershipError('invalid input/range')
        spec=CallSpec(self._binding,token_start,token_count,hashlib.sha256(value).hexdigest())
        self._done=self._loop.create_future();self._state=OwnerState('busy',token_start)
        try:
            # On this loop there is no await between authorization and enqueue.
            # Trusted authorize must grant one admitted call, not return cached
            # metadata or imply instantaneous authority at eventual CPU start.
            if _sync(self._authorize(spec)) is not True:raise OwnershipError('admission denied')
            if self._stop.is_set():raise OwnershipError('instance closing')
            # Keep the concurrent Future, not merely an asyncio wrapper whose
            # cancellation could finish while the underlying thread still runs.
            job=self._executor.submit(self._invoke,value,spec)
            exited=self._loop.create_future()
            def signal():
                if not exited.done():exited.set_result(None)
            job.add_done_callback(lambda unused:self._loop.call_soon_threadsafe(signal))
            cancelled=None
            while not job.done():
                try:await asyncio.shield(exited)
                except asyncio.CancelledError as exc:
                    cancelled=exc;self._retire()
            result=None;failure=None
            try:result=job.result()
            except BaseException as exc:failure=exc
            if cancelled is not None:raise cancelled from failure
            if failure is not None:raise failure
            if self._stop.is_set():raise OwnershipError('late result discarded')
            self._state=OwnerState('busy',token_start,'unconfirmed')
            # Trusted callback performs its own current-authority transaction.
            # It is synchronous on the owning loop; no worker-thread DB calls.
            if _sync(self._commit(spec,result)) is not None:raise OwnershipError('commit callback contract')
            if self._stop.is_set():raise OwnershipError('instance closed during commit')
            self._state=OwnerState('idle',token_start+token_count,'confirmed')
            return result
        except BaseException:
            self._retire();raise
        finally:
            if self._state.status=='retiring':
                self._state=OwnerState('retired',self._state.next_token,self._state.commit)
                self._executor.shutdown(wait=False,cancel_futures=False)
            done=self._done;self._done=None;done.set_result(None)
    async def close(self):
        self._loop_check();self._retire()
        done=self._done;cancelled=None
        if done is not None:
            while not done.done():
                try:await asyncio.shield(done)
                except asyncio.CancelledError as exc:cancelled=exc
        self._state=OwnerState('retired',self._state.next_token,self._state.commit)
        self._executor.shutdown(wait=False,cancel_futures=False)
        if cancelled is not None:raise cancelled
