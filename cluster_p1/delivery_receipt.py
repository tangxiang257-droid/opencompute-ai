"""Single-loop receipt/credit adapter. No prefill, network setup or execution grant."""
import asyncio
from dataclasses import dataclass, fields
from .channel import Delivery
from .envelope import Envelope
from .handoff import Receiver, MAX_FRAME_BYTES
from .stage_ownership import StageOwner

def _with_cleanup(primary, cleanup):
    """Preserve an existing cause/context graph; keep cancellation at the top."""
    if primary is cleanup:return primary
    if primary.__cause__ is None and primary.__context__ is None:
        primary.__cause__=cleanup
        return primary
    group=BaseExceptionGroup('operation and cleanup failures',[primary,cleanup])
    if isinstance(primary,asyncio.CancelledError):
        cancelled=asyncio.CancelledError('operation cancelled; internal failures retained')
        cancelled.__cause__=group
        return cancelled
    return group

@dataclass(frozen=True,slots=True)
class AttemptState:
    registration: str='not_started'
    release: str='not_started'
    execution_allowed: bool=False
    retry_safe: bool=False
    consumption: str='not_requested'

class DeliveryAttempt:
    """Trusted single-event-loop ownership; not a hostile Python-object boundary.

    Construction copies only strictly immutable Envelope fields into bounded
    canonical bytes. No source Envelope reference is used during registration.
    Caller owns delivery until run claims it; cancellation before run starts is
    caller's responsibility. Once claimed, run waits for its one release attempt.
    """
    def __init__(self,receiver,delivery,*,consumer=None):
        if type(receiver) is not Receiver or type(delivery) is not Delivery:
            raise TypeError('real Receiver and Delivery required')
        e=delivery.envelope
        if type(e) is not Envelope:raise TypeError('real Envelope required')
        for name in ('task_id','session_id','model_hash','dtype'):
            if type(getattr(e,name)) is not str:raise TypeError('immutable string required')
        for name in ('execution_epoch','stage_id','tile_id','layout_version'):
            if type(getattr(e,name)) is not int:raise TypeError('exact integer required')
        for name in ('token_range','tensor_shape'):
            value=getattr(e,name)
            if type(value) is not tuple or any(type(x) is not int for x in value):
                raise TypeError('immutable integer tuple required')
        if type(e.payload) is not bytes:raise TypeError('immutable bytes required')
        frozen=Envelope(**{f.name:getattr(e,f.name) for f in fields(Envelope)})
        frozen.validate(MAX_FRAME_BYTES)
        frame=frozen.encode()
        if len(frame)>MAX_FRAME_BYTES:raise ValueError('canonical frame budget')
        if consumer is not None:
            if type(consumer) is not StageOwner:raise TypeError('ordinary StageOwner required')
            if consumer._loop is not asyncio.get_running_loop():raise ValueError('consumer loop mismatch')
            b=consumer._binding;edge=receiver._binding
            if (b.task_id,b.session_id,b.epoch,b.stage_id,b.model_hash,b.layout_hash)!=(
                frozen.task_id,frozen.session_id,frozen.execution_epoch,1,frozen.model_hash,edge.layout_hash):
                raise ValueError('consumer binding mismatch')
            if (edge.task_id,edge.session_id,edge.epoch)!=(b.task_id,b.session_id,b.epoch):
                raise ValueError('edge binding mismatch')
        self._payload=frozen.payload;self._range=frozen.token_range;self._consumer=consumer
        self._frame=frame;self._receiver=receiver;self._delivery=delivery
        self._started=False;self._state=AttemptState();self._loop=asyncio.get_running_loop()

    @property
    def state(self):return self._state

    async def run(self):
        if asyncio.get_running_loop() is not self._loop:raise RuntimeError('wrong event loop')
        d=self._delivery
        if self._started or d._released or getattr(d,'_p1_receipt_claimed',False):
            raise RuntimeError('delivery ownership unavailable')
        # No await between claim checks and mutation: single-loop callers only.
        self._started=True;d._p1_receipt_claimed=True
        try:return await self._process(d)
        finally:
            # Drop this adapter's holds only; external aliases are not revoked.
            self._frame=None;self._payload=None;self._delivery=None;self._consumer=None

    async def _process(self,d):
        self._state=AttemptState('unconfirmed','not_started')
        primary=None;result=None
        try:
            result=self._receiver.receive_bytes(self._frame)
            self._state=AttemptState('duplicate' if result['duplicate'] else 'recorded','not_started')
            if self._consumer is not None:
                if result['duplicate']:
                    self._state=AttemptState('duplicate','not_started',consumption='skipped_duplicate')
                else:
                    self._state=AttemptState('recorded','not_started',consumption='unconfirmed')
                    # Explicit cancellation checkpoint: receipt may already exist
                    # without any consumption. A duplicate never authorizes retry.
                    await asyncio.sleep(0)
                    await self._consumer.call(self._payload,token_start=self._range[0],
                                              token_count=self._range[1]-self._range[0])
                    self._state=AttemptState('recorded','not_started',consumption='completed')
        except BaseException as exc:primary=exc
        if primary is not None and self._consumer is not None and self._state.consumption=='unconfirmed':
            # Even cancellation at the pre-call checkpoint retires the supplied
            # instance: a persisted receipt is not permission to try again.
            try:await self._consumer.close()
            except BaseException as close_error:
                if isinstance(close_error,asyncio.CancelledError):
                    primary=_with_cleanup(close_error,primary)
                else:
                    primary=BaseExceptionGroup('consumption and owner close failures',[primary,close_error])
        registration=self._state.registration;consumption=self._state.consumption
        self._state=AttemptState(registration,'pending',consumption=consumption)
        # Strong task reference and shield: caller cancellation does not abandon
        # the credit operation. Never retry a failed/cancelled release.
        release_coro=d.release()
        try:task=asyncio.create_task(release_coro)
        except BaseException as exc:
            release_coro.close()
            self._state=AttemptState(registration,'unconfirmed',consumption=consumption)
            if primary is not None:raise _with_cleanup(primary,exc)
            raise
        cancellation=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as exc:
                if not task.cancelled():cancellation=exc
            except BaseException:break
        release_error=None
        try:task.result()
        except BaseException as exc:release_error=exc
        self._state=AttemptState(registration,'unconfirmed' if release_error else 'completed',consumption=consumption)
        if cancellation is not None:
            faults=[x for x in (primary, release_error) if x is not None]
            # Keep cancellation as the propagated type, but retain BOTH failures
            # without overwriting either original exception's own cause chain.
            if len(faults)>1:
                raise _with_cleanup(cancellation,BaseExceptionGroup('receipt and release failures', faults))
            if faults:raise _with_cleanup(cancellation,faults[0])
            raise cancellation
        if primary is not None:
            if release_error is not None:raise _with_cleanup(primary,release_error)
            raise primary
        if release_error is not None:raise release_error
        return result
