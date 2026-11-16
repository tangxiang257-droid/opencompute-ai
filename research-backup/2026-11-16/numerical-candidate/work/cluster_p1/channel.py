"""Persistent mutually authenticated TLS stream with bounded consumer credits."""
import asyncio
from dataclasses import dataclass
import hashlib
import inspect
import ssl
import struct
import secrets
import threading
from contextlib import contextmanager
from typing import Protocol
from .envelope import Envelope, decode, MAX_PAYLOAD, MAX_HEADER, PREFIX

class ChannelError(Exception):
    pass

class Transport(Protocol):

    async def send(self, envelope: Envelope):
        ...

    async def receive(self,*,_session_owner=None):
        ...

    async def close(self):
        ...

@dataclass(frozen=True)
class TLSConfig:
    certfile: str
    keyfile: str
    cafile: str
    peers: dict[str, str]

    def context(self, server=False):
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH if server else ssl.Purpose.SERVER_AUTH, cafile=self.cafile)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_cert_chain(self.certfile, self.keyfile)
        return ctx

@dataclass(frozen=True)
class Grant:
    task_id: str
    session_id: str
    execution_epoch: int
    stage_id: int
    token_range: tuple[int, int]
    model_hash: str
    layout_version: int

    def permits(self, e):
        return (e.task_id, e.session_id, e.execution_epoch, e.stage_id, e.model_hash, e.layout_version) == (self.task_id, self.session_id, self.execution_epoch, self.stage_id, self.model_hash, self.layout_version) and self.token_range[0] <= e.token_range[0] < e.token_range[1] <= self.token_range[1]

class Delivery:

    def __init__(self, channel, envelope, size):
        self.envelope = envelope
        self._channel = channel
        self._size = size
        self._released = False

    async def release(self):
        if self._released:
            raise ChannelError('already released')
        self._released = True
        await self._channel._release(self._size)
    commit = release

@dataclass(frozen=True, slots=True)
class ChannelIdentity:
    node_id: str
    certificate_sha256: str
    connection_generation: str

class Channel:

    def __init__(self, r, w, config, grants, capacity_bytes=MAX_PAYLOAD, capacity_tiles=8, timeout=10, *, _gate_receipt=False, _single_start=False, _idle_aware=False):
        if type(_gate_receipt)is not bool:raise TypeError('explicit receipt gate flag required')
        if type(_single_start)is not bool:raise TypeError('explicit single-start flag required')
        if type(_idle_aware)is not bool:raise TypeError('explicit idle transition flag required')
        self._idle_aware_choice=_idle_aware
        self._undelivered=[]  # Popped but not delivered: still charged, never ACKed.
        self._single_start=_single_start
        self._receipt_state='unprepared' if _gate_receipt else None
        self._receipt_h_started=False
        self._start_attempted=False
        obj = w.get_extra_info('ssl_object')
        if obj is None or obj.context.verify_mode != ssl.CERT_REQUIRED:
            raise ChannelError('TLS certificate validation is required')
        if obj.version() not in ('TLSv1.2', 'TLSv1.3'):
            raise ChannelError('TLS 1.2 or newer is required')
        if not obj.server_side and not obj.context.check_hostname:
            raise ChannelError('TLS client hostname validation is required')
        cert = obj.getpeercert(binary_form=True)
        fp = hashlib.sha256(cert).hexdigest() if cert else None
        nodes = [node for node, digest in config.peers.items() if digest == fp]
        if len(nodes) != 1:
            raise ChannelError('unauthenticated node')
        if not 1 <= capacity_bytes <= MAX_PAYLOAD or not 1 <= capacity_tiles <= 1024:
            raise ChannelError('capacity bounds')
        self._identity_lock = threading.RLock()
        self._identity_loop = asyncio.get_running_loop()
        self._identity_revoked = False
        self._identity = ChannelIdentity(nodes[0], fp, secrets.token_hex(16))
        self.peer_node = nodes[0]
        self.r = r
        self.w = w
        self.grants = frozenset(grants.get(self.peer_node, ()))
        self.capacity_bytes = capacity_bytes
        self.capacity_tiles = capacity_tiles
        self.timeout = timeout
        self._data_session_owner=None;self._session_idle_owner=None;self._idle_changed=asyncio.Event()
        self._used_bytes = 0
        self._used_tiles = 0
        self._credits_bytes = 0
        self._credits_tiles = 0
        self._peer_capacity = None
        self._credit_seq = 0
        self._release_seq = 0
        self._condition = asyncio.Condition()
        self._write = asyncio.Lock()
        self._queue = asyncio.Queue(maxsize=capacity_tiles)
        self._closed = False
        self._first_failure = None
        self._cleanup_failure = None
        self._error = None
        self._close_error = None
        self._task = asyncio.create_task(self._reader())

    @property
    def _idle_aware(self):
        return self._idle_aware_choice

    def _identity_loop_check(self):
        if asyncio.get_running_loop() is not self._identity_loop:
            raise ChannelError('identity fence requires owning loop')

    @property
    def authenticated_identity(self):
        self._identity_loop_check()
        return self._identity

    @contextmanager
    def identity_fence(self, expected):
        self._identity_loop_check()
        if self._session_idle_owner is not None:self._session_idle_owner.check()
        if self._data_session_owner is not None:self._data_session_owner.check()
        with self._identity_lock:
            if (type(expected) is not ChannelIdentity or expected != self._identity
                    or self._identity_revoked or self._closed or self.w.is_closing()):
                raise PermissionError('channel identity unavailable')
            yield self._identity

    def revoke_identity(self):
        self._identity_loop_check()
        self._abort()

    async def start(self):
        self._identity_loop_check()
        if self._closed or self.w.is_closing():raise ChannelError('disconnected')
        if self._data_session_owner is not None and not self._data_session_owner.active:raise ChannelError('data session not ready')
        if self._start_attempted and (self._single_start or self._receipt_state is not None):
            raise ChannelError('local capacity advertisement already attempted')
        if self._receipt_state is not None and (self._receipt_state!='ready' or self._receipt_h_started):
            # An early call cannot bypass preparation or burn its future ready H.
            raise ChannelError('receipt capacity not ready')
        # Track every real H attempt, before any lock/drain await. Private
        # single-start and receipt paths reject duplicates without aborting the
        # first caller. Legacy ordinary Channels retain repeat-start behavior.
        self._start_attempted=True
        if self._receipt_state is not None:self._receipt_h_started=True
        try:
            await self._frame(b'H',struct.pack('!QI',self.capacity_bytes,self.capacity_tiles))
        except BaseException as error:
            self._record_failure('unknown', error)
            self._abort();raise
        return self

    async def _frame(self, kind, data, *, write_fence=None):
        async with self._write:
            if self._closed:
                raise ChannelError('disconnected')
            phase = 'write'
            try:
                if write_fence is None:
                    self.w.write(kind + struct.pack('!I', len(data)) + data)
                else:
                    from .local_attempt import _non_suppressing
                    phase = 'fence'
                    if not callable(write_fence):raise ChannelError('synchronous write fence required')
                    phase = 'fence'
                    context=write_fence()
                    if inspect.isawaitable(context):
                        if inspect.iscoroutine(context):context.close()
                        raise ChannelError('synchronous write fence required')
                    with _non_suppressing(context) as allowed:
                        if allowed is not None:raise ChannelError('write fence must yield None')
                        phase = 'write'
                        self.w.write(kind + struct.pack('!I', len(data)) + data)
                        phase = 'fence'
                phase = 'drain'
                await asyncio.wait_for(self.w.drain(), self.timeout)
            except BaseException as error:
                self._record_failure(phase, error)
                self._abort()
                raise

    def _record_failure(self, phase, error, *, cleanup=False):
        """Internal bounded observations only; never store/format the exception.

        First reason entering termination wins, not necessarily the root cause.
        Existing raw _error/_close_error and propagation remain unchanged.
        """
        try:
            phases = ('fence', 'write', 'drain', 'read', 'close', 'unknown')
            if type(phase) is not str or phase not in phases:
                phase = 'unknown'
            kind = type(error)
            if kind is asyncio.CancelledError:
                category = 'cancelled'
            elif kind is TimeoutError:
                category = 'deadline'
            elif any(kind is allowed for allowed in (ChannelError, ValueError, PermissionError)):
                category = 'rejected'
            elif any(kind is allowed for allowed in (OSError, ConnectionError, BrokenPipeError,
                          ConnectionResetError, asyncio.IncompleteReadError)):
                category = 'io'
            else:
                category = 'internal'
            record = (phase, category)
            with self._identity_lock:
                if self._first_failure is None:
                    self._first_failure = record
                if cleanup is True and self._cleanup_failure is None:
                    self._cleanup_failure = record
        except BaseException:
            # Diagnostics are best effort and must not prevent original teardown.
            # No raw diagnostic failure is retained or formatted.
            pass

    def _abort(self):
        with self._identity_lock:
            if self._receipt_state is not None:self._receipt_state='closed'
            self._identity_revoked = True
            if not self._closed:
                self._closed = True
                try:self.w.close()
                except BaseException as e:
                    self._record_failure('close', e, cleanup=True)
                    self._close_error=e
                    raise
                finally:
                    asyncio.create_task(self._wake())
                    if self._data_session_owner is not None:self._data_session_owner._channel_closed(self._error)
        if self._data_session_owner is not None:self._data_session_owner._channel_closed(self._error)

    async def _wake(self):
        async with self._condition:
            self._condition.notify_all()

    async def _header(self):
        loop=asyncio.get_running_loop();deadline=loop.time()+self.timeout
        self._reader_idle_deadline=deadline
        first=asyncio.create_task(self.r.readexactly(1));change=asyncio.create_task(self._idle_changed.wait())
        try:
            while True:
                owner=self._session_idle_owner
                if owner is None and self._data_session_owner is not None and self._data_session_owner.active:owner=self._data_session_owner
                if owner is not None:owner.check()
                remaining=max(0,(min(owner.deadline,getattr(owner,'expires',owner.deadline)) if owner is not None else deadline)-loop.time())
                done,_=await asyncio.wait((first,change),timeout=remaining,return_when=asyncio.FIRST_COMPLETED)
                if self._closed or self._identity_revoked:raise ChannelError('disconnected')
                if owner is not None:owner.check()
                elif loop.time()>=deadline:raise TimeoutError()
                if first in done:
                    lead=first.result();break
                if change in done:
                    self._idle_changed.clear();change=asyncio.create_task(self._idle_changed.wait());continue
                if owner is not None:continue
                raise TimeoutError()
            budget=self.timeout if owner is not None else max(0,deadline-loop.time())
            return lead+await asyncio.wait_for(self.r.readexactly(4),budget)
        finally:
            self._reader_idle_deadline=None
            for task in (first,change):
                if not task.done():task.cancel()
            await asyncio.gather(first,change,return_exceptions=True)

    async def _reader(self):
        try:
            while True:
                if self._idle_aware or getattr(self,"_online_issuance_claimed",False) or self._data_session_owner is not None:header = await self._header()
                else:
                    self._reader_idle_deadline=asyncio.get_running_loop().time()+self.timeout
                    try:header = await asyncio.wait_for(self.r.readexactly(5), self.timeout)
                    finally:self._reader_idle_deadline=None
                kind = header[:1]
                n = struct.unpack('!I', header[1:])[0]
                if kind==b'P':
                    from .data_session import FRAME_SIZE
                    if self._data_session_owner is None or n!=FRAME_SIZE:raise ChannelError('data pulse bound/type')
                    self._data_session_owner._accept(await asyncio.wait_for(self.r.readexactly(n),self.timeout))
                    continue
                # Latch rejection at D-header observation, before any body await.
                # Buffered bytes processed only after ready cannot reveal send time.
                if kind==b'D' and self._receipt_state not in (None,'ready'):
                    raise ChannelError('receipt receiver is not prepared')
                bound = MAX_PAYLOAD + MAX_HEADER + PREFIX.size + 32 if kind == b'D' else 20
                if kind==b'D' and self._receipt_state is not None:
                    bound=min(bound,65536)  # private receipt complete-frame cap, not RSS
                if n > bound or kind not in (b'H', b'C', b'D'):
                    raise ChannelError('frame bound/type')
                if kind == b'D' and (self._used_tiles >= self.capacity_tiles or n > self.capacity_bytes - self._used_bytes + MAX_HEADER + PREFIX.size + 32):
                    raise ChannelError('receiver capacity exceeded')
                data = await asyncio.wait_for(self.r.readexactly(n), self.timeout)
                if kind in (b'H', b'C'):
                    if n != (12 if kind == b'H' else 20):
                        raise ChannelError('control size')
                    byte, tiles = struct.unpack('!QI', data[:12])
                    if kind == b'C':
                        seq = struct.unpack('!Q', data[12:])[0]
                        if seq != self._credit_seq + 1:
                            raise ChannelError('duplicate/out of order credit')
                        self._credit_seq = seq
                    async with self._condition:
                        if kind == b'H':
                            if self._peer_capacity is not None or not 1 <= byte <= MAX_PAYLOAD or (not 1 <= tiles <= 1024):
                                raise ChannelError('invalid capacity grant')
                            self._peer_capacity = (byte, tiles)
                        elif self._peer_capacity is None or byte == 0 or tiles != 1:
                            raise ChannelError('invalid credit grant')
                        self._credits_bytes += byte
                        self._credits_tiles += tiles
                        if self._credits_bytes > self._peer_capacity[0] or self._credits_tiles > self._peer_capacity[1]:
                            raise ChannelError('excess credit')
                        self._condition.notify_all()
                else:
                    e = decode(data, max_payload=self.capacity_bytes)
                    if not any((g.permits(e) for g in self.grants)):
                        raise ChannelError('unauthorized context')
                    size = len(e.payload)
                    if self._used_bytes + size > self.capacity_bytes:
                        raise ChannelError('byte capacity')
                    self._used_bytes += size
                    self._used_tiles += 1
                    self._queue.put_nowait(Delivery(self, e, size))
                    await self._wake()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._record_failure('read', exc)
            self._error = exc
        finally:
            self._abort()

    async def send(self, envelope, *, max_frame_bytes=None, write_fence=None):
        # Serialize before credit reservation or wire I/O. The exact bytes checked
        # here are sent after the await, never a second potentially changed encoding.
        frame = envelope.encode()
        if type(frame) is not bytes:
            raise ValueError('immutable encoded envelope required')
        if max_frame_bytes is not None:
            if (type(max_frame_bytes) is not int
                    or not 1 <= max_frame_bytes <= MAX_PAYLOAD + MAX_HEADER + PREFIX.size + 32):
                raise ValueError('invalid complete envelope bound')
            if len(frame) > max_frame_bytes:
                raise ValueError('complete envelope exceeds receiver bound')
        # Caller fields / dynamic encode overrides are not an authority. Decode
        # the frozen bytes and use only that wire identity and payload for grants
        # and credits. A valid but different encoding gets no caller privilege.
        wire_envelope = decode(frame)
        if not any((g.permits(wire_envelope) for g in self.grants)):
            raise ChannelError('unauthorized context')
        size = len(wire_envelope.payload)

        async def reserve():
            async with self._condition:
                while True:
                    if self._closed:
                        raise ChannelError('disconnected')
                    if self._peer_capacity and size > self._peer_capacity[0]:
                        raise ChannelError('exceeds peer capacity')
                    if self._credits_bytes >= size and self._credits_tiles:
                        self._credits_bytes -= size
                        self._credits_tiles -= 1
                        return
                    await self._condition.wait()
        await asyncio.wait_for(reserve(), self.timeout)
        try:
            if write_fence is None:await self._frame(b'D', frame)
            else:await self._frame(b'D', frame,write_fence=write_fence)
        except BaseException as error:
            self._record_failure('unknown', error)
            self._abort()
            raise

    async def receive(self,*,_session_owner=None):
        def current():
            if _session_owner is None:return
            if self._closed or self._identity_revoked or self.w.is_closing():raise ChannelError('disconnected')
            if _session_owner is not self._data_session_owner and _session_owner is not self._session_idle_owner:raise ChannelError('foreign session owner')
            _session_owner.check()
        async def get():
            async with self._condition:
                while self._queue.empty():
                    if self._closed:raise ChannelError('disconnected') from self._error
                    await self._condition.wait()
                current()
                delivery=self._queue.get_nowait()
                self._undelivered.append(delivery)
                return delivery
        def checked(delivery):
            current()
            self._undelivered.remove(delivery)
            return delivery
        budget=self.timeout
        if _session_owner is not None and _session_owner is self._data_session_owner:
            if _session_owner.binding['purpose'] not in ('action-session','control-session') or not _session_owner.active:raise ChannelError('inactive session')
            while True:
                current()
                try:
                    async with asyncio.timeout_at(min(_session_owner.deadline,_session_owner.expires)):return checked(await get())
                except TimeoutError:_session_owner.check()
        if _session_owner is not None:
            current();budget=max(0,_session_owner.deadline-asyncio.get_running_loop().time())
        return checked(await asyncio.wait_for(get(), budget))

    async def _release(self, size):
        self._used_bytes -= size
        self._used_tiles -= 1
        self._release_seq += 1
        try:
            await self._frame(b'C', struct.pack('!QIQ', size, 1, self._release_seq))
        except BaseException as error:
            self._record_failure('unknown', error)
            # Accounting is committed. A cancelled lock acquisition or write
            # cannot safely leave this stream available for another release.
            self._abort()
            raise

    async def close(self):
        error=self._close_error
        try:self._abort()
        except BaseException as e:error=e
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        if self._data_session_owner is not None:
            await asyncio.gather(*(t for t in self._data_session_owner.tasks if t is not asyncio.current_task()),return_exceptions=True)
        try:
            await asyncio.wait_for(self.w.wait_closed(), self.timeout)
        except Exception:
            pass
        if error is not None:raise error

async def _failed_owned_stream(error, channel, writer, timeout):
    """Retain and join local cleanup even if its caller is cancelled again."""
    async def cleanup():
        errors=[]
        if channel is not None:
            channel._identity_loop_check()
            try:await channel.close()
            except BaseException as exc:
                errors.append(exc)
                try:await asyncio.wait_for(writer.wait_closed(),timeout)
                except BaseException as wait_error:errors.append(wait_error)
            finally:
                if not channel._task.done():channel._task.cancel()
                await asyncio.gather(channel._task,return_exceptions=True)
        else:
            try:writer.close()
            except BaseException as exc:errors.append(exc)
            try:await asyncio.wait_for(writer.wait_closed(),timeout)
            except BaseException as exc:errors.append(exc)
        if errors:raise BaseExceptionGroup('supplied transport cleanup failed',errors)
    task=asyncio.create_task(cleanup());cancelled=None
    while not task.done():
        try:await asyncio.shield(task)
        except asyncio.CancelledError as exc:cancelled=exc
        except BaseException:break
    errors=[error]
    try:task.result()
    except BaseException as exc:errors.append(exc)
    if cancelled is not None and cancelled is not error:errors.append(cancelled)
    if len(errors)>1:raise BaseExceptionGroup('transport construction/start failed',errors)
    raise error


async def _accept_unstarted(reader, writer, config, grants, **kwargs):
    """Private supplied-stream seam: validates TLS, starts reader, sends NO H.

    Ownership transfers to this helper, including on constructor failure. It does
    not mark a B receipt gate ready. PreparedReceipt alone owns that transition.
    """
    try:
        kwargs.setdefault('_single_start',True)
        return Channel(reader,writer,config,grants,**kwargs)
    except BaseException as error:
        await _failed_owned_stream(error,None,writer,kwargs.get('timeout',10))


async def _connect_unstarted(host, port, config, grants, server_hostname, **kwargs):
    """Private TLS dial returning a Channel before local H; no retry policy."""
    r,w=await asyncio.wait_for(asyncio.open_connection(host,port,ssl=config.context(),
        server_hostname=server_hostname),kwargs.get('timeout',10))
    return await _accept_unstarted(r,w,config,grants,**kwargs)


async def accept(reader, writer, config, grants, **kwargs):
    kwargs.setdefault('_single_start',False) # preserve legacy ordinary repeat-start
    channel=await _accept_unstarted(reader,writer,config,grants,**kwargs)
    try:return await channel.start()
    except BaseException as error:
        await _failed_owned_stream(error,channel,writer,kwargs.get('timeout',10))


async def connect(host, port, config, grants, server_hostname, **kwargs):
    r, w = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=config.context(), server_hostname=server_hostname), kwargs.get('timeout', 10))
    return await accept(r, w, config, grants, **kwargs)
