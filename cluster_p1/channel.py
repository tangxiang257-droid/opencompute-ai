"""Persistent mutually authenticated TLS stream with bounded consumer credits."""
import asyncio
from dataclasses import dataclass
import hashlib
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

    async def receive(self):
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

    def __init__(self, r, w, config, grants, capacity_bytes=MAX_PAYLOAD, capacity_tiles=8, timeout=10, *, _gate_receipt=False):
        if type(_gate_receipt)is not bool:raise TypeError('explicit receipt gate flag required')
        self._receipt_state='unprepared' if _gate_receipt else None
        self._receipt_h_started=False
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
        self._error = None
        self._task = asyncio.create_task(self._reader())

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
        with self._identity_lock:
            if (type(expected) is not ChannelIdentity or expected != self._identity
                    or self._identity_revoked or self._closed or self.w.is_closing()):
                raise PermissionError('channel identity unavailable')
            yield self._identity

    def revoke_identity(self):
        self._identity_loop_check()
        self._abort()

    async def start(self):
        if self._receipt_state is not None:
            # Opt-in only: reserve before the first await, even if the write lock
            # or drain blocks. A cancelled/unknown advertisement is never retried.
            if self._receipt_state!='ready' or self._receipt_h_started:
                raise ChannelError('receipt capacity not ready or already advertised')
            self._receipt_h_started=True
            try:
                await self._frame(b'H',struct.pack('!QI',self.capacity_bytes,self.capacity_tiles))
            except BaseException:
                self._abort();raise
            return self
        await self._frame(b'H', struct.pack('!QI', self.capacity_bytes, self.capacity_tiles))
        return self

    async def _frame(self, kind, data):
        async with self._write:
            if self._closed:
                raise ChannelError('disconnected')
            try:
                self.w.write(kind + struct.pack('!I', len(data)) + data)
                await asyncio.wait_for(self.w.drain(), self.timeout)
            except BaseException:
                self._abort()
                raise

    def _abort(self):
        with self._identity_lock:
            if self._receipt_state is not None:self._receipt_state='closed'
            self._identity_revoked = True
            if not self._closed:
                self._closed = True
                self.w.close()
                asyncio.create_task(self._wake())

    async def _wake(self):
        async with self._condition:
            self._condition.notify_all()

    async def _reader(self):
        try:
            while True:
                header = await asyncio.wait_for(self.r.readexactly(5), self.timeout)
                kind = header[:1]
                n = struct.unpack('!I', header[1:])[0]
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
            self._error = exc
        finally:
            self._abort()

    async def send(self, envelope, *, max_frame_bytes=None):
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
            await self._frame(b'D', frame)
        except BaseException:
            self._abort()
            raise

    async def receive(self):

        async def get():
            async with self._condition:
                while self._queue.empty():
                    if self._closed:
                        raise ChannelError('disconnected') from self._error
                    await self._condition.wait()
                return self._queue.get_nowait()
        return await asyncio.wait_for(get(), self.timeout)

    async def _release(self, size):
        self._used_bytes -= size
        self._used_tiles -= 1
        self._release_seq += 1
        try:
            await self._frame(b'C', struct.pack('!QIQ', size, 1, self._release_seq))
        except BaseException:
            # Accounting is committed. A cancelled lock acquisition or write
            # cannot safely leave this stream available for another release.
            self._abort()
            raise

    async def close(self):
        self._abort()
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        try:
            await asyncio.wait_for(self.w.wait_closed(), self.timeout)
        except Exception:
            pass

async def accept(reader, writer, config, grants, **kwargs):
    try:
        return await Channel(reader, writer, config, grants, **kwargs).start()
    except BaseException:
        writer.close()
        raise

async def connect(host, port, config, grants, server_hostname, **kwargs):
    r, w = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=config.context(), server_hostname=server_hostname), kwargs.get('timeout', 10))
    return await accept(r, w, config, grants, **kwargs)
