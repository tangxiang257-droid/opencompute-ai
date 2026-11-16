"""Bounded ordinary-byte object framing; no model, node or execution runtime.

One frame credit, one logical object, sequential fixed-size chunks, no resume.
Caller reserves full persistent storage quota BEFORE constructing this receiver.
"""
import asyncio
from dataclasses import asdict, dataclass, replace
import hashlib
import hmac
import json
import struct
import time

from cluster_p1.channel import Channel, Grant
from isolated_tls_delivery import Denied

CHUNK = 32768
MAX_OBJECT = 2*1024*1024
MAX_FRAMES = 64
OVERHEAD = 48


@dataclass(frozen=True, slots=True)
class ObjectContext:
    binding: object
    object_id: str
    total_bytes: int
    sha256: str
    connection_generation: str
    version: int = 1


@dataclass(frozen=True, slots=True)
class ResultTileContext:
    binding: object
    object_id: str
    total_bytes: int
    sha256: str
    connection_generation: str
    set_digest: str
    tile_index: int
    token_range: tuple
    version: int = 2


@dataclass(frozen=True, slots=True)
class MiddleTileContext:
    binding: object
    object_id: str
    total_bytes: int
    sha256: str
    connection_generation: str
    tile_index: int
    token_range: tuple
    version: int = 3


def _magic(version):
    return {1:b'DFR1',2:b'DFR2',3:b'DFM1'}[version]


def tag(context):
    if context.version not in (1,2,3):raise Denied()
    raw=json.dumps(asdict(context),sort_keys=True,separators=(',', ':'),ensure_ascii=True,allow_nan=False).encode('ascii')
    return hashlib.sha256(b'opencompute/disk-framed-result/v'+str(context.version).encode('ascii')+b'\0'+raw).digest()


def payload(context, sequence, offset, block):
    if type(block) is not bytes or not 0<len(block)<=CHUNK:raise Denied()
    return _magic(context.version)+tag(context)+struct.pack('!IQ',sequence,offset)+block


class ObjectReceiver:
    def __init__(self, reader, writer, config, binding, object_id, size, digest, *, middle_tile=None):
        if type(size) is not int or not 1<=size<=MAX_OBJECT or (size+CHUNK-1)//CHUNK>MAX_FRAMES:raise Denied()
        for value in (object_id,digest):
            if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):raise Denied()
        scope=binding.scope
        if middle_tile is not None:
            if type(middle_tile) is not int or not 0<=middle_tile<64 or binding.stage_id!=0 or scope.port!='hidden_states' or scope.direction!='stage0->stage1' or len(binding.tiles)!=1:raise Denied()
        elif binding.stage_id!=1:raise Denied()
        tls=writer.get_extra_info('ssl_object')
        if tls is None:raise Denied()
        cert=tls.getpeercert(binary_form=True)
        if not cert or hashlib.sha256(cert).hexdigest()!=scope.peer_certificate_der_sha256:raise Denied()
        unique=tls.get_channel_binding('tls-unique')
        if type(unique) is not bytes or not unique:raise Denied()
        generation=hashlib.sha256(b'opencompute/tls-incarnation/v1\0'+unique).hexdigest()
        binding=replace(binding,scope=replace(scope,incarnation=generation))
        self.context=ObjectContext(binding,object_id,size,digest,generation)
        if middle_tile is not None:
            start,end,_=binding.tiles[0]
            self.context=MiddleTileContext(binding,object_id,size,digest,generation,middle_tile,(start,end))
        grant=Grant(scope.task,scope.session,scope.epoch,binding.stage_id,(binding.tiles[0][0],binding.tiles[-1][1]),binding.model_hash,binding.layout_version)
        self._channel=Channel(reader,writer,config,{scope.peer_node:{grant}},capacity_bytes=CHUNK+OVERHEAD,capacity_tiles=1)
        self._fenced=False;self._event=asyncio.Event();self._deadline=time.monotonic()+60
        self._timer=asyncio.get_running_loop().call_later(60,self._fence)
        self.written=0;self.frames=0;self.active=False;self.held=None

    def check(self):
        if self._fenced or self._channel._closed or time.monotonic()>=self._deadline:raise Denied()

    def _fence(self):
        self._fenced=True;self._event.set();self._timer.cancel();self._channel._abort()
        while not self._channel._queue.empty():
            abandoned=self._channel._queue.get_nowait();abandoned.envelope=None
        # Dead connection credit is retired, never reused.
        self._channel._used_bytes=0;self._channel._used_tiles=0

    async def close(self):
        self._fence();await self._channel.close()

    def validate(self, delivery):
        self.check()
        e=delivery.envelope;c=self.context;b=c.binding;s=b.scope
        if (e.task_id,e.session_id,e.execution_epoch,e.stage_id,e.model_hash,e.layout_version,e.dtype)!=(s.task,s.session,s.epoch,b.stage_id,b.model_hash,b.layout_version,'uint8'):raise Denied()
        remaining=c.total_bytes-self.written
        expected=min(CHUNK,remaining)
        if expected<=0 or e.tile_id!=self.frames or e.token_range!=(c.token_range if c.version in (2,3) else (0,b.tiles[-1][1])) or e.tensor_shape!=(expected+OVERHEAD,):raise Denied()
        raw=e.payload
        if len(raw)!=expected+OVERHEAD or raw[:4]!=_magic(c.version) or not hmac.compare_digest(raw[4:36],tag(c)):raise Denied()
        sequence,offset=struct.unpack('!IQ',raw[36:48])
        if sequence!=self.frames or offset!=self.written:raise Denied()
        return raw[48:]

    async def release_written_frame(self):
        delivery=self.held;self.held=None
        # Clear only this receiver reference. Channel reader may retain decode
        # aliases; this is not physical memory release or an RSS guarantee.
        delivery.envelope=None
        if self.written==self.context.total_bytes:
            # Final frame needs no successor; retire its slot on close rather
            # than granting permission for an unvalidated extra frame.
            delivery._released=True
            return
        await delivery.release()


class ResultSetReceiver(ObjectReceiver):
    """One frame/one Tile in flight; next Tile credit only after file publication."""
    def __init__(self, reader, writer, config, binding, object_id, entries, set_digest):
        if not 1<=len(entries)==len(binding.tiles)<=64:raise Denied()
        sizes=tuple(4*shape[0]*shape[1]*shape[2] for _,_,shape in binding.tiles)
        if sum(sizes)>MAX_OBJECT or sum((size+CHUNK-1)//CHUNK for size in sizes)>127:raise Denied()
        super().__init__(reader,writer,config,binding,object_id,sizes[0],entries[0][2])
        actual=self.context.binding;generation=self.context.connection_generation
        self.contexts=tuple(ResultTileContext(actual,
            hashlib.sha256(b'opencompute/result-tile/v1\0'+object_id.encode('ascii')+struct.pack('!I',i)).hexdigest(),
            size,e[2],generation,set_digest,i,(e[0],e[0]+e[1]))
            for i,(size,e) in enumerate(zip(sizes,entries)))
        self.context=self.contexts[0];self._boundary_credit=None

    async def release_written_frame(self):
        if self.written!=self.context.total_bytes:
            return await super().release_written_frame()
        delivery=self.held;self.held=None;delivery.envelope=None
        self._boundary_credit=delivery

    async def published_tile(self):
        self.check()
        delivery=self._boundary_credit
        if delivery is None or self.written!=self.context.total_bytes:raise Denied()
        self._boundary_credit=None
        next_index=self.context.tile_index+1
        if next_index==len(self.contexts):
            delivery._released=True  # Final slot retired by connection close.
            return
        self.context=self.contexts[next_index];self.written=0;self.frames=0
        await delivery.release()

    def _fence(self):
        super()._fence()
        if getattr(self,'_boundary_credit',None) is not None:
            self._boundary_credit.envelope=None;self._boundary_credit=None
