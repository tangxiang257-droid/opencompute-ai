"""Opt-in receive-only TLS byte delivery, no production listeners or authority.

Single event-loop owner; local revocation is not a DB/distributed transaction.
TLS certificate DER SHA256 pins identify certificates, never GPUs or machines.
One transport delivery outstanding; <=64 KiB complete payloads, no fragmentation.
"""
import asyncio
from dataclasses import dataclass, replace, asdict
import hashlib
import hmac
import json
import time

from cluster_p1.channel import Channel, Grant, TLSConfig
from cluster_p1.envelope import DTYPES
from local_delivery_gate import DeliveryGate, LocalAuthority, Scope, Denied, _scope


@dataclass(frozen=True, slots=True)
class ReceiveBinding:
    scope: Scope
    stage_id: int
    model_hash: str
    layout_version: int
    dtype: str
    # (token_start, token_end, exact shape), in canonical Tile order.
    tiles: tuple


def _binding(value, capacity):
    if type(value) is not ReceiveBinding:raise Denied()
    _scope(value.scope)
    if type(value.stage_id) is not int or value.stage_id not in (0,1):raise Denied()
    if type(value.layout_version) is not int or value.layout_version<1:raise Denied()
    if type(value.model_hash) is not str or len(value.model_hash)!=64 or any(c not in '0123456789abcdef' for c in value.model_hash):raise Denied()
    if type(value.dtype) is not str or value.dtype not in DTYPES:raise Denied()
    if type(value.tiles) is not tuple or not 1<=len(value.tiles)<=64:raise Denied()
    end=0
    for tile in value.tiles:
        if type(tile) is not tuple or len(tile)!=3:raise Denied()
        start,stop,shape=tile
        if type(start) is not int or type(stop) is not int or start!=end or not start<stop<=131072:raise Denied()
        if type(shape) is not tuple or not 1<=len(shape)<=16:raise Denied()
        size=DTYPES[value.dtype]
        for dim in shape:
            if type(dim) is not int or not 1<=dim<=capacity//size:raise Denied()
            size*=dim
        end=stop


_PREFIX = b'BTG1'
_OVERHEAD = 36


def context_tag(binding):
    """Public deterministic context encoding, not a secret or execution proof."""
    raw=json.dumps(asdict(binding),sort_keys=True,separators=(',', ':'),ensure_ascii=True,allow_nan=False).encode('ascii')
    return hashlib.sha256(b'opencompute/tls-byte-gate/v1\0'+raw).digest()


def frame_payload(binding,payload):
    if type(payload) is not bytes:raise Denied()
    return _PREFIX+context_tag(binding)+payload


class Receipt:
    __slots__=('_owner','_local','_transport','_released')
    def __init__(self,owner,local,transport):
        self._owner,self._local,self._transport=owner,local,transport
        self._released=False
    @property
    def payload(self):
        self._owner._on_loop()
        if self._released:raise Denied()
        return self._local.payload
    async def release(self):
        owner=self._owner;owner._on_loop()
        if self._released:raise Denied()
        # Never await after consumer relinquishment without permanently recording it.
        self._local.release();self._released=True
        delivery=self._transport;self._transport=None
        try:
            if not owner._fenced:await delivery.release()
        except BaseException:
            owner._fence()
            raise
        finally:
            owner._claimed.discard(self)


class Receiver:
    def __init__(self, channel, authority, binding, deadline, capacity):
        self._loop=asyncio.get_running_loop()
        self._channel=channel
        self._authority=authority
        cert=channel.w.get_extra_info('ssl_object').getpeercert(binary_form=True)
        fingerprint=hashlib.sha256(cert).hexdigest()
        if channel.peer_node!=binding.scope.peer_node or fingerprint!=binding.scope.peer_certificate_der_sha256:raise Denied()
        # Fresh local incarnation, never accepted from remote envelope/reconnect.
        unique=channel.w.get_extra_info('ssl_object').get_channel_binding('tls-unique')
        if type(unique) is not bytes or not unique:raise Denied()
        scope=replace(binding.scope,incarnation=hashlib.sha256(b'opencompute/tls-incarnation/v1\0'+unique).hexdigest())
        self._binding=replace(binding,scope=scope)
        self._gate=DeliveryGate(authority=authority,capacity=capacity,max_tiles=len(binding.tiles))
        self._gate.authorize(authority,scope,deadline=deadline,tiles=tuple((t[0],t[1]) for t in binding.tiles))
        self._event=asyncio.Event();self._fenced=False
        self._queued={};self._claimed=set();self._seen=set()
        self._next_received=0;self._next_claim=0
        self._timer=self._loop.call_later(max(0,deadline-time.monotonic()),self._fence)
        self._pump_task=self._loop.create_task(self._pump())

    @property
    def scope(self):return self._binding.scope

    def _on_loop(self):
        if asyncio.get_running_loop() is not self._loop:raise Denied()

    def _fence(self):
        # Synchronous prefix: local delivery invalidation and wake never await.
        if self._fenced:return
        self._fenced=True
        self._gate.close();self._event.set();self._queued.clear()
        self._timer.cancel()
        self._channel._abort()  # Closes wire; outstanding credits never reused.
        if self._pump_task is not asyncio.current_task():
            self._pump_task.cancel()

    async def revoke(self,authority):
        self._on_loop()
        if authority is not self._authority:raise Denied()
        self._fence()
        await self.close()

    async def close(self):
        self._on_loop();self._fence()
        await asyncio.gather(self._pump_task,return_exceptions=True)
        await self._channel.close()

    def _validate_envelope(self,e):
        b=self._binding;s=b.scope
        if (e.task_id,e.session_id,e.execution_epoch,e.stage_id,e.model_hash,e.layout_version,e.dtype)!=(s.task,s.session,s.epoch,b.stage_id,b.model_hash,b.layout_version,'uint8'):raise Denied()
        if type(e.tile_id) is not int or not 0<=e.tile_id<len(b.tiles):raise Denied()
        start,end,shape=b.tiles[e.tile_id]
        size=DTYPES[b.dtype]
        for dim in shape:size*=dim
        if e.token_range!=(start,end) or e.tensor_shape!=(size+_OVERHEAD,):raise Denied()
        if not e.payload.startswith(_PREFIX) or not hmac.compare_digest(e.payload[4:36],context_tag(b)):raise Denied()
        if e.tile_id in self._seen or e.tile_id!=self._next_received:raise Denied()
        return e.payload[_OVERHEAD:]

    async def _pump(self):
        try:
            while not self._fenced:
                delivery=await self._channel.receive()
                if self._fenced:raise Denied()
                e=delivery.envelope;payload=self._validate_envelope(e)
                # No blocking waits in loop. Transport has only one outstanding slot;
                # identical local capacity avoids acquiring credit while waiting here.
                queued=dict(self._queued);queued[e.tile_id]=delivery
                seen=self._seen|{e.tile_id}
                self._gate.publish(self.scope,e.tile_id,e.token_range,payload,timeout=0)
                self._queued,self._seen=queued,seen
                self._next_received+=1
                self._event.set()
        except BaseException:
            self._fence()

    def _claim_nowait(self,tile):
        """Synchronous transfer for a containing single-loop authority lock."""
        self._on_loop()
        if type(tile) is not int or not 0<=tile<len(self._binding.tiles):raise Denied()
        if tile!=self._next_claim or self._claimed or self._fenced:raise Denied()
        if tile not in self._queued:raise Denied()
        receipt=Receipt(self,None,self._queued[tile])
        claimed=self._claimed|{receipt}
        local=self._gate.claim(self.scope,tile,timeout=0)
        receipt._local=local
        del self._queued[tile];self._claimed=claimed
        self._next_claim+=1
        return receipt

    async def claim(self,tile):
        self._on_loop()
        if type(tile) is not int or not 0<=tile<len(self._binding.tiles):raise Denied()
        while True:
            if tile!=self._next_claim or self._claimed:raise Denied()
            self._event.clear()
            if self._fenced:raise Denied()
            if tile in self._queued:return self._claim_nowait(tile)
            if tile in self._seen:raise Denied()
            await self._event.wait()


async def accept_receiver(reader,writer,config,*,authority=None,binding=None,deadline=None,capacity=65536):
    """Explicit opt-in only; caller owns trusted config/binding and TLS listener.

    This receiver never creates a listener or initiates outbound connections.
    Binding is local connection policy, not peer message claims or execution proof.
    """
    channel=None;receiver=None
    try:
        if type(authority) is not LocalAuthority or type(config) is not TLSConfig:raise Denied()
        if type(capacity) is not int or not 1<=capacity<=65536:raise Denied()
        _binding(binding,capacity)
        # The channel verifies actual TLS mode, hostname policy and DER pin mapping.
        s=binding.scope
        grant=Grant(s.task,s.session,s.epoch,binding.stage_id,(0,binding.tiles[-1][1]),binding.model_hash,binding.layout_version)
        channel=Channel(reader,writer,config,{s.peer_node:{grant}},capacity_bytes=capacity+_OVERHEAD,capacity_tiles=1)
        receiver=Receiver(channel,authority,binding,deadline,capacity)
        await channel.start()
        return receiver
    except BaseException:
        if receiver is not None:await receiver.close()
        elif channel is not None:await channel.close()
        else:writer.close()
        raise
