"""Private same-process bootstrap with actual bounded Channel command exchange.

Both endpoints are supplied, not connected here. Only the service invokes its
once-bound operation after durable consume. None of the providers, TLS metadata,
resource claims or body implementations is synthesized by this module. Reconnect
execution/resume, remote bootstrap and globally unique admission are NOT supplied.
"""
import asyncio
from contextlib import contextmanager
from contextlib import nullcontext
from dataclasses import asdict
import hashlib
import inspect
import json

from .channel import Channel
from .envelope import Envelope
from .control_journal import ControlJournal
from .local_attempt import _non_suppressing
from .delivery_receipt import _with_cleanup
from .async_role_bridge import _ssl_snapshot

BODY_LIMIT=32768


def _need(ok):
    if not ok:raise ValueError('control exchange rejected')


def _json(value):
    nodes=0
    def check(item,depth=0):
        nonlocal nodes
        nodes+=1;_need(nodes<=1024 and depth<=8)
        if item is None or type(item)is bool:return
        if type(item)is int:_need(-(2**63)<=item<2**63);return
        if type(item)is str:_need(len(item)<=8192);return
        if type(item)is list:
            _need(len(item)<=128)
            for member in item:check(member,depth+1)
            return
        _need(type(item)is dict and len(item)<=64)
        for key,member in item.items():
            _need(type(key)is str and 0<len(key)<=128);check(member,depth+1)
    check(value)
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode()
    _need(len(raw)<=BODY_LIMIT)
    return raw


def _parse(raw):
    _need(type(raw)is bytes and 0<len(raw)<=BODY_LIMIT)
    def pairs(items):
        result={}
        for key,value in items:
            _need(key not in result);result[key]=value
        return result
    value=json.loads(raw,object_pairs_hook=pairs)
    _need(_json(value)==raw)
    return value


def result_digest(value):
    """Digest of bounded ordinary JSON result, never a Python object reference."""
    _need(value is None or type(value)is dict)
    return hashlib.sha256(_json(value)).hexdigest()


def _receipt(receipt,command,reservation,binding,charge):
    _need(type(receipt)is dict and set(receipt)=={'command_id','reservation_id','binding','charge',
          'state','output_digest','retired','execution_allowed','resource_verified'})
    _need(type(receipt['command_id'])is str and receipt['command_id']==command
          and type(receipt['reservation_id'])is str and receipt['reservation_id']==reservation
          and _json(receipt['binding'])==_json(binding)
          and type(receipt['charge'])is int and receipt['charge']==charge
          and type(receipt['retired'])is bool and receipt['execution_allowed'] is False
          and receipt['resource_verified'] is False
          and type(receipt['state'])is str and receipt['state'] in ('UNKNOWN','RESERVED','CONSUMED','COMMITTED'))
    digest=receipt['output_digest']
    if receipt['state']=='COMMITTED':
        _need(type(digest)is str and len(digest)==64 and all(c in '0123456789abcdef' for c in digest))
    else:_need(digest is None)


def _nothing(value):
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value) and inspect.getcoroutinestate(value)==inspect.CORO_CREATED:value.close()
        raise ValueError('synchronous control fence required')
    _need(value is None)


class _Endpoint:
    def __init__(self,channel):
        _need(type(channel)is Channel)
        self.channel=channel;self._loop=asyncio.get_running_loop()
        self._identity=channel.authenticated_identity;self._ssl=_ssl_snapshot(channel.w)
        self._capacity=(channel.capacity_bytes,channel.capacity_tiles)
        _need(self._capacity==(BODY_LIMIT,1))
        self._sequence=0
    def current(self):
        _need(asyncio.get_running_loop() is self._loop)
        _need((self.channel.capacity_bytes,self.channel.capacity_tiles)==self._capacity)
        with self.channel.identity_fence(self._identity):
            _need(_ssl_snapshot(self.channel.w)==self._ssl)
    def envelope(self,binding,sequence,body):
        return Envelope(binding['task'],binding['session'],binding['epoch'],binding['stage'],sequence,
                        (0,1),binding['model'],2,(len(body),),'uint8',body)
    def validate(self,envelope,binding,sequence):
        expected=self.envelope(binding,sequence,envelope.payload)
        _need(envelope==expected and len(envelope.encode())<=65536)


class ControlClient(_Endpoint):
    """Wire client; has no operation, permit or local compute fallback."""
    async def request(self,action,command,reservation,binding,charge):
        _need(not getattr(self.channel,'_descriptor_bootstrap_attempted',False) or getattr(self.channel,'_descriptor_bootstrap_ready',False))
        self.channel._control_requests_started=True
        self.current();sequence=self._sequence;self._sequence+=1
        request=dict(version=1,sequence=sequence,action=action,command=command,
                     reservation=reservation,binding=binding,charge=charge)
        raw=_json(request)
        await self.channel.send(self.envelope(binding,sequence,raw),max_frame_bytes=65536)
        delivery=await self.channel.receive()
        try:
            self.current();self.validate(delivery.envelope,binding,sequence)
            reply=_parse(delivery.envelope.payload)
            _need(type(reply)is dict and set(reply)==({'version','sequence','reply_to','receipt','input_ready'} if action=='input-ready' else {'version','sequence','reply_to','receipt'})
                  and type(reply['version'])is int and reply['version']==1
                  and type(reply['sequence'])is int and reply['sequence']==sequence
                  and reply['reply_to']==hashlib.sha256(raw).hexdigest())
            receipt=reply['receipt']
            _receipt(receipt,command,reservation,binding,charge)
            await delivery.release()
            return dict(receipt,result_digest=receipt['output_digest'],**({'input_ready':reply['input_ready']} if action=='input-ready' else {}))
        finally:delivery.envelope=None


class ControlService(_Endpoint):
    """One local stage operation; execution/providers absent until trusted bind.

    identity.hold(binding, actual_channel_identity) and authorize.hold(binding,
    action) are synchronous contexts yielding None. They cannot be network calls
    or remain held across operation/network awaits. They must implement REAL
    deployment policy; test contexts establish only ordering and default-deny.
    """
    def __init__(self,channel,journal,*,stage,node,coordinator_node,device,instance,
                 identity=None,authorize=None):
        super().__init__(channel)
        _need(type(journal)is ControlJournal and type(stage)is int and stage in (0,1))
        self.journal=journal;self.stage=stage;self.node=node;self.coordinator_node=coordinator_node
        self.device=device;self.instance=instance;self.identity=identity;self.authorize=authorize
        self._scope=None;self._declaration=None;self._operation=None;self._call=None;self._running=False
    def ready(self):
        self.current();self.journal.require_ready()
        if getattr(self,'_action_gate_pin',None) is not None:
            from private_experiment_actions import ActionGate
            _need(type(getattr(self,'_action_gate',None))is ActionGate and self._action_gate is self._action_gate_pin)
        _need(self._identity.node_id==self.coordinator_node)
        _need(self.identity is not None and callable(getattr(self.identity,'hold',None))
              and self.authorize is not None and callable(getattr(self.authorize,'hold',None)))
    def check(self,spec,declaration):
        self.ready()
        _need(type(declaration)is str and len(declaration)==64 and all(c in '0123456789abcdef' for c in declaration))
        scope=(spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.plan_digest,
               spec.input_snapshot,spec.token_digest,spec.preprocessing,spec.hidden,spec.vocab)
        if self._scope is None:self._scope=scope;self._declaration=declaration
        _need(scope==self._scope and self._declaration==declaration and len(spec.inputs)==1)
    def binding(self,spec):
        self.check(spec,self._declaration)
        return dict(owner=spec.owner,task=spec.request,session=spec.session,epoch=spec.epoch,
                    stage=self.stage,input_snapshot=spec.input_snapshot,model=spec.model,layout=spec.layout,
                    plan=spec.plan_digest,input_digest=spec.inputs[0],device=self.device,instance=self.instance,
                    peer=hashlib.sha256(_json(asdict(self._identity))).hexdigest(),
                    declaration_digest=self._declaration,token_digest=spec.token_digest,preprocessing=spec.preprocessing)
    @contextmanager
    def fence(self,binding,action):
        self.ready()
        with (self.channel.identity_fence(self._identity),
              _non_suppressing(self.identity.hold(_parse(_json(binding)),self._identity)) as identity,
              _non_suppressing(self.authorize.hold(_parse(_json(binding)),action)) as allowed):
            _nothing(identity);_nothing(allowed);yield
    def bind(self,spec,operation,command,reservation,charge):
        _need(self._operation is None and callable(operation))
        binding=self.binding(spec)
        self._operation=operation;self._call=(command,reservation,binding,charge)
    def _query(self,binding):
        with self.fence(binding,'query'):return self.journal.query(self._call[0])
    def _fail(self,binding):
        with self.fence(binding,'fail'):return self.journal.fail(*self._call[:3])
    async def serve_once(self):
        _need(not getattr(self.channel,'_descriptor_bootstrap_attempted',False) or getattr(self.channel,'_descriptor_bootstrap_ready',False))
        self.ready();_need(not self._running and self._call is not None and self._operation is not None)
        self._running=True;delivery=None;consumed=False;binding=self._call[2]
        try:
            delivery=await self.channel.receive();self.current()
            sequence=self._sequence;self._sequence+=1
            self.validate(delivery.envelope,binding,sequence)
            raw=delivery.envelope.payload;request=_parse(raw)
            _need(set(request)=={'version','sequence','action','command','reservation','binding','charge'}
                  and request['action'] in ('prepare','query','execute','input-ready'))
            action=request['action'];command,reservation,_,charge=self._call
            _need(_json(request)==_json(dict(version=1,sequence=sequence,action=action,command=command,
                                            reservation=reservation,binding=binding,charge=charge)))
            try:
                if action=='execute' and self.journal.require_receiving_gate:
                    from .c_dispatch_gate import BDispatchGate
                    gate=getattr(self,'_receiver_gate',None)
                    with self.fence(binding,action):before=self.journal.query(command)
                    if before is not None and before['state']=='RESERVED' and not before['retired']:
                        _need(type(gate)is BDispatchGate)
                        await gate.acquire(self,binding) # No identity/authorization lock across I/O.
                with self.fence(binding,action):
                    if action=='prepare':receipt=self.journal.prepare(command,reservation,binding,charge)
                    elif action in ('query','input-ready'):receipt=self.journal.query(command)
                    else:
                        receipt=self.journal.query(command);_need(receipt is not None)
                        if receipt['state']=='RESERVED' and not receipt['retired']:
                            if self.journal.require_receiving_gate:self._receiver_gate.check(self,binding)
                            receipt=self.journal.consume(command,reservation,binding);consumed=True
                _need(receipt is not None)
            except (Exception,) as error:
                # An enter/exit/commit failure cannot be "repaired" by observing
                # RESERVED or CONSUMED. Query is history; retire any known intent.
                consumed=False
                if action=='query':raise
                receipt=self._query(binding)
                if receipt is None:raise error
                receipt=self._fail(binding)
            await delivery.release();delivery.envelope=None;delivery=None
            if consumed:
                try:
                    action_gate=getattr(self,'_action_gate_pin',None)
                    if action_gate is not None:await action_gate.prepare('start')
                    with self.fence(binding,'execute-start'):
                        if self.journal.require_receiving_gate:self._receiver_gate.check(self,binding)
                        receipt=self.journal.revalidate_consumed(command,reservation,binding)
                        _receipt(receipt,command,reservation,binding,charge)
                        _need(receipt['state']=='CONSUMED' and receipt['retired'] is False)
                    if self.journal.require_receiving_gate:self._receiver_gate.check(self,binding)
                    # Final synchronous once-only check; no lock crosses body await.
                    if action_gate is not None:
                        with action_gate.fence('start'):pass
                    value=self._operation()
                    if inspect.isawaitable(value):value=await value
                    digest=result_digest(value)
                    if action_gate is not None:await action_gate.prepare('commit',digest)
                    with self.fence(binding,'complete'),(action_gate.fence('commit') if action_gate is not None else nullcontext()):
                        receipt=self.journal.complete(command,reservation,binding,digest)
                except asyncio.CancelledError as cancelled:
                    try:self._fail(binding)
                    except BaseException as cleanup:raise _with_cleanup(cancelled,cleanup)
                    raise
                except Exception:
                    receipt=self._fail(binding)
            proof={}
            if action=='input-ready':
                from .role_assembly import InstalledStageRole
                owner=getattr(self,'_receipt_owner',None)
                with self.fence(binding,'prepare'):
                    _need(type(owner)is InstalledStageRole and owner.role._service is self and self.stage==1)
                    owner._check();_need(receipt['state']=='RESERVED' and receipt['retired'] is False)
                    payload=owner.operation.received_bytes();value=hashlib.sha256(payload).hexdigest()
                    _need(value==binding['input_digest'])
                    proof={'input_ready':dict(resource_id=owner.startup.descriptor['resource_id'],input_digest=value,receipt_digest=hashlib.sha256(_json(dict(binding=binding,bytes=len(payload),digest=value))).hexdigest())}
            self.current()
            reply=_json(dict(version=1,sequence=sequence,reply_to=hashlib.sha256(raw).hexdigest(),receipt=receipt,**proof))
            await self.channel.send(self.envelope(binding,sequence,reply),max_frame_bytes=65536)
        finally:
            if delivery is not None:delivery.envelope=None
            self._running=False


class ControlPair:
    """Two stage services bootstrapped locally, never a cross-host deployment.

    dispatch only binds once, then uses prepare/execute bytes. Retried dispatch
    and query send QUERY and never call a replacement body. Cancellation retires
    these connections; intact journal history remains queryable by a separately
    authorized recovery service, NOT by reconnecting to reexecute this pair.
    """
    def __init__(self,pairs,*,charges):
        _need(type(pairs)is dict and set(pairs)=={0,1} and type(charges)is dict and set(charges)=={0,1})
        self._pairs=dict(pairs);self._charges=dict(charges);self._closed=False;self._busy=False
        self._calls={};self._assignments=None;self._declaration=None;self._tasks=set();self._failure=None
        self._minimum_charge=0
        channels=[]
        for stage,(client,service) in self._pairs.items():
            _need(type(client)is ControlClient and type(service)is ControlService and service.stage==stage)
            _need(type(charges[stage])is int and 0<charges[stage]<2**63)
            channels.extend((client.channel,service.channel))
        _need(len({id(c) for c in channels})==4);self._channels=tuple(channels)
    def buffer_bytes(self):
        _need(not self._closed)
        for client,service in self._pairs.values():client.current();service.current()
        return 4*BODY_LIMIT
    def check(self,spec,assignments,*,declaration_digest,minimum_charge=None):
        self.buffer_bytes();_need(type(assignments)is tuple and len(assignments)==2)
        if minimum_charge is not None:
            _need(type(minimum_charge)is int and minimum_charge>0)
            self._minimum_charge=max(self._minimum_charge,minimum_charge)
        _need(all(charge>=self._minimum_charge for charge in self._charges.values()))
        if self._assignments is not None:
            _need(assignments==self._assignments and declaration_digest==self._declaration)
        for stage,row in enumerate(assignments):
            client,service=self._pairs[stage]
            _need(service.stage==stage and type(row)is tuple and row==(stage,service.node,service.device,service.instance)
                  and client._identity.node_id==service.node)
            service.check(spec,declaration_digest)
        self._assignments=assignments;self._declaration=declaration_digest
    async def _exchange(self,stage,action):
        _need(not self._closed and not self._busy);self._busy=True
        client,service=self._pairs[stage];command,reservation,binding,charge=self._calls[stage]
        tasks=(asyncio.create_task(service.serve_once()),asyncio.create_task(client.request(action,command,reservation,binding,charge)))
        self._tasks.update(tasks)
        try:
            _,receipt=await asyncio.gather(*tasks);return receipt
        except BaseException as error:
            self._failure=error
            try:await self.close()
            except BaseException as cleanup:raise _with_cleanup(error,cleanup)
            raise
        finally:self._busy=False
    async def dispatch(self,stage,spec,body):
        descriptor=self.describe(stage,spec)
        binding=descriptor['binding']
        client,service=self._pairs[stage]
        if stage in self._calls:
            _need(binding==self._calls[stage][2]);return await self._exchange(stage,'query')
        command,reservation=descriptor['command_id'],descriptor['reservation_id']
        self._calls[stage]=(command,reservation,binding,self._charges[stage])
        service.bind(spec,body,command,reservation,self._charges[stage])
        receipt=await self._exchange(stage,'prepare')
        if receipt['state']!='RESERVED' or receipt['retired']:return receipt
        return await self._exchange(stage,'execute')
    def describe(self,stage,spec):
        """Stable metadata only: no bind, charge, resource allocation or grant.

        A trusted pipeline resource provider must associate its existing aggregate
        ticket with this exact descriptor BEFORE dispatch/prepare may verify it.
        """
        _need(type(stage)is int and stage in (0,1) and self._assignments is not None and not self._closed)
        self.check(spec,self._assignments,declaration_digest=self._declaration)
        client,service=self._pairs[stage];binding=service.binding(spec)
        command=hashlib.sha256(b'control-command-v1\0'+_json(binding)).hexdigest()
        reservation=hashlib.sha256(b'control-reservation-v1\0'+command.encode()).hexdigest()
        return dict(command_id=command,reservation_id=reservation,binding=binding,charge=self._charges[stage])
    async def query(self,stage):
        _need(stage in self._calls);return await self._exchange(stage,'query')
    async def _cleanup(self):
        self._closed=True;tasks=tuple(self._tasks)
        for task in tasks:
            if not task.done():task.cancel()
        results=await asyncio.gather(*tasks,return_exceptions=True)
        self._tasks.clear()
        errors=[e for e in results if isinstance(e,BaseException) and not isinstance(e,asyncio.CancelledError) and e is not self._failure]
        results=await asyncio.gather(*(c.close() for c in self._channels),return_exceptions=True)
        errors.extend(e for e in results if isinstance(e,BaseException))
        readers=[c._task for c in self._channels]
        for task in readers:
            if not task.done():task.cancel()
        await asyncio.gather(*readers,return_exceptions=True)
        if errors:raise BaseExceptionGroup('control cleanup unknown',errors)
    async def close(self):
        task=asyncio.create_task(self._cleanup());cancelled=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as exc:cancelled=exc
            except BaseException:break
        try:task.result()
        except BaseException as error:
            if cancelled is not None:raise _with_cleanup(cancelled,error)
            raise
        if cancelled is not None:raise cancelled
