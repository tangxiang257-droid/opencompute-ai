"""Private role-local byte endpoints. No startup, execution grant or remote objects.

B's two Spool Books are local logical promises, not C's artifact ledger. C reserves
its own storage before data H. No distributed atomicity, replay/resume or refunds.
Providers and trusted immutable ReceiveBinding are required, never synthesized.
"""
import asyncio
from contextlib import contextmanager, ExitStack
from dataclasses import asdict, replace
import hashlib
import json
import os
import struct
from .channel import Channel, TLSConfig
from .bound_connection import BoundReceiptConnection
from .receipt_attempt import ReceiptAttemptJournal
from .envelope import Envelope, decode
from .handoff import send_handoff
from .byte_spool import Spool
from .control_exchange import _json, _parse, _need, _nothing
from .async_role_bridge import _ssl_snapshot
from .local_attempt import _non_suppressing
from .delivery_receipt import _with_cleanup
from framed_result_transport import ObjectContext, ObjectReceiver, payload, CHUNK, OVERHEAD, MAX_OBJECT
from isolated_tls_delivery import ReceiveBinding, _binding
from result_artifacts import _Ledger


def transport_scope(spec):
    return {k:getattr(spec,k) for k in ('owner','request','session','epoch','model','layout',
        'plan_digest','input_snapshot','token_digest','preprocessing','hidden','vocab')}


def _digest(value):
    _need(type(value)is str and len(value)==64 and all(c in '0123456789abcdef' for c in value))


def _generation(writer):
    _ssl_snapshot(writer)
    return hashlib.sha256(b'opencompute/tls-incarnation/v1\0'+writer.get_extra_info('ssl_object').get_channel_binding('tls-unique')).hexdigest()


class _Local:
    def __init__(self,channels,identity,authorize):
        _need(callable(getattr(identity,'hold',None)) and callable(getattr(authorize,'hold',None)))
        self._channels=tuple(channels);self._identity=identity;self._authorize=authorize
        self._snap=tuple((c.authenticated_identity,_ssl_snapshot(c.w),c.capacity_bytes,c.capacity_tiles) for c in channels)
        self._used=False;self._closed=False;self._loop=asyncio.get_running_loop()
    @contextmanager
    def fence(self,binding,action):
        _need(not self._closed and asyncio.get_running_loop() is self._loop)
        principal=None
        with ExitStack() as stack:
            for c,(who,ssl,size,tiles) in zip(self._channels,self._snap):
                _need((c.capacity_bytes,c.capacity_tiles)==(size,tiles))
                stack.enter_context(c.identity_fence(who))
                _need(_ssl_snapshot(c.w)==ssl)
                p=stack.enter_context(_non_suppressing(self._identity.hold(who)))
                _need(type(p)is str and 0<len(p)<=128)
                if principal is None:principal=p
                else:_need(principal==p)
                permitted=stack.enter_context(_non_suppressing(self._authorize.hold(p,binding,action)))
                _nothing(permitted)
            yield
    async def _extra_cleanup(self):pass
    async def close(self):
        self._closed=True
        async def cleanup():
            errors=[]
            try:await self._extra_cleanup()
            except BaseException as error:errors.append(error)
            results=await asyncio.gather(*(c.close() for c in self._channels),return_exceptions=True)
            for c in self._channels:
                if not c._task.done():c._task.cancel()
            await asyncio.gather(*(c._task for c in self._channels),return_exceptions=True)
            errors.extend(x for x in results if isinstance(x,BaseException))
            if errors:raise BaseExceptionGroup('role transport cleanup',errors)
        task=asyncio.create_task(cleanup());cancelled=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as e:cancelled=e
            except BaseException:break
        task.result()
        if cancelled:raise cancelled
    async def failed(self,error):
        try:await self.close()
        except BaseException as cleanup:raise _with_cleanup(error,cleanup)
        raise error
    def once(self):
        _need(not self._used);self._used=True
    async def send_json(self,c,scope,value,stage):
        raw=_json(value)
        with self.fence(scope,'send-metadata'):pass
        await c.send(Envelope(scope['request'],scope['session'],scope['epoch'],stage,0,(0,1),scope['model'],2,(len(raw),),'uint8',raw),max_frame_bytes=65536)
    async def read_json(self,c,scope,stage):
        d=await c.receive()
        try:
            with self.fence(scope,'read-metadata'):
                e=d.envelope
                _need((e.task_id,e.session_id,e.execution_epoch,e.stage_id,e.tile_id,e.token_range,e.model_hash,e.layout_version,e.dtype,e.tensor_shape)==
                      (scope['request'],scope['session'],scope['epoch'],stage,0,(0,1),scope['model'],2,'uint8',(len(e.payload),)))
                value=_parse(e.payload)
            await d.release()
            with self.fence(scope,'read-metadata'):pass
            return value
        finally:d.envelope=None


class HiddenSender(_Local):
    def __init__(self,channel,*,scope,identity=None,authorize=None):
        _need(type(channel)is Channel and 0<channel.capacity_bytes<=65536 and channel.capacity_tiles==1)
        super().__init__((channel,),identity,authorize);self._channel=channel;self._scope=_parse(_json(scope))
    async def send(self,raw):
        try:
            self.once();_need(type(raw)is bytes and len(raw)<=65536)
            e=decode(raw);s=self._scope
            _need(e.encode()==raw and (e.task_id,e.session_id,e.execution_epoch,e.stage_id,e.tile_id,e.model_hash,e.tensor_shape,e.dtype)==
                  (s['request'],s['session'],s['epoch'],0,0,s['model'],(1,1,s['hidden']),'float32'))
            with self.fence(s,'hidden-send'):pass
            await send_handoff(self._channel,e)
            ack=await self.read_json(self._channel,s,0)
            expected=dict(version=1,kind='hidden-committed',scope=s,frame_digest=hashlib.sha256(raw).hexdigest(),payload_digest=hashlib.sha256(e.payload).hexdigest(),execution_allowed=False)
            _need(_json(ack)==_json(expected));return ack
        except BaseException as e:await self.failed(e)


class HiddenReceiver(_Local):
    def __init__(self,bound,journal,*,scope,identity=None,authorize=None):
        _need(type(bound)is BoundReceiptConnection and type(journal)is ReceiptAttemptJournal)
        bound._journal(journal);c=bound._channel
        _need(c._receipt_state=='ready' and c._receipt_h_started is True and c.capacity_bytes<=65536 and c.capacity_tiles==1)
        super().__init__((c,),identity,authorize);self._bound=bound;self._journal=journal;self._scope=_parse(_json(scope))
        edge=journal._receipt_edge
        _need((scope['request'],scope['session'],scope['epoch'],scope['plan_digest'],scope['layout'])==
              (edge.task_id,edge.session_id,edge.epoch,edge.plan_digest,edge.layout_hash))
    async def receive(self):
        try:
            self.once();s=self._scope
            with self.fence(s,'hidden-receive'):pass
            receipt=await self._bound.receive_once(self._journal)
            with self.fence(s,'hidden-committed'),self._journal._authorized(),self._journal._receipt_fence(),self._journal._store._tx() as c:
                _need(receipt.get('duplicate') is False and receipt.get('execution_allowed') is False)
                row=c.execute('SELECT frame FROM p1_received_frames WHERE tile=0').fetchone();_need(row is not None)
                raw=row[0];e=decode(raw)
                _need(e.tensor_shape==(1,1,s['hidden']))
                ack=dict(version=1,kind='hidden-committed',scope=s,frame_digest=hashlib.sha256(raw).hexdigest(),payload_digest=hashlib.sha256(e.payload).hexdigest(),execution_allowed=False)
            await self.send_json(self._bound._channel,s,ack,0);return ack
        except BaseException as e:await self.failed(e)


class ResultSender(_Local):
    def __init__(self,data_channel,metadata_channel,binding,*,identity=None,authorize=None):
        _need(type(data_channel)is Channel and type(metadata_channel)is Channel and data_channel is not metadata_channel)
        _need((data_channel.capacity_bytes,data_channel.capacity_tiles)==(CHUNK+OVERHEAD,1)
              and (metadata_channel.capacity_bytes,metadata_channel.capacity_tiles)==(32768,1))
        _binding(binding,MAX_OBJECT)
        a,b=data_channel.authenticated_identity,metadata_channel.authenticated_identity
        _need((a.node_id,a.certificate_sha256)==(b.node_id,b.certificate_sha256))
        super().__init__((data_channel,metadata_channel),identity,authorize)
        self._data=data_channel;self._meta=metadata_channel;self._binding=binding
    def generation(self):return _generation(self._data.w)
    async def send(self,spec,spool,digest,check):
        try:
            self.once();s=transport_scope(spec);_digest(digest)
            _need(type(spool)is Spool and callable(check))
            p=spool.plan.binding
            _need(spool.plan.counts==(1,) and spool.plan.vocab==spec.vocab and p.connection==self.generation())
            _need((p.owner,p.task,p.session_nonce,p.epoch,p.model,p.layout,p.plan_digest,p.input_snapshot,p.token_digest,p.preprocessing,p.attempt)==
                  (spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.plan_digest,spec.input_snapshot,spec.token_digest,spec.preprocessing,'single-token'))
            binding=_result_binding(self._binding,spec,self.generation())
            meta=dict(version=1,kind='result',scope=s,binding=json.loads(json.dumps(asdict(binding))),key=spool.rid,size=spec.output_bytes,digest=digest,generation=self.generation(),actual_inputs=list(spec.inputs))
            # Roundtrip removes tuples without sharing Python context objects.
            meta=_parse(_json(meta))
            with self.fence(meta,'result-send'):_nothing(check());spool.check(('SEALED',))
            await self._meta.start();await self.send_json(self._meta,s,meta,1)
            await self._data.start()
            context=ObjectContext(binding,hashlib.sha256(spool.rid.encode()).hexdigest(),spec.output_bytes,digest,self.generation())
            iterator=spool.frames()
            try:
                for frame in iterator:
                    with self.fence(meta,'result-frame'):_nothing(check());_need(frame.object_index==0)
                    wire=payload(context,frame.sequence,frame.offset,frame.block)
                    await self._data.send(Envelope(spec.request,spec.session,spec.epoch,1,frame.sequence,(0,1),spec.model,binding.layout_version,(len(wire),),'uint8',wire),max_frame_bytes=65536)
            finally:iterator.close()
            ack=await self.read_json(self._meta,s,1)
            _need(type(ack)is dict and set(ack)=={'version','kind','metadata_digest','manifest_digest','execution_allowed'} and type(ack['version'])is int and ack['version']==1
                  and ack['kind']=='result-published' and ack['metadata_digest']==hashlib.sha256(_json(meta)).hexdigest() and ack['execution_allowed'] is False)
            _digest(ack['manifest_digest']);return ack
        except BaseException as e:
            if type(spool)is Spool:
                for book in (spool.source,spool.destination):
                    try:book.isolate(spool.rid)
                    except BaseException as cleanup:e=_with_cleanup(e,cleanup)
            await self.failed(e)


def _result_binding(binding,spec,generation):
    _need(type(binding)is ReceiveBinding);_binding(binding,MAX_OBJECT);s=binding.scope
    _need((s.task,s.session,s.epoch,s.plan,s.layout,s.direction,s.port,s.attempt)==
          (spec.request,spec.session,spec.epoch,spec.plan_digest,spec.layout,'out','logits','single-token'))
    _need((binding.stage_id,binding.model_hash,binding.layout_version,binding.dtype,binding.tiles)==
          (1,spec.model,2,'float32',((0,1,(1,1,spec.vocab)),)))
    return replace(binding,scope=replace(s,incarnation=generation))


class ResultIngress(_Local):
    def __init__(self,reader,writer,config,metadata_channel,binding,*,identity=None,authorize=None):
        _need(type(config)is TLSConfig and type(metadata_channel)is Channel and (metadata_channel.capacity_bytes,metadata_channel.capacity_tiles)==(32768,1))
        _binding(binding,MAX_OBJECT)
        super().__init__((metadata_channel,),identity,authorize)
        self._reader=reader;self._writer=writer;self._config=config;self._meta=metadata_channel;self._binding=binding;self._receiver=None
        self._ssl=_ssl_snapshot(writer)
        _need(self._ssl[0]==binding.scope.peer_certificate_der_sha256
              and metadata_channel.authenticated_identity.node_id==binding.scope.peer_node
              and metadata_channel.authenticated_identity.certificate_sha256==self._ssl[0])
    async def _extra_cleanup(self):
        if self._receiver is not None:
            self._receiver._fenced=True;self._receiver._event.set();self._receiver._timer.cancel()
            if self._receiver.held is not None:self._receiver.held.envelope=None
            self._receiver.held=None
        else:
            errors=[]
            try:self._writer.close()
            except BaseException as e:errors.append(e)
            try:await self._writer.wait_closed()
            except BaseException as e:errors.append(e)
            if errors:raise BaseExceptionGroup('raw ingress cleanup',errors)
    async def receive(self,spec,artifacts,check,publication_fence):
        directory=fd=None
        try:
            self.once();s=transport_scope(spec)
            _need(type(artifacts)is _Ledger and callable(check) and callable(publication_fence))
            with self.fence(s,'result-metadata'):_nothing(check())
            await self._meta.start();meta=await self.read_json(self._meta,s,1)
            binding=_result_binding(self._binding,spec,_generation(self._writer))
            _need(type(meta)is dict and set(meta)=={'version','kind','scope','binding','key','size','digest','generation','actual_inputs'}
                  and type(meta['version'])is int and meta['version']==1 and meta['kind']=='result'
                  and _json(meta['scope'])==_json(s) and _json(meta['binding'])==_json(json.loads(json.dumps(asdict(binding))))
                  and _json(meta['actual_inputs'])==_json(list(spec.inputs))
                  and type(meta['size'])is int and meta['size']==spec.output_bytes and meta['generation']==_generation(self._writer)
                  and type(meta['key'])is str and 0<len(meta['key'])<=128)
            _digest(meta['digest']);entries=((0,1,meta['digest']),)
            def guard():
                with self.fence(meta,'result-store'):
                    _need(_ssl_snapshot(self._writer)==self._ssl);_nothing(check())
                    if self._receiver is not None:self._receiver.check()
            guard()
            with self.fence(meta,'result-reserve'):
                writer,record,raw,_,retry=artifacts._reserve(spec.owner,spec.request,meta['key'],spec.layout,entries)
            _need(not retry and record['total_bytes']==meta['size'] and len(record['tiles'])==1)
            self._receiver=ObjectReceiver(self._reader,self._writer,self._config,binding,hashlib.sha256(meta['key'].encode()).hexdigest(),meta['size'],meta['digest'])
            # Both local Channels now get the same-principal/provider fence.
            c=self._receiver._channel;self._channels+=(c,);self._snap+=((c.authenticated_identity,_ssl_snapshot(c.w),c.capacity_bytes,c.capacity_tiles),)
            guard();await c.start()
            directory=artifacts._directory_for(writer,True);entry=record['tiles'][0]
            fd=os.open('.pending-'+entry['file'],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
            digest=hashlib.sha256();r=self._receiver
            while r.written<meta['size']:
                r.held=await c.receive();guard();block=r.validate(r.held)
                _need(len(block)%4==0 and all(x[0]&0x7f800000!=0x7f800000 for x in struct.iter_unpack('<I',block)))
                view=memoryview(block)
                while view:
                    n=os.write(fd,view);_need(n>0);view=view[n:]
                digest.update(block);r.written+=len(block);r.frames+=1;block=view=None
                await r.release_written_frame()
            guard();_need(digest.hexdigest()==meta['digest'])
            os.fchmod(fd,0o400);os.fsync(fd);old,fd=fd,None;os.close(old)
            os.link('.pending-'+entry['file'],entry['file'],src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
            os.unlink('.pending-'+entry['file'],dir_fd=directory);os.fsync(directory)
            artifacts._write(directory,'manifest.json',(raw,),len(raw),hashlib.sha256(raw).hexdigest())
            old,directory=directory,None;os.close(old);artifacts._verify_files(writer,record,raw)
            with self.fence(meta,'result-commit'),_non_suppressing(publication_fence()) as allowed:
                _nothing(allowed);guard();result=artifacts._commit_published(spec.owner,spec.request,spec.layout,entries,writer,raw,_guard=guard)
            ack=dict(version=1,kind='result-published',metadata_digest=hashlib.sha256(_json(meta)).hexdigest(),manifest_digest=result,execution_allowed=False)
            await self.send_json(self._meta,s,ack,1);return result
        except BaseException as e:await self.failed(e)
        finally:
            if fd is not None:old,fd=fd,None;os.close(old)
            if directory is not None:old,directory=directory,None;os.close(old)
