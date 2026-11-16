"""Private same-loop three-role byte bridge; no connection or execution startup.

Callers supply both ends of already-established transports. Real remote context
negotiation, authority and device stopping are not provided here. Persistent
destination quota is reserved before advertising result credit. No retry/resume,
quota refund, blocking-thread adaptation or additional payload copy file.
"""
import asyncio
from contextlib import ExitStack
from dataclasses import asdict
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
from .projection_driver import nothing
from .local_attempt import _non_suppressing
from .delivery_receipt import _with_cleanup
from framed_result_transport import ObjectReceiver, payload, CHUNK, OVERHEAD, MAX_OBJECT
from isolated_tls_delivery import ReceiveBinding, _binding
from result_artifacts import _Ledger


def _need(value):
    if not value:raise ValueError('async role bridge rejected')


def _ssl_snapshot(writer):
    _need(not writer.is_closing())
    tls=writer.get_extra_info('ssl_object');_need(tls is not None)
    cert=tls.getpeercert(binary_form=True);unique=tls.get_channel_binding('tls-unique')
    _need(type(cert)is bytes and bool(cert) and type(unique)is bytes and bool(unique))
    return (hashlib.sha256(cert).hexdigest(),hashlib.sha256(unique).hexdigest(),
            tls.version(),tls.context.verify_mode,tls.context.check_hostname,tls.server_side)


class AsyncRoleBridge:
    def __init__(self,*,hidden_sender,hidden_receiver,receipt_journal,result_sender,
                 result_reader,result_writer,result_config,result_binding):
        _need(type(hidden_sender)is Channel and type(hidden_receiver)is BoundReceiptConnection
              and type(receipt_journal)is ReceiptAttemptJournal and type(result_sender)is Channel
              and type(result_config)is TLSConfig and type(result_binding)is ReceiveBinding)
        hidden_receiver._journal(receipt_journal)
        _binding(result_binding,MAX_OBJECT)
        self._loop=asyncio.get_running_loop()
        self._hidden_sender=hidden_sender;self._bound=hidden_receiver;self._journal=receipt_journal
        self._result_sender=result_sender;self._reader=result_reader;self._writer=result_writer
        self._config=result_config;self._binding=result_binding
        self._channels=(hidden_sender,hidden_receiver._channel,result_sender)
        _need(len({id(c) for c in self._channels})==3)
        self._capacities=tuple((c.capacity_bytes,c.capacity_tiles) for c in self._channels)
        self._check_capacities()
        self._identities=tuple(c.authenticated_identity for c in self._channels)
        self._writers=tuple(c.w for c in self._channels)+(result_writer,)
        self._ssl=tuple(_ssl_snapshot(w) for w in self._writers)
        _need(self._ssl[-1][0]==result_binding.scope.peer_certificate_der_sha256)
        self._receiver=None;self._tasks=set();self._closed=False;self._failure=None
        self._hidden_started=False;self._hidden_done=False;self._published=False;self._busy=False
        self._spec=None
        # Ingress identity and egress identity are distinct. This composite is
        # bookkeeping only, NOT ObjectReceiver's tls-unique generation/tag.
        raw=json.dumps({'identities':[asdict(i) for i in self._identities],
                        'ssl':self._ssl,'binding':asdict(result_binding)},sort_keys=True,separators=(',',':'))
        self._generation=hashlib.sha256(raw.encode()).hexdigest()

    @property
    def receipt_journal(self):return self._journal

    def _check_capacities(self):
        current=tuple((c.capacity_bytes,c.capacity_tiles) for c in self._channels)
        _need(current==self._capacities)
        for (size,tiles),limit in zip(current,(65536,65536,CHUNK+OVERHEAD)):
            _need(type(size)is int and 0<size<=limit and type(tiles)is int and tiles==1)
        incoming=self._bound._channel
        _need(incoming._receipt_state=='ready' and incoming._receipt_h_started is True)

    def validate_journal(self,journal):
        _need(journal is self._journal);self._bound._journal(journal)

    def require_current(self,spec):
        _need(asyncio.get_running_loop() is self._loop and not self._closed)
        self._check_capacities()
        edge=self._journal._receipt_edge;b=self._binding;s=b.scope
        _need((spec.request,spec.session,spec.epoch,spec.plan_digest,spec.model,spec.layout)==
              (edge.task_id,edge.session_id,edge.epoch,edge.plan_digest,b.model_hash,edge.layout_hash))
        _need((s.task,s.session,s.epoch,s.plan,s.layout,s.direction,s.port,s.attempt,s.peer_node)==
              (spec.request,spec.session,spec.epoch,spec.plan_digest,spec.layout,'out','logits','single-token',edge.receiver_node))
        _need((b.stage_id,b.dtype,b.layout_version,b.tiles)==
              (1,'float32',edge.layout_version,((0,1,(1,1,spec.vocab)),)))
        _need(len(spec.inputs)==1 and self._identities[0].node_id==edge.receiver_node
              and self._identities[1].node_id==edge.sender_node)
        with ExitStack() as stack:
            for channel,identity in zip(self._channels,self._identities):stack.enter_context(channel.identity_fence(identity))
            _need(tuple(_ssl_snapshot(w) for w in self._writers)==self._ssl)
        self._spec=spec

    def reservation_generation(self,spec):
        self.require_current(spec);return self._generation

    def transport_buffer_bytes(self,spec):
        """Four endpoint logical payload slots, NOT RSS/device peak memory.

        The first three capacities are immutable snapshots; the result receiver
        has the fixed CHUNK+OVERHEAD single slot configured by ObjectReceiver.
        Encoding headers, aliases, file I/O and allocator costs remain additional.
        """
        self.require_current(spec)
        return sum(size for size,_ in self._capacities)+CHUNK+OVERHEAD

    def conduit_binding(self,spec):
        self.require_current(spec);return self._binding

    def _spawn(self,coro):
        task=asyncio.create_task(coro);self._tasks.add(task);return task

    async def _cleanup(self):
        self._closed=True
        tasks=tuple(self._tasks)
        for task in tasks:
            if not task.done():task.cancel()
        results=await asyncio.gather(*tasks,return_exceptions=True) if tasks else []
        errors=[e for e in results if isinstance(e,BaseException)
                and not isinstance(e,asyncio.CancelledError) and e is not self._failure]
        self._tasks.clear()
        closers=[c.close() for c in self._channels]
        if self._receiver is not None:closers.append(self._receiver.close())
        else:
            try:self._writer.close()
            except BaseException as exc:errors.append(exc)
            try:closers.append(asyncio.wait_for(self._writer.wait_closed(),self._result_sender.timeout))
            except BaseException as exc:errors.append(exc)
        results=await asyncio.gather(*closers,return_exceptions=True)
        errors.extend(e for e in results if isinstance(e,BaseException))
        # A writer.close exception must not strand Channel's reader task before
        # its own close method reaches cancellation/join.
        readers=[c._task for c in self._channels]
        if self._receiver is not None:readers.append(self._receiver._channel._task)
        for task in readers:
            if not task.done():task.cancel()
        await asyncio.gather(*readers,return_exceptions=True)
        if self._receiver is not None and self._receiver.held is not None:
            self._receiver.held.envelope=None;self._receiver.held=None
        if errors:raise BaseExceptionGroup('transport cleanup failed',errors)

    async def close(self):
        # Strong reference; repeated cancellation cannot abandon owned I/O.
        task=asyncio.create_task(self._cleanup());cancelled=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as exc:cancelled=exc
            except BaseException:break
        try:task.result()
        except BaseException as exc:
            if cancelled is not None:raise _with_cleanup(cancelled,exc)
            raise
        if cancelled is not None:raise cancelled

    async def _failed(self,error):
        self._failure=error
        try:await self.close()
        except BaseException as cleanup:raise _with_cleanup(error,cleanup)
        raise error

    async def exchange_hidden(self,raw):
        try:
            _need(self._spec is not None and not self._hidden_started and not self._busy)
            self._hidden_started=True;self._busy=True;self.require_current(self._spec)
            _need(type(raw)is bytes and len(raw)<=65536)
            envelope=decode(raw);_need(envelope.encode()==raw and envelope.stage_id==0 and envelope.tile_id==0
                                     and envelope.tensor_shape==(1,1,self._spec.hidden))
            send=self._spawn(send_handoff(self._hidden_sender,envelope))
            receive=self._spawn(self._bound.receive_once(self._journal))
            _,receipt=await asyncio.gather(send,receive)
            self.require_current(self._spec)
            _need(type(receipt)is dict and receipt.get('duplicate') is False and receipt.get('execution_allowed') is False)
            with self._journal._authorized(),self._journal._receipt_fence(),self._journal._store._tx() as c:
                _need(c.execute('SELECT frame FROM p1_received_frames WHERE tile=0').fetchone()==(raw,))
            self._hidden_done=True;return raw
        except BaseException as exc:await self._failed(exc)
        finally:self._busy=False

    async def _send_result(self,spec,spool,context,check):
        iterator=spool.frames()
        try:
            for frame in iterator:
                self.require_current(spec);nothing(check());_need(frame.object_index==0)
                wire=payload(context,frame.sequence,frame.offset,frame.block)
                envelope=Envelope(spec.request,spec.session,spec.epoch,1,frame.sequence,(0,1),spec.model,
                    self._binding.layout_version,(len(wire),),'uint8',wire)
                await self._result_sender.send(envelope,max_frame_bytes=65536)
        finally:iterator.close()

    async def _receive_file(self,spec,directory,entry,check):
        receiver=self._receiver;fd=None;digest=hashlib.sha256()
        try:
            fd=os.open('.pending-'+entry['file'],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
            while receiver.written<receiver.context.total_bytes:
                receiver.held=await receiver._channel.receive()
                self.require_current(spec);nothing(check());receiver.check()
                block=receiver.validate(receiver.held)
                _need(0<len(block)<=CHUNK and len(block)%4==0
                      and all((x[0]&0x7f800000)!=0x7f800000 for x in struct.iter_unpack('<I',block)))
                view=memoryview(block)
                while view:
                    count=os.write(fd,view);_need(count>0);view=view[count:]
                digest.update(block);receiver.written+=len(block);receiver.frames+=1
                block=view=None
                await receiver.release_written_frame()
            _need(digest.hexdigest()==entry['sha256'])
            os.fchmod(fd,0o400);os.fsync(fd);old,fd=fd,None;os.close(old)
            os.link('.pending-'+entry['file'],entry['file'],src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
            os.unlink('.pending-'+entry['file'],dir_fd=directory);os.fsync(directory)
        finally:
            if fd is not None:
                old,fd=fd,None;os.close(old)

    async def publish_result(self,spec,spool,artifacts,result_key,digest,check,publication_fence):
        directory=None
        try:
            _need(not self._published and not self._busy and self._hidden_done)
            self._published=True;self._busy=True;self.require_current(spec)
            _need(type(spool)is Spool and type(artifacts)is _Ledger and callable(check) and callable(publication_fence))
            _need(spool.rid==result_key and spool.plan.binding.connection==self._generation
                  and spool.plan.counts==(1,) and spool.plan.vocab==spec.vocab)
            p=spool.plan.binding
            _need((p.owner,p.task,p.session_nonce,p.epoch,p.model,p.layout,p.plan_digest,
                   p.input_snapshot,p.token_digest,p.preprocessing,p.attempt)==
                  (spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.plan_digest,
                   spec.input_snapshot,spec.token_digest,spec.preprocessing,'single-token'))
            nothing(check());spool.check(('SEALED',))
            entries=((0,1,digest),)
            writer,record,raw,_,retry=artifacts._reserve(spec.owner,spec.request,result_key,spec.layout,entries)
            _need(not retry and len(record['tiles'])==1 and record['total_bytes']==spec.output_bytes)
            # Reservation precedes even the result receiver's construction and H.
            self._receiver=ObjectReceiver(self._reader,self._writer,self._config,self._binding,
                hashlib.sha256(result_key.encode()).hexdigest(),spec.output_bytes,digest)
            self.require_current(spec)
            await asyncio.gather(self._spawn(self._result_sender.start()),self._spawn(self._receiver._channel.start()))
            directory=artifacts._directory_for(writer,True)
            send=self._spawn(self._send_result(spec,spool,self._receiver.context,check))
            receive=self._spawn(self._receive_file(spec,directory,record['tiles'][0],check))
            await asyncio.gather(send,receive)
            self.require_current(spec);nothing(check());self._receiver.check()
            artifacts._write(directory,'manifest.json',(raw,),len(raw),hashlib.sha256(raw).hexdigest())
            old,directory=directory,None;os.close(old)
            artifacts._verify_files(writer,record,raw)
            def guard():
                self.require_current(spec);self._receiver.check();nothing(check())
            # No network await, transport lock or device call under this fence.
            with _non_suppressing(publication_fence()) as allowed:
                nothing(allowed);guard()
                result=artifacts._commit_published(spec.owner,spec.request,spec.layout,entries,writer,raw,_guard=guard)
            await self.close()
            return result
        except BaseException as exc:
            if type(spool)is Spool:
                for book in (spool.source,spool.destination):
                    try:book.isolate(spool.rid)
                    except BaseException as isolation:exc=_with_cleanup(exc,isolation)
            await self._failed(exc)
        finally:
            self._busy=False
            if directory is not None:
                old,directory=directory,None;os.close(old)
