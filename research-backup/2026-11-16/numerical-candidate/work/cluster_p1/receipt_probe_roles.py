"""Private fixed-byte receipt probe roles over already authenticated streams.

Local trusted manifest hashes only. No socket creation, model operation, sender
journal, remote permits, online negotiation or new wire protocol. Returned reports
are local observations, not signed attestations. Credits are NOT durable ACKs.
"""
import asyncio
import copy
import hashlib
import inspect
import re
import sqlite3
import ssl
from .channel import Channel,TLSConfig,Grant,ChannelError
from .nonce_coordinator import NonceCoordinator
from .prepared_receipt import PreparedReceiptConnection
from .handoff import send_handoff
from .store import canonical,Conflict
from .receipt_probe_manifest import validate_probe_manifest,fixture_frames,FIXTURE_LABEL

class ProbeRejected(ValueError):
    def __init__(self):super().__init__('receipt probe rejected')
class ProbeCancelled(asyncio.CancelledError):
    def __init__(self,report):
        super().__init__('receipt probe cancelled after cleanup');self.report=report
class _EmitFailed(Exception):pass

def _need(ok):
    if not ok:raise ProbeRejected()
def _sha(raw):return hashlib.sha256(raw).hexdigest()
def _same(a,b):return canonical(a)==canonical(b)
def _ignore(*args):pass

def _emit(emit,kind,data):
    try:
        result=emit(kind,copy.deepcopy(data))
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):result.close()
            raise TypeError('synchronous evidence callback required')
    except BaseException as exc:
        if isinstance(exc,asyncio.CancelledError):raise
        raise _EmitFailed() from None

def _error_name(exc):
    for cls in (asyncio.CancelledError,_EmitFailed,ProbeRejected,PermissionError,Conflict,
                ChannelError,TimeoutError,ConnectionError,OSError,ValueError,TypeError):
        if isinstance(exc,cls):return cls.__name__
    return 'Error'

class _ObservedReader:
    """Exclusive Channel read proxy; no extra reads or byte transformation.

    Channel alternates fixed five-byte header and one body read. Track the phase
    rather than infer it from EOF.expected (a D body can also be five bytes).
    """
    def __init__(self,reader):
        self.reader=reader;self.phase='header';self.error=None;self.failure_phase=None;self._reading=False
    async def readexactly(self,n):
        phase=self.phase
        if self._reading or self.error is not None:
            if self.error is None:
                self.error=ChannelError('probe reader reentry');self.failure_phase=phase
            raise ChannelError('probe reader observation retired')
        self._reading=True
        try:
            if phase=='header' and n!=5:raise ChannelError('unexpected probe parser phase')
            data=await self.reader.readexactly(n)
            if self.error is not None:raise ChannelError('probe reader observation retired')
            self.phase='body' if phase=='header' else 'header'
            return data
        except BaseException as exc:
            if self.error is None:self.error=exc;self.failure_phase=phase
            raise
        finally:self._reading=False
    def clean_eof(self):
        return (type(self.error)is asyncio.IncompleteReadError and self.failure_phase=='header'
                and self.error.expected==5 and self.error.partial==b'')

def _authenticate(writer,config,m,role):
    peer=m.receiver_node if role=='sender' else m.sender_node
    fp=m.receiver_certificate_sha256 if role=='sender' else m.sender_certificate_sha256
    _need(type(config)is TLSConfig and type(config.peers)is dict and config.peers=={peer:fp})
    obj=writer.get_extra_info('ssl_object')
    _need(obj is not None and obj.context.verify_mode==ssl.CERT_REQUIRED)
    _need(obj.version() in ('TLSv1.2','TLSv1.3') and type(obj.server_side)is bool)
    if not obj.server_side:
        _need(obj.context.check_hostname is True and obj.server_hostname==m.expected_server_name)
    cert=obj.getpeercert(binary_form=True)
    _need(type(cert)is bytes and bool(cert) and _sha(cert)==fp and not writer.is_closing())
    return {'peer_node':peer,'peer_certificate_sha256':fp,'tls_version':obj.version(),
            'server_side':obj.server_side,'hostname_checked':not obj.server_side,
            'server_name':m.expected_server_name if not obj.server_side else None}

def _grant(m,role):
    peer=m.receiver_node if role=='sender' else m.sender_node
    return {peer:{Grant(m.task.task_id,m.session.session_id,m.session.epoch,0,(0,5),m.task.model_hash,2)}}

def _plan_digest(m):
    plan={'protocol':2,'session_id':m.session.session_id,'hidden_size':24,'vocab_size':32,
          'tiles':[t.model_dump(mode='json') for t in m.tiles]}
    return _sha(canonical(['register_v2',plan]).encode())

def _snapshot(j,m,release_count):
    # Read-only historical data. Do not turn a snapshot into live peer permission;
    # every receipt write has already used the real owner/peer/context fences.
    with sqlite3.connect(j._path.as_uri()+'?mode=ro',uri=True) as db:
        frames=[{'tile':t,'frame_sha256':_sha(bytes(f)),'frame_bytes':len(f)} for t,f in db.execute('SELECT tile,frame FROM p1_received_frames ORDER BY tile')]
        receipts=[{'tile':t,'plan_digest':p,'digest':d} for p,t,d in db.execute('SELECT plan_digest,tile_id,digest FROM p1_v2_receipts ORDER BY tile_id')]
        events=[{'id_sha256':_sha(i.encode()),'payload_sha256':_sha(p.encode()),'result_sha256':_sha(r.encode())} for i,p,r in db.execute("SELECT id,payload,result FROM p1_events WHERE id LIKE 'v2:receipt:%' ORDER BY id")]
        local=db.execute('SELECT count(*) FROM p1_local_attempts').fetchone()[0]
        stage1=db.execute('SELECT count(*) FROM p1_local_stage1_attempts').fetchone()[0]
    _need(len(frames)<=3 and len(receipts)<=3 and len(events)<=3 and local==stage1==0)
    return {'frames':frames,'receipts':receipts,'events':events,'local_attempts':local,
            'stage1_attempts':stage1,'release_count':release_count}

def _expected_snapshot(m,count=3,release_count=4):
    frames=fixture_frames(m);plan=_plan_digest(m);events=[]
    for e in frames[:count]:
        digest=_sha(e.encode());event_id='v2:receipt:'+_sha(canonical([plan,e.tile_id]).encode())
        events.append({'id_sha256':_sha(event_id.encode()),
            'payload_sha256':_sha(canonical(['receipt_v2',plan,e.tile_id,digest]).encode()),
            'result_sha256':_sha(canonical({'recorded':True,'execution_allowed':False}).encode())})
    return {'frames':[{'tile':e.tile_id,'frame_sha256':_sha(e.encode()),'frame_bytes':len(e.encode())} for e in frames[:count]],
            'receipts':[{'tile':e.tile_id,'plan_digest':plan,'digest':_sha(e.encode())} for e in frames[:count]],
            'events':sorted(events,key=lambda x:x['id_sha256']),
            'local_attempts':0,'stage1_attempts':0,'release_count':release_count}

def _normalize_snapshot(snapshot):
    s=copy.deepcopy(snapshot);s['events']=sorted(s['events'],key=lambda x:x['id_sha256']);return s

async def _credit_complete(channel):
    async with channel._condition:
        while True:
            # C4 survives only a known clean header-boundary EOF, never a
            # protocol error, partial header, or incomplete body (even length 5).
            if channel._credit_seq==4:
                observed=channel.r
                _need(type(observed)is _ObservedReader)
                if channel._error is not None:
                    if (channel._closed and observed.clean_eof()
                            and channel._error is observed.error):
                        return 'clean_header_eof_after_credit'
                    raise ChannelError('probe invalid terminal frame')
                if observed.error is not None:
                    if observed.clean_eof() and not channel._closed:
                        await channel._condition.wait();continue
                    raise ChannelError('probe read failure')
                if not channel._closed:return 'open_at_credit_checkpoint'
                raise ChannelError('probe retired without clean EOF')
            if channel._credit_seq>4 or channel._closed:raise ChannelError('probe credits incomplete')
            await channel._condition.wait()

async def _cleanup(channel,journal,writer):
    ok=True
    if journal is not None:
        try:journal.close()
        except BaseException:ok=False
    try:
        if channel is not None:await channel.close()
        else:
            writer.close();await asyncio.wait_for(writer.wait_closed(),3)
    except BaseException:ok=False
    return ok

async def _settle_cleanup(channel,journal,writer):
    task=asyncio.create_task(_cleanup(channel,journal,writer));cancelled=False
    while not task.done():
        try:await asyncio.shield(task)
        except asyncio.CancelledError:cancelled=True
    return task.result(),cancelled

def _new_report(role,trusted_hash):
    digest=trusted_hash if type(trusted_hash)is str and re.fullmatch('[0-9a-f]{64}',trusted_hash) else '0'*64
    return {'schema_version':1,'role':role,'manifest_sha256':digest,'session_id':None,'nonce':None,
            'status':'failed','phase':'manifest','error_type':'none','cleanup_ok':False,
            'emission_ok':True,'execution_allowed':False,'operations':0,
            'payload_source':FIXTURE_LABEL,'tls':None,'frames_sent':[],
            'payload_bytes_sent':0,'credits_received':0,'credit_termination':None,'receipts':[],'snapshot':None}

async def _run(role,raw,trusted_hash,reader,writer,config,*,coordinator=None,path=None,emit=_ignore,prepare_fence=None):
    report=_new_report(role,trusted_hash);channel=None;journal=None;cancelled=False
    try:
        m=validate_probe_manifest(raw,trusted_manifest_sha256=trusted_hash)
        report['session_id']=m.session.session_id;report['nonce']=m.session.session_id;report['phase']='authentication'
        report['tls']=_authenticate(writer,config,m,role)
        _emit(emit,'role_started',{'role':role,'manifest_sha256':trusted_hash,'session_id':m.session.session_id})
        frames=fixture_frames(m);sequence=frames+(frames[0],);observed_reader=_ObservedReader(reader)
        if role=='sender':
            report['phase']='channel'
            channel=Channel(observed_reader,writer,config,_grant(m,role),capacity_bytes=65536,capacity_tiles=1,timeout=3)
            await channel.start();report['phase']='send'
            for index,e in enumerate(sequence):
                await send_handoff(channel,e)
                report['frames_sent'].append(_sha(e.encode()));report['payload_bytes_sent']+=len(e.payload)
                _emit(emit,'sender_delivery',{'index':index,'tile':e.tile_id,'frame_sha256':_sha(e.encode())})
            report['phase']='credits'
            report['credit_termination']=await asyncio.wait_for(_credit_complete(channel),3)
            report['credits_received']=channel._credit_seq
            _need(channel._queue.empty() and channel._used_tiles==0)
        else:
            report['phase']='channel'
            prepared=await PreparedReceiptConnection.create(observed_reader,writer,config,_grant(m,role),timeout=3)
            channel=prepared.channel;report['phase']='register'
            _need(type(coordinator)is NonceCoordinator)
            reservation=coordinator.register(m.task,m.session,m.stages,m.tiles,hidden_size=24,vocab_size=32,config_sha256=m.config_sha256)
            report['phase']='prepare'
            try:journal=await prepared.prepare(coordinator,reservation,path,prepare_fence=prepare_fence)
            finally:journal=prepared.journal
            report['phase']='receive'
            for index,e in enumerate(sequence):
                receipt=await prepared.bound.receive_once(journal)
                _need(receipt['frame_digest']==_sha(e.encode()) and receipt['duplicate']==(index==3) and receipt['execution_allowed'] is False)
                report['receipts'].append({'tile':e.tile_id,'frame_sha256':receipt['frame_digest'],'duplicate':receipt['duplicate'],'execution_allowed':False})
                snapshot=_snapshot(journal,m,channel._release_seq)
                _need(_normalize_snapshot(snapshot)==_expected_snapshot(m,min(index+1,3),index+1))
                report['snapshot']=snapshot
                _emit(emit,'receiver_delivery',{'index':index,'tile':e.tile_id,'duplicate':receipt['duplicate'],'frame_sha256':receipt['frame_digest'],'receipt_count':min(index+1,3),'release_count':channel._release_seq,'execution_allowed':False})
        report['status']='ok';report['phase']='complete'
    except BaseException as exc:
        cancelled=isinstance(exc,asyncio.CancelledError)
        report['status']='cancelled' if cancelled else 'failed';report['error_type']=_error_name(exc)
        if isinstance(exc,_EmitFailed):report['emission_ok']=False
    finally:
        if role=='sender' and channel is not None:
            report['credits_received']=channel._credit_seq
        ok,cleanup_cancelled=await _settle_cleanup(channel,journal,writer)
        report['cleanup_ok']=ok;cancelled=cancelled or cleanup_cancelled
        if not ok:
            if report['status']=='ok':report.update(status='failed',phase='cleanup',error_type='CleanupError')
        if cancelled:report['status']='cancelled'
    try:_emit(emit,'role_done',report)
    except BaseException as exc:
        report['emission_ok']=False
        if isinstance(exc,asyncio.CancelledError):cancelled=True;report['status']='cancelled'
        elif report['status']=='ok':report.update(status='failed',phase='emit',error_type='EvidenceError')
    if cancelled:raise ProbeCancelled(report) from None
    return report

async def send_probe(raw,trusted_hash,reader,writer,tls_config,*,emit=_ignore):
    return await _run('sender',raw,trusted_hash,reader,writer,tls_config,emit=emit)

async def receive_probe(raw,trusted_hash,reader,writer,tls_config,*,coordinator,fresh_journal_path,emit=_ignore,prepare_fence=None):
    return await _run('receiver',raw,trusted_hash,reader,writer,tls_config,coordinator=coordinator,path=fresh_journal_path,emit=emit,prepare_fence=prepare_fence)

def join_probe_reports(raw,trusted_hash,sender_report,receiver_report):
    """Pure local comparison. Neither success/credit alone substitutes for receipt."""
    m=validate_probe_manifest(raw,trusted_manifest_sha256=trusted_hash)
    outcome={'matched':False,'scope':'fixed ordinary-byte receipt probe','execution_allowed':False,
             'sender_ok':False,'receiver_ok':False,'reasons':[]}
    frames=fixture_frames(m);sequence=frames+(frames[0],)
    for role,report in [('sender',sender_report),('receiver',receiver_report)]:
        try:
            _need(type(report)is dict and set(report)==set(_new_report(role,trusted_hash)))
            _need(type(report['schema_version'])is int and report['schema_version']==1 and report['role']==role and report['manifest_sha256']==trusted_hash and report['session_id']==m.session.session_id and report['nonce']==m.session.session_id)
            _need(report['status']=='ok' and report['error_type']=='none' and report['cleanup_ok'] is True and report['emission_ok'] is True and report['phase']=='complete')
            _need(report['execution_allowed'] is False and type(report['operations'])is int and report['operations']==0 and report['payload_source']==FIXTURE_LABEL)
            tls=report['tls'];peer=m.receiver_node if role=='sender' else m.sender_node;fp=m.receiver_certificate_sha256 if role=='sender' else m.sender_certificate_sha256
            _need(type(tls)is dict and set(tls)=={'peer_node','peer_certificate_sha256','tls_version','server_side','hostname_checked','server_name'})
            _need(tls['peer_node']==peer and tls['peer_certificate_sha256']==fp and tls['tls_version'] in ('TLSv1.2','TLSv1.3') and type(tls['server_side'])is bool)
            _need((tls['hostname_checked'] is False and tls['server_name'] is None) if tls['server_side'] else (tls['hostname_checked'] is True and tls['server_name']==m.expected_server_name))
            if role=='sender':
                _need(report['frames_sent']==[_sha(e.encode()) for e in sequence] and report['payload_bytes_sent']==672 and report['credits_received']==4 and report['receipts']==[] and report['snapshot'] is None and report['credit_termination'] in ('open_at_credit_checkpoint','clean_header_eof_after_credit'))
            else:
                _need(report['frames_sent']==[] and report['credit_termination'] is None and type(report['payload_bytes_sent'])is int and report['payload_bytes_sent']==0 and type(report['credits_received'])is int and report['credits_received']==0)
                _need(_same(report['receipts'],[{'tile':e.tile_id,'frame_sha256':_sha(e.encode()),'duplicate':i==3,'execution_allowed':False} for i,e in enumerate(sequence)]))
                _need(_same(_normalize_snapshot(report['snapshot']),_expected_snapshot(m)))
            outcome[role+'_ok']=True
        except (ValueError,TypeError,KeyError,AttributeError):outcome['reasons'].append(role+'_report_incomplete_or_failed')
    outcome['matched']=outcome['sender_ok'] and outcome['receiver_ok']
    if outcome['matched']:
        a,b=sender_report['tls'],receiver_report['tls']
        if a['server_side']==b['server_side'] or a['tls_version']!=b['tls_version']:
            outcome['matched']=False;outcome['reasons'].append('incompatible_TLS_role_observations')
    return outcome
