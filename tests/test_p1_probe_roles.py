"""Ordinary bytes/SQLite only. Paired in-memory streams and fake SSL metadata."""
import asyncio,copy,hashlib,json,sqlite3,ssl,struct,sys,tempfile,unittest
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from cluster_p1.channel import TLSConfig
from cluster_p1.nonce_coordinator import NonceCoordinator
from cluster_p1.store import Conflict
try:
 from cluster_p1.receipt_probe_roles import send_probe,receive_probe,join_probe_reports,ProbeCancelled
 from test_p1_probe_manifest import probe_fixture
except ImportError:send_probe=None

def guard(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):raise AssertionError('offline only: '+event)
sys.addaudithook(guard)
@contextmanager
def owner():yield 'owner'
def raw_of(d):return json.dumps(d,sort_keys=True,separators=(',',':')).encode()
class Reader:
 def __init__(self):self.stream=asyncio.StreamReader();self.reads=[]
 def feed(self,data):self.stream.feed_data(data)
 def eof(self):self.stream.feed_eof()
 async def readexactly(self,n):self.reads.append(n);return await self.stream.readexactly(n)
class Writer:
 def __init__(self,peer,cert,server):
  self.peer=peer;self.cert=cert;self.server_side=server;self.server_hostname=None if server else 'receiver.test'
  self.context=SimpleNamespace(verify_mode=ssl.CERT_REQUIRED,check_hostname=not server)
  self.frames=[];self.closed=False;self.closed_event=asyncio.Event()
 def get_extra_info(self,name):return self if name=='ssl_object' else None
 def getpeercert(self,binary_form=False):return self.cert
 def version(self):return 'TLSv1.2'
 def is_closing(self):return self.closed
 def close(self):
  if not self.closed:self.closed=True;self.peer.eof();self.closed_event.set()
 async def wait_closed(self):pass
 def write(self,raw):
  if self.closed:raise ConnectionResetError('fake writer closed')
  self.frames.append(raw);self.peer.feed(raw)
 async def drain(self):await asyncio.sleep(0)
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(send_probe,'probe roles missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.co=NonceCoordinator.initialize(self.root/'registry',authorize=owner);self.addCleanup(self.co.close)
  self.manifest=probe_fixture();self.raw=raw_of(self.manifest);self.trusted=hashlib.sha256(self.raw).hexdigest()
  self.tasks=[];self.writers=[];self.send_cfg=TLSConfig('unused','unused','unused',{'b':hashlib.sha256(b'cert-b').hexdigest()});self.recv_cfg=TLSConfig('unused','unused','unused',{'a':hashlib.sha256(b'cert-a').hexdigest()})
 async def asyncTearDown(self):
  for w in self.writers:w.close()
  for t in self.tasks:
   if not t.done():t.cancel()
  await asyncio.gather(*self.tasks,return_exceptions=True)
 def streams(self):
  sr,rr=Reader(),Reader();sw=Writer(rr,b'cert-b',False);rw=Writer(sr,b'cert-a',True);self.writers.extend([sw,rw]);return sr,sw,rr,rw
 def launch_pair(self,*,sender_emit=lambda *a:None,receiver_emit=lambda *a:None,barrier=None,journal_name='receiver'):
  sr,sw,rr,rw=self.streams()
  a=asyncio.create_task(send_probe(self.raw,self.trusted,sr,sw,self.send_cfg,emit=sender_emit))
  b=asyncio.create_task(receive_probe(self.raw,self.trusted,rr,rw,self.recv_cfg,coordinator=self.co,fresh_journal_path=self.root/journal_name,emit=receiver_emit,prepare_fence=barrier))
  self.tasks.extend([a,b]);return a,b,sr,sw,rr,rw
 async def test_complete_pair_real_receipts_join(self):
  a,b,*_=self.launch_pair();s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual((s['status'],r['status']),('ok','ok'));j=join_probe_reports(self.raw,self.trusted,s,r);self.assertTrue(j['matched'])
  self.assertEqual(s['credits_received'],4);self.assertEqual(len(r['snapshot']['receipts']),3);self.assertEqual(r['snapshot']['local_attempts'],0)
  self.assertEqual(self.co.query(self.manifest['session']['session_id'])['claimed_stages'],(1,))
 async def test_sender_waits_for_prepared_H(self):
  entered,release=asyncio.Event(),asyncio.Event()
  async def barrier():entered.set();await release.wait()
  a,b,sr,sw,rr,rw=self.launch_pair(barrier=barrier);await entered.wait();await asyncio.sleep(0)
  self.assertEqual(rw.frames,[]);self.assertNotIn(b'D',[f[:1] for f in sw.frames]);self.assertFalse(a.done())
  release.set();s,r=await asyncio.wait_for(asyncio.gather(a,b),3);self.assertTrue(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_invalid_manifest_hash_before_H_DB(self):
  sr,sw,rr,rw=self.streams()
  r=await receive_probe(self.raw,'0'*64,rr,rw,self.recv_cfg,coordinator=self.co,fresh_journal_path=self.root/'receiver')
  self.assertEqual(r['status'],'failed');self.assertEqual(rw.frames,[]);self.assertFalse((self.root/'receiver').exists());self.assertFalse(self.co.query(self.manifest['session']['session_id'])['exists'])
 async def test_actual_hostname_not_config_name(self):
  sr,sw,_,_=self.streams();sw.server_hostname='other.test'
  s=await send_probe(self.raw,self.trusted,sr,sw,self.send_cfg)
  self.assertEqual(s['status'],'failed');self.assertEqual(sw.frames,[]);self.assertTrue(sw.closed)
 async def test_actual_peer_mismatch_before_DB(self):
  _,_,rr,rw=self.streams();rw.cert=b'wrong-cert'
  r=await receive_probe(self.raw,self.trusted,rr,rw,self.recv_cfg,coordinator=self.co,fresh_journal_path=self.root/'receiver')
  self.assertEqual(r['status'],'failed');self.assertEqual(rw.frames,[]);self.assertFalse((self.root/'receiver').exists())
 async def test_tls_config_grant_map_mismatch_before_DB(self):
  _,_,rr,rw=self.streams();bad=TLSConfig('unused','unused','unused',{'wrong':self.recv_cfg.peers['a']})
  r=await receive_probe(self.raw,self.trusted,rr,rw,bad,coordinator=self.co,fresh_journal_path=self.root/'receiver')
  self.assertEqual(r['status'],'failed');self.assertFalse(self.co.query(self.manifest['session']['session_id'])['exists']);self.assertEqual(rw.frames,[])
 async def test_task_change_between_receipts_rejects(self):
  def mutate(kind,data):
   if kind=='receiver_delivery' and data['index']==0:
    with sqlite3.connect(self.root/'receiver') as db:
     body=json.loads(db.execute('SELECT body FROM p1_tasks').fetchone()[0]);body['cost_cap']=1.0;db.execute('UPDATE p1_tasks SET body=?',(raw_of(body).decode(),))
  a,b,*_=self.launch_pair(receiver_emit=mutate);s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(r['status'],'failed');self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
  with sqlite3.connect(self.root/'receiver') as db:self.assertEqual(db.execute('SELECT count(*) FROM p1_v2_receipts').fetchone()[0],1)
 async def test_same_nonce_second_stream_path_no_rebind(self):
  a,b,*_=self.launch_pair();await asyncio.wait_for(asyncio.gather(a,b),3)
  _,_,rr,rw=self.streams();r=await receive_probe(self.raw,self.trusted,rr,rw,self.recv_cfg,coordinator=self.co,fresh_journal_path=self.root/'receiver2')
  self.assertEqual(r['status'],'failed');self.assertEqual(rw.frames,[]);self.assertFalse((self.root/'receiver2').exists())
 async def test_emit_failure_preserves_both_reports_no_retry(self):
  def bad(kind,data):
   if kind=='receiver_delivery':raise RuntimeError('secret arbitrary text must not escape')
  a,b,*_=self.launch_pair(receiver_emit=bad);s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(r['status'],'failed');self.assertFalse(r['emission_ok']);self.assertNotIn('secret',repr(r));self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
  with sqlite3.connect(self.root/'receiver') as db:self.assertEqual(db.execute('SELECT count(*) FROM p1_v2_receipts').fetchone()[0],1)
 async def test_receiver_cancel_reports_then_cleanup(self):
  entered=asyncio.Event()
  async def barrier():entered.set();await asyncio.Future()
  a,b,*_=self.launch_pair(barrier=barrier);await entered.wait();b.cancel()
  with self.assertRaises(ProbeCancelled) as caught:await b
  s=await asyncio.wait_for(a,2);r=caught.exception.report
  self.assertEqual(r['status'],'cancelled');self.assertTrue(r['cleanup_ok']);self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_missing_report_never_passes(self):
  a,b,*_=self.launch_pair();s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertFalse(join_probe_reports(self.raw,self.trusted,s,None)['matched']);self.assertFalse(join_probe_reports(self.raw,self.trusted,None,r)['matched'])
 async def test_D_before_prepared_body_not_read(self):
  from cluster_p1.receipt_probe_manifest import fixture_frames,validate_probe_manifest
  entered,release=asyncio.Event(),asyncio.Event()
  async def barrier():entered.set();await release.wait()
  sr,sw,rr,rw=self.streams();b=asyncio.create_task(receive_probe(self.raw,self.trusted,rr,rw,self.recv_cfg,coordinator=self.co,fresh_journal_path=self.root/'receiver',prepare_fence=barrier));self.tasks.append(b)
  await entered.wait();m=validate_probe_manifest(self.raw,trusted_manifest_sha256=self.trusted);wire=fixture_frames(m)[0].encode();rr.feed(b'D'+struct.pack('!I',len(wire)))
  await rw.closed_event.wait();release.set();r=await b
  self.assertEqual(r['status'],'failed');self.assertEqual(rr.reads,[5]);self.assertEqual(rw.frames,[])
 async def test_final_credit_observed_with_peer_EOF_is_success(self):
  import cluster_p1.receipt_probe_roles as roles
  original=roles._credit_complete;observed=[]
  async def after_EOF(channel):
   async with channel._condition:
    while not channel._closed:await channel._condition.wait()
   observed.append((channel._credit_seq,channel._closed))
   return await original(channel)
  with patch.object(roles,'_credit_complete',after_EOF):
   a,b,*_=self.launch_pair();s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(observed,[(4,True)]);self.assertEqual(s['status'],'ok')
  self.assertTrue(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_final_emit_failure_does_not_hide_other_success(self):
  def fail(kind,data):
   if kind=='role_done':raise RuntimeError('private error')
  a,b,*_=self.launch_pair(receiver_emit=fail);s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(s['status'],'ok');self.assertEqual(r['status'],'failed');self.assertTrue(r['cleanup_ok'])
  result=join_probe_reports(self.raw,self.trusted,s,r)
  self.assertTrue(result['sender_ok']);self.assertFalse(result['receiver_ok']);self.assertFalse(result['matched'])
 async def test_safe_report_tampering_not_accepted(self):
  a,b,*_=self.launch_pair();s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  for key,value in [('manifest_sha256','0'*64),('session_id','2'*32),('nonce','2'*32),('snapshot',None),('receipts',[])]:
   bad=copy.deepcopy(r);bad[key]=value;self.assertFalse(join_probe_reports(self.raw,self.trusted,s,bad)['matched'])
  bad=copy.deepcopy(r);bad['snapshot']['local_attempts']=False
  self.assertFalse(join_probe_reports(self.raw,self.trusted,s,bad)['matched'])
 async def test_sender_cancel_preserves_receipt_outcome_and_closes(self):
  entered,release=asyncio.Event(),asyncio.Event()
  async def barrier():entered.set();await release.wait()
  a,b,*_=self.launch_pair(barrier=barrier);await entered.wait();a.cancel()
  with self.assertRaises(ProbeCancelled) as caught:await a
  release.set();r=await asyncio.wait_for(b,2)
  self.assertEqual(caught.exception.report['status'],'cancelled');self.assertTrue(caught.exception.report['cleanup_ok'])
  self.assertFalse(join_probe_reports(self.raw,self.trusted,caught.exception.report,r)['matched'])
 async def test_join_rejects_noncomplementary_TLS_roles(self):
  a,b,*_=self.launch_pair();s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  r=copy.deepcopy(r);r['tls'].update(server_side=False,hostname_checked=True,server_name=self.manifest['expected_server_name'])
  self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_join_rejects_different_TLS_versions(self):
  a,b,*_=self.launch_pair();s,r=await asyncio.wait_for(asyncio.gather(a,b),3);r=copy.deepcopy(r);r['tls']['tls_version']='TLSv1.3'
  self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_EOF_before_fourth_credit_fails_sender(self):
  def stop(kind,data):
   if kind=='receiver_delivery' and data['index']==2:raise RuntimeError('stop before duplicate')
  a,b,*_=self.launch_pair(receiver_emit=stop);s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual((s['status'],r['status']),('failed','failed'))
  self.assertEqual(r['snapshot']['release_count'],3);self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_prepare_cancel_burns_slot_no_second_path(self):
  entered=asyncio.Event()
  async def barrier():entered.set();await asyncio.Future()
  a,b,*_=self.launch_pair(barrier=barrier);await entered.wait();b.cancel()
  with self.assertRaises(ProbeCancelled):await b
  await asyncio.wait_for(a,2)
  self.assertEqual(self.co.query(self.manifest['session']['session_id'])['claimed_stages'],(1,))
  _,_,rr,rw=self.streams();again=await receive_probe(self.raw,self.trusted,rr,rw,self.recv_cfg,coordinator=self.co,fresh_journal_path=self.root/'again')
  self.assertEqual(again['status'],'failed');self.assertEqual(rw.frames,[]);self.assertFalse((self.root/'again').exists())
 async def test_reports_from_two_valid_runs_cannot_mix(self):
  original_raw,original_hash=self.raw,self.trusted
  a,b,*_=self.launch_pair();s1,r1=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertTrue(join_probe_reports(original_raw,original_hash,s1,r1)['matched'])
  second=copy.deepcopy(self.manifest);second['session']['session_id']='2'*32
  for x in second['stages']+second['tiles']:x['session_id']='2'*32
  self.raw=raw_of(second);self.trusted=hashlib.sha256(self.raw).hexdigest()
  a,b,*_=self.launch_pair(journal_name='second');s2,r2=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertTrue(join_probe_reports(self.raw,self.trusted,s2,r2)['matched'])
  self.assertFalse(join_probe_reports(original_raw,original_hash,s1,r2)['matched'])
  self.assertFalse(join_probe_reports(self.raw,self.trusted,s1,r2)['matched'])
 async def _post_credit_corruption(self,extra):
  import cluster_p1.receipt_probe_roles as roles
  original=roles._credit_complete;observed=[]
  async def after_error(channel):
   async with channel._condition:
    while not channel._closed:await channel._condition.wait()
   observed.append((channel._credit_seq,type(channel._error).__name__))
   return await original(channel)
  with patch.object(roles,'_credit_complete',after_error):
   a,b,sr,sw,rr,rw=self.launch_pair();old=rw.write
   def corrupt(data):
    old(data)
    if data[:1]==b'C' and struct.unpack('!Q',data[-8:])[0]==4:
     rw.peer.feed(extra);rw.close()
   rw.write=corrupt
   s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(observed[0][0],4);self.assertEqual(s['status'],'failed')
  self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
  return observed
 async def test_C4_followed_by_protocol_error_is_not_normal_EOF(self):
  self.assertEqual(await self._post_credit_corruption(b'X'+struct.pack('!I',0)),[(4,'ChannelError')])
 async def test_C4_then_D_body_length5_empty_EOF_rejected(self):
  self.assertEqual(await self._post_credit_corruption(b'D'+struct.pack('!I',5)),[(4,'IncompleteReadError')])
 async def test_C4_then_partial_header_EOF_rejected(self):
  self.assertEqual(await self._post_credit_corruption(b'C'),[(4,'IncompleteReadError')])
 async def _post_credit_read_failure(self,error):
  import cluster_p1.receipt_probe_roles as roles
  original=roles._credit_complete
  async def after_error(channel):
   async with channel._condition:
    while not channel._closed:await channel._condition.wait()
   return await original(channel)
  with patch.object(roles,'_credit_complete',after_error):
   a,b,sr,sw,rr,rw=self.launch_pair();read=sr.readexactly;complete=False
   async def injected(n):
    nonlocal complete
    if complete:raise error
    data=await read(n)
    if n==20 and struct.unpack('!Q',data[-8:])[0]==4:complete=True
    return data
   sr.readexactly=injected
   s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(s['credits_received'],4);self.assertEqual(s['status'],'failed')
  self.assertIsNone(s['credit_termination']);self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
 async def test_C4_then_read_timeout_not_EOF(self):await self._post_credit_read_failure(TimeoutError('fake timeout'))
 async def test_C4_then_reader_cancellation_not_EOF(self):await self._post_credit_read_failure(asyncio.CancelledError())
 async def test_read_observer_reentry_poison_is_monotonic(self):
  import cluster_p1.receipt_probe_roles as roles
  reader=Reader();proxy=roles._ObservedReader(reader)
  task=asyncio.create_task(proxy.readexactly(5));self.tasks.append(task);await asyncio.sleep(0)
  try:
   with self.assertRaises(roles.ChannelError):await proxy.readexactly(5)
   reader.feed(b'H'+struct.pack('!I',12))
   with self.assertRaises(roles.ChannelError):await task
   self.assertEqual(proxy.phase,'header');self.assertEqual(reader.reads,[5])
   with self.assertRaises(roles.ChannelError):await proxy.readexactly(5)
   self.assertFalse(proxy.clean_eof())
  finally:
   if not task.done():task.cancel()
   await asyncio.gather(task,return_exceptions=True)
 async def test_proxy_failure_before_Channel_error_already_rejects(self):
  import cluster_p1.receipt_probe_roles as roles
  recorded,release=asyncio.Event(),asyncio.Event();seen=[]
  original_read=roles._ObservedReader.readexactly;original_check=roles._credit_complete
  async def pause_failure(proxy,n):
   try:return await original_read(proxy,n)
   except asyncio.IncompleteReadError as exc:
    if exc.partial==b'C':recorded.set();await release.wait()
    raise
  async def check_gap(channel):
   await recorded.wait();seen.append((channel._credit_seq,channel._error,channel._closed))
   return await original_check(channel)
  try:
   with patch.object(roles._ObservedReader,'readexactly',pause_failure),patch.object(roles,'_credit_complete',check_gap):
    a,b,sr,sw,rr,rw=self.launch_pair();write=rw.write
    def inject(data):
     write(data)
     if data[:1]==b'C' and struct.unpack('!Q',data[-8:])[0]==4:rw.peer.feed(b'C');rw.close()
    rw.write=inject
    s,r=await asyncio.wait_for(asyncio.gather(a,b),3)
   self.assertEqual(seen,[(4,None,False)]);self.assertEqual(s['status'],'failed')
   self.assertFalse(join_probe_reports(self.raw,self.trusted,s,r)['matched'])
  finally:release.set()
 async def test_success_done_emitted_after_cleanup(self):
  done=[]
  def capture(kind,data):
   if kind=='role_done':done.append(data)
  a,b,*_=self.launch_pair(sender_emit=capture,receiver_emit=capture);await asyncio.wait_for(asyncio.gather(a,b),3)
  self.assertEqual(len(done),2);self.assertTrue(all(d['cleanup_ok'] for d in done))
if __name__=='__main__':unittest.main(verbosity=2)
