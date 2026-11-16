"""Actual B/C staged transport assembly; all sockets/TLS/backends are injected."""
import ssl
from dataclasses import asdict
from validation.authority_chain.test_chain import *
from validation.control_host.test_host import Flow as Base
from validation.install_host.test_host import Socket,context
from cluster_p1.control_exchange import _json
from cluster_p1.result_dial import BResultDial
from private_c_result_host import CResultHost
import cluster_p1.result_dial as dial_module
import private_c_result_host as host_module

def digest(value):return hashlib.sha256(_json(json.loads(json.dumps(value)))).hexdigest()
class Flow(Base):
 async def pending_endpoints(self,actual):
  s=actual;d=self.cp.descriptor
  binding=ReceiveBinding(Scope(s.request,1,s.plan_digest,s.layout,'out','logits',s.session,'single-token','b',hashlib.sha256(b'b').hexdigest(),'placeholder'),1,s.model,2,'float32',((0,1,(1,1,s.vocab)),))
  ac,cc=self.pair('issuer','c',1);store=ReceivingStore(self.root/'c-receiving');self._dependency_closers.append(store.close)
  controller=ReceivingController(ac,ReceivingStore.for_authority(self.strict),d,identity=Identity(),authorize=self.cp,maintenance=self.cp,resources=CPolicy());self.owned.append(controller)
  receiving=await ReceivingLauncher.create(configuration(self.root,'C',d),cc,store,d,identity=Identity(),authorize=self.cp,maintenance=self.cp,resources=CPolicy());self.owned.append(receiving);self.c_launcher=receiving
  await asyncio.gather(controller.pending(),receiving.serve_once())
  materials={x:_json({'leaf_sha256':hashlib.sha256(x.encode()).hexdigest()}) for x in ('b','c')}
  endpoints={k:dict(bind='127.0.0.1',port=25000+i,hostname=k+'.example') for i,k in enumerate(('data','metadata','gate'))}
  c=dict(version=1,purpose='C-result-host',launcher_sha256=receiving.config._sha,spec_sha256=digest(asdict(s)),binding_sha256=digest(asdict(binding)),certificate=hashlib.sha256(b'c').hexdigest(),material_sha256=hashlib.sha256(materials['c']).hexdigest(),accept_seconds=5,handshake_seconds=5,close_seconds=5,prepare_seconds=30,**endpoints)
  craw=_json(c);self.host_args=(craw,hashlib.sha256(craw).hexdigest(),receiving,s,binding,wire.config('b'),materials['c']);self.host_kw=dict(identity=Identity(),authorize=Holds(),gate_identity=Identity(),gate_authorize=CPolicy(),resources=receiving.role.resources)
  host=CResultHost(*self.host_args,**self.host_kw);self.owned.append(host);self.result_host=host
  b=dict(version=1,purpose='B-result-dial',launcher_sha256=self.launchers[1].config._sha,spec_sha256=c['spec_sha256'],binding_sha256=c['binding_sha256'],receiving_sha256=digest(d),peer_certificate=c['certificate'],material_sha256=hashlib.sha256(materials['b']).hexdigest(),timeout=5,prepare_seconds=30,**{k:dict(host=e['hostname'],port=e['port'],hostname=e['hostname']) for k,e in endpoints.items()})
  braw=_json(b);self.dial_args=(braw,hashlib.sha256(braw).hexdigest(),self.launchers[1],s,binding,d,wire.config('c'),materials['b']);self.dial_kw=dict(identity=Identity(),authorize=Holds(),gate_identity=Identity(),gate_authorize=CPolicy())
  sender=BResultDial(*self.dial_args,**self.dial_kw);self.owned.append(sender);self.result_dial=sender
  self.result_streams={}
  for k in endpoints:
   br,bw,cr,cw=wire.streams('b','c');bw.unique=cw.unique=b'LAB-'+k.encode();self.result_streams[k]=((br,bw),(cr,cw))
  self.pending_controller=controller;return sender,host,receiving,store,d
 async def connect_endpoints(self,sender,host):
  sockets=[Socket(),Socket(),Socket()];accepted=[Socket(),Socket(),Socket()];connected={k:asyncio.Event() for k in self.result_streams};seen=[]
  async def accept(sock):
   self.assertTrue(all(s.bound is not None for s in sockets));return accepted[sockets.index(sock)],('127.0.0.1',30000)
  async def handshake(slot,sock,*a):
   k=tuple(connected)[accepted.index(sock)];await connected[k].wait();return self.result_streams[k][1]
  async def opened(hostname,port,**opts):
   k=hostname.split('.')[0];seen.append(k);connected[k].set();return self.result_streams[k][0]
  def client(*a,**kw):return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
  with patch.object(host_module.socket,'socket',side_effect=sockets),patch.object(host.loop,'sock_accept',accept),patch.object(host_module,'load_context',lambda *a,**k:context()),patch.object(host_module,'_accepted_tls_streams',handshake),patch.object(dial_module,'load_context',client),patch.object(dial_module.asyncio,'open_connection',opened):
   await asyncio.gather(host.prepare(enabled=True),sender.prepare(enabled=True))
  self.assertEqual(seen,['data','metadata','gate']);self.assertEqual((host.state,sender.state),('CONNECTED','CONNECTED'))
  self.assertIsNone(self.launchers[1].operation);self.assertIsNone(host.role.ingress);self.assertFalse(host.role.receiving)
  for a,b in self.result_streams.values():self.assertEqual(a[1].frames,[]);self.assertEqual(b[1].frames,[])
 async def run_chain(self):
  actual=await self.execute_a();sender,host,receiving,store,d=await self.pending_endpoints(actual)
  await self.connect_endpoints(sender,host)
  s=actual;source=Book(self.root/'source-book',2097152);dest=Book(self.root/'dest-book',2097152);self._dependency_closers.extend((source.close,dest.close))
  plan=Plan(Binding(s.owner,s.request,s.session,'single-token',1,sender.generation(),s.model,s.layout,s.input_snapshot,s.token_digest,s.preprocessing,s.plan_digest),(1,),s.vocab)
  source.reserve('result',plan,'source');dest.reserve('result',plan,'destination');spool=Spool(self.root/'spool',source,dest,'result',plan)
  op=self.launchers[1].build_operation(actual,self.pipe_policy,spool=spool);self.b_operation=op
  role,control=await self.register_role(1,actual,op)
  await asyncio.gather(self.pending_controller.activate(CommittedBInput(control)),receiving.serve_once())
  await host.attach_ready(enabled=True);await sender.attach_ready(enabled=True)
  job=asyncio.create_task(host.serve_once(self.artifacts,lambda:None,lambda:self.cp.hold(d,'receive'),enabled=True));self.jobs.append(job)
  receipt=await control.dispatch();self.assertEqual(receipt['state'],'COMMITTED');op.enable_read()
  ack,manifest=await asyncio.gather(sender.send(actual,spool,op.metadata['output_digest'],op.check_export),job)
  self.assertEqual(ack['manifest_digest'],manifest);self.assertEqual(store.history(self.owner,s.request)['state'],'RECEIVED')
  chunks=[];self.artifacts.fetch(self.owner,s.request,lambda _,raw:chunks.append(raw));self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*100)
 async def test_actual_pending_connect_proof_activate_result(self):await self.run_chain()
def load_tests(*args):return unittest.TestSuite([Flow('test_actual_pending_connect_proof_activate_result')])
if __name__=='__main__':unittest.main()
