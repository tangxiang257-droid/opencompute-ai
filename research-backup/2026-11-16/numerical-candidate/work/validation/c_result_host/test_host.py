import json,asyncio,hashlib,importlib.util,unittest
from dataclasses import asdict
from unittest.mock import patch
from validation.control_host.test_host import Flow as Base
from validation.authority_chain.test_chain import wire,Channel,Grant,Identity,Holds,CPolicy
from validation.install_host.test_host import Socket,context
from cluster_p1.control_exchange import _json
class Flow(Base):
 def result_metadata_pair(self):
  r,w,cr,cw=wire.streams('b','c');s=self.spec;g=Grant(s.request,s.session,1,1,(0,1),s.model,2)
  channel=Channel(r,w,wire.config('c'),{'c':{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(channel)
  return channel,(cr,cw)
 async def attach_c_result(self,receiving,actual,rr,rw,cmeta,binding,d):
  self.assertTrue(importlib.util.find_spec('private_c_result_host') is not None,'C result endpoint host missing')
  import private_c_result_host as module
  r,w,gr,gw=wire.streams('b','c');g=Grant(actual.request,actual.session,1,1,(0,1),actual.model,2)
  bg=Channel(r,w,wire.config('c'),{'c':{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(bg)
  self.launchers[1].attach_gate(bg,d,identity=Identity(),authorize=CPolicy())
  pairs={'data':(rr,rw),'metadata':cmeta,'gate':(gr,gw)}
  material=_json({'leaf_sha256':hashlib.sha256(b'c').hexdigest()})
  endpoints={k:dict(bind='127.0.0.1',port=25000+i,hostname=k+'.example') for i,k in enumerate(pairs)}
  config=dict(version=1,purpose='C-result-host',launcher_sha256=receiving.config._sha,spec_sha256=hashlib.sha256(_json(json.loads(json.dumps(asdict(actual))))).hexdigest(),binding_sha256=hashlib.sha256(_json(json.loads(json.dumps(asdict(binding))))).hexdigest(),certificate=hashlib.sha256(b'c').hexdigest(),material_sha256=hashlib.sha256(material).hexdigest(),accept_seconds=5,handshake_seconds=5,close_seconds=5,**endpoints)
  raw=_json(config);self.c_host_args=(raw,hashlib.sha256(raw).hexdigest(),receiving,actual,binding,wire.config('b'),material)
  self.c_host_kw=dict(identity=Identity(),authorize=Holds(),gate_identity=Identity(),gate_authorize=CPolicy(),resources=receiving.role.resources)
  host=module.CResultHost(*self.c_host_args,**self.c_host_kw);self.c_host=host;self.owned.append(host)
  await self.start_c_host(host,pairs)
 async def start_c_host(self,host,pairs):
  import private_c_result_host as module
  sockets=[Socket() for _ in pairs];accepted=[Socket() for _ in pairs]
  async def accept(sock):
   self.assertTrue(all(s.bound is not None and s.backlog==1 for s in sockets))
   return accepted[sockets.index(sock)],('127.0.0.1',30000)
  async def handshake(slot,sock,ctx,*args):return pairs[tuple(pairs)[accepted.index(sock)]]
  with patch.object(module.socket,'socket',side_effect=sockets),patch.object(asyncio.get_running_loop(),'sock_accept',accept),patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module,'_accepted_tls_streams',handshake):await host.start(enabled=True)
  self.assertTrue(all(s.closed for s in sockets));self.assertEqual(host.state,'READY');self.assertIsNone(host.endpoints['data'].channel);self.assertIsNone(host.role.ingress._receiver);self.assertFalse(host.endpoints['metadata'].channel._start_attempted);self.assertFalse(host.endpoints['gate'].channel._start_attempted)
 async def deliver_c_result(self,receiving,control,sender,actual,spool,op,d):
  job=asyncio.create_task(self.c_host.serve_once(self.artifacts,lambda:None,lambda:self.cp.hold(d,'receive'),enabled=True));self.jobs.append(job)
  receipt=await control.dispatch();self.assertEqual(receipt['state'],'COMMITTED');op.enable_read()
  return await asyncio.gather(sender.send(actual,spool,op.metadata['output_digest'],op.check_export),job)
 async def test_real_control_and_C_host_to_durable_result(self):
  await self.run_chain();self.assertEqual(self.c_host.state,'RECEIVED')
def load_tests(*args):return unittest.TestSuite([Flow('test_real_control_and_C_host_to_durable_result')])
if __name__=='__main__':unittest.main()
