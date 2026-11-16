"""Existing full authority/result chain, accepted hidden-data seam injected."""
from validation.b_result_dial.test_pipeline import *
from validation.b_result_dial.test_pipeline import Flow as Base
import importlib
from cluster_p1.control_exchange import _json

class Flow(Base):
 async def prepare_hidden(self):
  s=self.spec;ar,aw,br,bw=wire.streams('a','b');self.hidden_raw=(br,bw)
  g=Grant(s.request,s.session,s.epoch,0,(0,1),s.model,2)
  self.hidden_send=Channel(ar,aw,wire.config('b'),{'b':{g}},capacity_bytes=65536,capacity_tiles=1)
  self.channels.append(self.hidden_send);self.prepared=None
 async def serve_installation_endpoint(self,service,launch,args,policy):
  if launch.config.data['role']=='A':return await super().serve_installation_endpoint(service,launch,args,policy)
  self.assertIsNotNone(importlib.util.find_spec('private_b_hidden_host'),'missing B accepted data host')
  module=importlib.import_module('private_b_hidden_host');self.hidden_module=module
  material=_json({'leaf_sha256':hashlib.sha256(b'b').hexdigest()})
  c=dict(version=1,purpose='B-hidden-host',launcher_sha256=launch.config._sha,spec_sha256=digest(asdict(self.spec)),peer='a',peer_certificate=hashlib.sha256(b'a').hexdigest(),certificate=hashlib.sha256(b'b').hexdigest(),material_sha256=hashlib.sha256(material).hexdigest(),bind='127.0.0.1',port=26001,hostname='b.example',accept_seconds=5,handshake_seconds=5,close_seconds=5,session=dict(interval=2,silence=10,lifetime=120))
  raw=_json(c);self.hidden_args=(raw,hashlib.sha256(raw).hexdigest(),launch,self.spec,wire.config('a'),material)
  self.hidden_kw=dict(identity=Identity(),authorize=Holds(),resources=launch.startup.runtime['resources'])
  self.assertIsNotNone(importlib.util.find_spec('cluster_p1.hidden_session'),'missing original A session assembly')
  from cluster_p1.hidden_session import open_hidden_session
  from cluster_p1.data_session import DataSessionPolicy
  host=module.BHiddenHost(*self.hidden_args,**self.hidden_kw);self.hidden_host=host;self.owned.append(host)
  with self.assertRaises(Exception):await host.start()
  self.assertFalse(host.started)
  listener=Socket();accepted=Socket()
  async def accept(sock):self.assertIs(sock,listener);return accepted,('127.0.0.1',30000)
  async def handshake(*a):return self.hidden_raw
  with patch.object(module.socket,'socket',return_value=listener),patch.object(host.loop,'sock_accept',accept),patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module,'_accepted_tls_streams',handshake):
   _,self.a_session=await asyncio.gather(host.start(enabled=True),open_hidden_session(self.launchers[0],self.hidden_send,self.spec,local_certificate=hashlib.sha256(b'a').hexdigest(),peer_certificate=hashlib.sha256(b'b').hexdigest(),policy=DataSessionPolicy(**c['session']),identity=Identity(),authorize=Holds(),enabled=True))
  self.owned.append(self.a_session)
  self.assertTrue(self.hidden_send._start_attempted)
  self.assertEqual(host.state,'PREPARED');self.assertTrue(all(f[:1] not in (b'H',b'C',b'D') for f in self.hidden_raw[1].frames))
  self.prepared=host.prepared;self.channels.append(self.prepared.channel)
  self.assertIsNone(self.prepared.journal);self.assertFalse(self.prepared.channel._receipt_h_started)
  # Same existing central installation service, no alternate installation path.
  central_listener=Socket();central_accepted=Socket()
  async def central_accept(sock):return central_accepted,('127.0.0.1',30001)
  async def central_handshake(*a):return service.lab_streams
  with patch('private_installation_host.socket.socket',return_value=central_listener),patch.object(host.loop,'sock_accept',central_accept),patch('private_installation_host.load_context',lambda *a,**k:context()),patch('private_installation_host.InstallationHost._handshake',central_handshake):
   await asyncio.gather(service.start(enabled=True),host.install(enabled=True,session_policy=policy))
  self.assertEqual(host.state,'READY');self.assertIs(host.prepared.journal,launch.startup.journal)
 async def execute_a(self):
  role,control=await self.prepare_a();receipt=await control.dispatch();self.assertEqual(receipt['state'],'COMMITTED')
  a,b=await asyncio.gather(self.a_session.send_committed(enabled=True),self.hidden_host.receive_once(enabled=True));self.assertEqual(a,b)
  self.assertEqual(self.hidden_host.state,'RECEIVED')
  return replace(self.spec,inputs=(b['payload_digest'],))
 async def test_full_chain_through_accepted_hidden_host(self):await self.run_chain()
def load_tests(*args):return unittest.TestSuite([Flow('test_full_chain_through_accepted_hidden_host')])
if __name__=='__main__':unittest.main()
