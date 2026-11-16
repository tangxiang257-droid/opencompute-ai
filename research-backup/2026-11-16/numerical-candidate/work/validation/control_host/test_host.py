"""Actual role dial and central handlers; sockets/TLS boundaries are injected."""
import asyncio,hashlib,ssl,unittest,importlib.util
from unittest.mock import patch
from validation.control_dial.test_dial import Flow as Base
from validation.authority_chain.test_chain import wire
from validation.control_exchange.test_transport import Holds
from validation.install_host.test_host import Socket,context
from cluster_p1.control_exchange import _json
import cluster_p1.role_dial as dial
class Flow(Base):
 async def host_inputs(self,stage,spec):
  l=self.launchers[stage];node=self.body['placements'][stage]['node']
  cr,cw,sr,sw=wire.streams('controller',node);ar,aw,ir,iw=wire.streams(node,'issuer')
  self.raw_streams=(sr,sw,ar,aw);self.central_streams={'control':(cr,cw),'actions':(ir,iw)}
  materials={k:_json({'leaf_sha256':hashlib.sha256(peer.encode()).hexdigest()}) for k,peer in [('control','controller'),('actions','issuer')]}
  tls={k:wire.config(node) for k in reversed(materials)}
  endpoints={k:dict(bind='127.0.0.1',port=23456+i,hostname=k+'.example',node=peer,certificate=hashlib.sha256(peer.encode()).hexdigest(),material_sha256=hashlib.sha256(materials[k]).hexdigest()) for i,(k,peer) in enumerate([('control','controller'),('actions','issuer')])}
  raw=_json(dict(version=1,purpose='central-control-action-host',experiment=self.receipt.proposal_id,context=self.receipt.proposal_digest,stage=stage,role_node=node,role_certificate=hashlib.sha256(node.encode()).hexdigest(),charge=2097152,accept_seconds=5,handshake_seconds=5,close_seconds=5,**endpoints))
  self.host_args=(raw,hashlib.sha256(raw).hexdigest(),self.adm,self.token,tls,materials)
  self.host_kw=dict(control_identity=Holds(),action_identity=Holds(),clock=self.clock,resources=self.central_resources)
  role_materials={k:b'LAB role material boundary' for k in materials}
  role_endpoints={k:dict(host=k+'.example',port=endpoints[k]['port'],hostname=k+'.example',peer=endpoints[k]['node'],certificate=endpoints[k]['certificate'],material_sha256=hashlib.sha256(role_materials[k]).hexdigest()) for k in materials}
  role_raw=_json(dict(version=1,purpose='role-control-dial',launcher_sha256=l.config._sha,**role_endpoints))
  self.dial_args=(l,role_raw,hashlib.sha256(role_raw).hexdigest(),{'control':wire.config('controller'),'actions':wire.config('issuer')},role_materials)
  self.dial_kw=dict(enabled=True,identity=Holds(),authorize=Holds(),action_identity=Holds(),clock=self.clock)
  return l
 async def register_role(self,stage,spec,operation,*,bind=True):
  self.assertTrue(importlib.util.find_spec('private_control_action_host') is not None,'central control/action accepted-stream host missing')
  import private_control_action_host as host_module
  l=await self.host_inputs(stage,spec);host=host_module.ControlActionHost(*self.host_args,**self.host_kw);self.owned.append(host)
  self.control_hosts=getattr(self,'control_hosts',[])+[host]
  sockets=[Socket(),Socket()];accepted=[Socket(),Socket()];pairs=dict(self.central_streams);seen=[]
  async def accept(sock):
   self.assertTrue(all(s.bound is not None and s.backlog==1 for s in sockets),'both listeners must precede first accept')
   index=sockets.index(sock);return accepted[index],('127.0.0.1',30000+index)
  async def handshake(slot,sock,ctx,*args):
   self.assertTrue(sockets[accepted.index(sock)].closed)
   return pairs[('control','actions')[accepted.index(sock)]]
  async def opened(hostname,port,**opts):
   seen.append(hostname);return (self.raw_streams[0],self.raw_streams[1]) if hostname=='control.example' else (self.raw_streams[2],self.raw_streams[3])
  def client_context(*args):
   c=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);c.minimum_version=ssl.TLSVersion.TLSv1_2;return c
  with patch.object(host_module.socket,'socket',side_effect=sockets),patch.object(asyncio.get_running_loop(),'sock_accept',accept),patch.object(host_module,'load_context',lambda *a,**k:context()),patch.object(host_module,'_accepted_tls_streams',handshake),patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',client_context):
   start=asyncio.create_task(host.start(enabled=True))
   try:
    role=await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
    async def control_loop():
     while True:await l.serve_once()
    self.spawn_job(control_loop())
    await start
   finally:
    if not start.done():start.cancel();await asyncio.gather(start,return_exceptions=True)
  self.assertEqual(host.state,'READY');self.assertEqual(seen,['control.example','actions.example']);self.assertTrue(all(s.closed for s in sockets))
  self.controls[stage]=host.control;self.action_servers[stage]=host.assembly.server
  async def actions():
   for _ in range(2):await host.serve_action_once()
  self.spawn_job(actions());return role,host.control
 async def test_dial_host_real_authority_chain(self):
  await self.run_chain();self.assertEqual(len(self.control_hosts),2)
  for host in self.control_hosts:self.assertEqual(host.action_count,2)
def load_tests(*args):return unittest.TestSuite([Flow('test_dial_host_real_authority_chain')])
if __name__=='__main__':unittest.main()
