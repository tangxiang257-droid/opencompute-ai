"""Actual installed chain; injected raw stream dial, no listener/device/model."""
import asyncio,hashlib,ssl,unittest
from unittest.mock import patch
from validation.c_service.test_service import Flow as Base
from validation.authority_chain.test_chain import Channel,Grant,wire
from validation.control_exchange.test_transport import Holds
from cluster_p1.control_exchange import _json
import cluster_p1.role_dial as dial
class Flow(Base):
 async def endpoints(self,stage,spec):
  l=self.launchers[stage];node=self.body['placements'][stage]['node']
  cr,cw,sr,sw=wire.streams('controller',node);ar,aw,ir,iw=wire.streams(node,'issuer')
  g=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
  client=Channel(cr,cw,wire.config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1)
  ps=Channel(ir,iw,wire.config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1)
  self.channels.extend((client,ps));await asyncio.gather(client.start(),ps.start())
  self.raw_streams=(sr,sw,ar,aw)
  materials={k:b'LAB material boundary' for k in ('control','actions')}
  tls={'control':wire.config('controller'),'actions':wire.config('issuer')}
  endpoints={k:dict(host=k+'.example',port=23456+i,hostname=k+'.example',peer=peer,certificate=hashlib.sha256(peer.encode()).hexdigest(),material_sha256=hashlib.sha256(materials[k]).hexdigest()) for i,(k,peer) in enumerate((('control','controller'),('actions','issuer')))}
  raw=_json(dict(version=1,purpose='role-control-dial',launcher_sha256=l.config._sha,**endpoints))
  self.dial_args=(l,raw,hashlib.sha256(raw).hexdigest(),tls,materials)
  kw=dict(enabled=True,identity=Holds(),authorize=Holds(),action_identity=Holds(),clock=self.clock)
  self.dial_kw=kw
  return (l,),kw,client,ps
 async def connect_role(self,args,kw):
  self.assertTrue(callable(getattr(dial,'dial_control_ready',None)),'installed role cannot dial control/action path')
  sr,sw,ar,aw=self.raw_streams;seen=[]
  async def opened(host,port,**opts):
   seen.append(host);self.assertTrue(opts['ssl'].check_hostname);self.assertEqual(opts['server_hostname'],host)
   return (sr,sw) if host=='control.example' else (ar,aw)
  def context(*args):
   c=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);c.minimum_version=ssl.TLSVersion.TLSv1_2;return c
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   result=await dial.dial_control_ready(*self.dial_args,**kw)
  self.assertEqual(seen,['control.example','actions.example']);self.assertEqual([f[:1] for f in aw.frames].count(b'H'),1)
  return result
 async def test_complete_chain_uses_raw_control_dial(self):
  await self.run_chain();self.assertEqual(self.calls[0][0][0],'stage0');self.assertEqual(self.calls[1][0][0],'stage1')
def load_tests(*args):return unittest.TestSuite([Flow('test_complete_chain_uses_raw_control_dial')])
if __name__=='__main__':unittest.main()
