"""Real local certificate loading into LAB memory dial/A-B-C; no TLS handshake."""
import ssl,unittest
from unittest.mock import patch
from validation.host_dial.test_dial import Pipeline as Base
from validation.tls_material.test_material import Material
from cluster_p1.channel import TLSConfig
from cluster_p1.role_dial import dial_stage

class Flow(Base):
 async def create_stage(self,c,r,w,tls,grants,store,d,**opts):
  material=Material();material.setUp();self.addCleanup(material.doCleanups)
  original,raw,pin=material.args();tls=TLSConfig(original.certfile,original.keyfile,original.cafile,tls.peers)
  contexts=[]
  async def opened(*args,**kw):contexts.append(kw['ssl']);return r,w
  with patch('asyncio.open_connection',opened):
   for changes in ({'material':None},{'material_pin':None},{'identity':None},{'resources':None},{'enabled':False}):
    kw=dict(opts,material=raw,material_pin=pin,enabled=True,server_hostname='issuer.example');kw.update(changes)
    with self.assertRaises(ValueError):await dial_stage(c,'issuer.example',443,tls,grants,store,d,**kw)
   self.assertEqual(contexts,[])
   launch=await dial_stage(c,'issuer.example',443,tls,grants,store,d,material=raw,material_pin=pin,enabled=True,server_hostname='issuer.example',**opts)
  self.assertEqual(len(contexts),1);self.assertIs(type(contexts[0]),ssl.SSLContext);self.assertEqual(contexts[0].cert_store_stats()['x509_ca'],1+len(material.baseline))
  self.dial_stages=getattr(self,'dial_stages',[])+[d['stage']];return launch

def load_tests(loader,tests,pattern):return unittest.TestSuite([Flow('test_A_B_C_via_raw_dial_and_control_entry')])
if __name__=='__main__':unittest.main()
