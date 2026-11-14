"""Actual installation chain; socket/TLS boundaries injected, never listening."""
import asyncio,hashlib,ssl,unittest
from unittest.mock import patch
from validation.role_entry.test_entry import Flow as Base
from validation.online_issuance.test_transport import Identity,Holds
from cluster_p1.control_exchange import _json

class Socket:
 def __init__(self):self.closed=False;self.bound=None;self.backlog=None
 def setblocking(self,value):pass
 def bind(self,address):self.bound=address
 def listen(self,n):self.backlog=n
 def close(self):self.closed=True
def context():
 c=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);c.verify_mode=ssl.CERT_REQUIRED;c.minimum_version=ssl.TLSVersion.TLSv1_2;return c
class Flow(Base):
 async def create_installation_endpoint(self,stage,r,w,tls,grants):
  from private_installation_host import InstallationHost
  material=_json({'leaf_sha256':hashlib.sha256(b'issuer').hexdigest()})
  d=dict(version=1,purpose='one-installation-host',experiment=self.receipt.proposal_id,context=self.receipt.proposal_digest,stage=stage,bind='127.0.0.1',port=23456+stage,issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest(),server_hostname='issuer.example',role_node=('a','b')[stage],role_certificate=hashlib.sha256(('a','b')[stage].encode()).hexdigest(),material_sha256=hashlib.sha256(material).hexdigest(),accept_seconds=5,handshake_seconds=5,close_seconds=5)
  raw=_json(d);host=InstallationHost(raw,hashlib.sha256(raw).hexdigest(),self.adm,self.token,tls,grants,material,identity=Identity(),authorize=Holds(),resources=self.central_resources)
  host.lab_streams=(r,w);self.hosts=getattr(self,'hosts',[])+[host];return host
 async def serve_installation_endpoint(self,host,launch,args,policy):
  listener=Socket();accepted=Socket()
  async def accept(sock):self.assertIs(sock,listener);return accepted,('127.0.0.1',34567)
  async def handshake(obj,sock,ctx):
   self.assertTrue(listener.closed);self.assertIs(sock,accepted);return host.lab_streams
  with patch('private_installation_host.socket.socket',return_value=listener),patch.object(asyncio.get_running_loop(),'sock_accept',accept),patch('private_installation_host.load_context',lambda *a,**k:context()),patch('private_installation_host.InstallationHost._handshake',handshake):
   await asyncio.gather(host.start(enabled=True),launch.install(**args,session_policy=policy))
  self.assertEqual(listener.backlog,1);self.assertTrue(listener.closed);self.assertEqual(host.state,'ACTIVE');self.assertTrue(host.service._session.active)
 async def test_existing_A_B_C_chain_from_host(self):
  await self.run_chain();self.assertEqual(len(self.hosts),2)

def load_tests(loader,tests,pattern):return unittest.TestSuite([Flow('test_existing_A_B_C_chain_from_host')])
if __name__=='__main__':unittest.main()
