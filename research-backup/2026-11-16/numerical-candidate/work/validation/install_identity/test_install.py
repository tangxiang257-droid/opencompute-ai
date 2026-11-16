"""Original install/dial chain over memory streams; external authority is LAB."""
import asyncio,hashlib,unittest
from unittest.mock import patch
from validation.host_dial.test_dial import Cases as DialCases,Pipeline,context
from cluster_p1.role_dial import dial_stage
from cluster_p1.channel import Channel
from validation.scoped_identity.test_identity import Cases as ChannelCases

class Flow(Pipeline):
 async def create_stage(self,c,r,w,tls,grants,store,d,**opts):
  opts['identity']=None;seen=[];original=Channel.__init__
  def init(ch,reader,*a,**kw):
   if reader is r:seen.append(ch)
   original(ch,reader,*a,**kw)
  async def opened(*a,**kw):return r,w
  with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()),patch.object(Channel,'__init__',init):
   launch=await dial_stage(c,'issuer.example',443,tls,grants,store,d,identity_mode='channel',material=b'LAB loader',material_pin='LAB',server_hostname='issuer.example',enabled=True,**opts)
  self.assertEqual(seen,[launch.startup.channel]);self.assertEqual(w.frames,[])
  self.assertIs(launch._providers[0],launch.startup.client.identity)
  self.dial_stages=getattr(self,'dial_stages',[])+[d['stage']];return launch

class Cases(DialCases):
 async def test_identity_callback_swap_refuses_direct_client_and_launcher(self):
  from contextlib import contextmanager
  async def probe(c,r,w,tls,grants,store,d,opts):
   async def opened(*a,**kw):return r,w
   kw=dict(opts,identity=None,identity_mode='channel',material=b'LAB',material_pin='LAB',server_hostname='issuer.example',enabled=True)
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):launch=await dial_stage(c,'issuer.example',443,tls,grants,store,d,**kw)
   old=launch.startup.client.identity.hold;seen=[]
   @contextmanager
   def swapped(actual):yield 'wrong-principal'
   try:
    launch.startup.client.identity.hold=swapped
    with self.assertRaises(Exception):
     with launch.startup.client.fence(d,'diagnostic') as principal:seen.append(principal)
    launch.startup.client.identity.hold=old
    with self.assertRaises(Exception):launch.check()
    self.assertEqual(seen,[])
   finally:await launch.close()
  await self.case(probe)
 async def test_bad_mode_and_missing_authority_never_dial(self):
  async def probe(c,r,w,tls,grants,store,d,opts):
   calls=[]
   async def opened(*a,**kw):calls.append(1);return r,w
   with patch('asyncio.open_connection',opened),patch('cluster_p1.role_dial.load_context',lambda *a:context()):
    for change in ({'identity_mode':'other'},{'identity_mode':'channel'},{'identity':None},{'identity_mode':'channel','identity':None,'authorize':None},{'identity_mode':'channel','identity':None,'resources':None},{'identity_mode':'channel','identity':None,'completion':None}):
     kw=dict(opts,enabled=True,server_hostname='issuer.example',material=b'LAB',material_pin='LAB');kw.update(change)
     with self.assertRaises(ValueError):await dial_stage(c,'issuer.example',443,tls,grants,store,d,**kw)
   self.assertEqual(calls,[]);self.assertFalse(c.path('claim').exists());self.assertFalse(w.is_closing())
  await self.case(probe)

class IdentityCases(ChannelCases):
 def principal_view(self):
  from cluster_p1.role_startup import _InstallationIdentity
  peer=self.c.authenticated_identity
  return _InstallationIdentity(self.c,peer.node_id,peer.certificate_sha256)
 async def test_principal_is_fixed_original_peer(self):
  view=self.principal_view()
  with view.hold(self.c.authenticated_identity) as principal:self.assertEqual(principal,'a')
 async def test_exit_revocation_retires(self):
  view=self.principal_view()
  with self.assertRaises(Exception):
   with view.hold(self.c.authenticated_identity):self.c.revoke_identity()
  self.assertTrue(view.retired)
 async def test_new_connection_never_restores_view(self):
  view=self.principal_view();new,_=self.pair()
  with self.assertRaises(Exception):
   with view.hold(new.authenticated_identity):self.fail('replayed')
  self.assertTrue(view.retired);self.assertFalse(self.c._closed)
 async def test_wrong_issuer_pin_refuses(self):
  from cluster_p1.role_startup import _InstallationIdentity
  with self.assertRaises(Exception):_InstallationIdentity(self.c,'a','0'*64)

def load_tests(loader,tests,pattern):return unittest.TestSuite([Flow('test_A_B_C_via_raw_dial_and_control_entry'),*(Cases(n) for n in Cases.__dict__ if n.startswith('test_')),*(IdentityCases(n) for n in IdentityCases.__dict__ if n.startswith('test_'))])
if __name__=='__main__':unittest.main()
