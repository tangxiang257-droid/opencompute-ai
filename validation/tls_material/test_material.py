"""Ephemeral LAB certificates, local SSL loading only; no sockets or real identity."""
import datetime,hashlib,json,os,pathlib,ssl,tempfile,threading,unittest
from unittest.mock import patch
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID

def digest(b):return hashlib.sha256(b).hexdigest()
class Material(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=pathlib.Path(self.tmp.name)
  key=ec.generate_private_key(ec.SECP256R1());ca_key=ec.generate_private_key(ec.SECP256R1());now=datetime.datetime.now(datetime.timezone.utc)
  name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'synthetic.invalid')])
  def cert(pub,issuer,ca):
   return x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(pub).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(hours=1)).add_extension(x509.BasicConstraints(ca=ca,path_length=None),critical=True).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),critical=False).sign(issuer,hashes.SHA256())
  leaf=cert(key.public_key(),ca_key,False);ca=cert(ca_key.public_key(),ca_key,True)
  # The managed interpreter injects public rejection-policy certs. Explicitly
  # pin them in this LAB bundle; never bypass the interpreter or hide its store.
  self.baseline=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).get_ca_certs(binary_form=True)
  self.data={'cert.pem':leaf.public_bytes(serialization.Encoding.PEM),'ca.pem':ca.public_bytes(serialization.Encoding.PEM)+b''.join(ssl.DER_cert_to_PEM_cert(b).encode() for b in self.baseline),'key.pem':key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())}
  for n,b in self.data.items():(self.root/n).write_bytes(b);(self.root/n).chmod(0o600)
  self.body=dict(version=1,directory=str(self.root),certificate='cert.pem',private_key='key.pem',ca='ca.pem',certificate_sha256=digest(self.data['cert.pem']),ca_sha256=digest(self.data['ca.pem']),leaf_sha256=digest(leaf.public_bytes(serialization.Encoding.DER)))
 def args(self):
  from cluster_p1.channel import TLSConfig
  raw=json.dumps(self.body,sort_keys=True,separators=(',',':')).encode()
  tls=TLSConfig(str(self.root/'cert.pem'),str(self.root/'key.pem'),str(self.root/'ca.pem'),{'issuer':'a'*64})
  return tls,raw,digest(raw)
 def load(self):
  from cluster_p1.tls_material import load_context
  return load_context(*self.args())
 def test_real_local_context(self):
  with patch.dict(os.environ,{'SSLKEYLOGFILE':str(self.root/'must-not-exist')}):ctx=self.load()
  self.assertIs(type(ctx),ssl.SSLContext);self.assertTrue(ctx.check_hostname);self.assertEqual(ctx.verify_mode,ssl.CERT_REQUIRED);self.assertIsNone(ctx.keylog_filename);self.assertFalse((self.root/'must-not-exist').exists())
 def test_pin_mismatch(self):
  for field in ('certificate_sha256','ca_sha256','leaf_sha256'):
   with self.subTest(field=field):
    old=self.body[field];self.body[field]='0'*64
    with self.assertRaises(ValueError):self.load()
    self.body[field]=old
 def test_key_mismatch_sanitized(self):
  key=ec.generate_private_key(ec.SECP256R1());(self.root/'key.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
  with self.assertRaises(ValueError) as caught:self.load()
  self.assertEqual(str(caught.exception),'TLS material unavailable');self.assertIsNone(caught.exception.__cause__)
 def test_permissions_links_types_and_bounds(self):
  for name in ('cert.pem','key.pem','ca.pem'):
   path=self.root/name;path.chmod(0o644)
   with self.assertRaises(ValueError):self.load()
   path.chmod(0o600);path.unlink();path.symlink_to('missing')
   with self.assertRaises(ValueError):self.load()
   path.unlink();os.mkfifo(path,0o600)
   with self.assertRaises(ValueError):self.load()
   path.unlink();path.write_bytes(self.data[name]);path.chmod(0o600)
  os.link(self.root/'key.pem',self.root/'alias')
  with self.assertRaises(ValueError):self.load()
  (self.root/'alias').unlink();(self.root/'key.pem').write_bytes(b'x'*131073)
  with self.assertRaises(ValueError):self.load()
 def test_load_uses_sealed_snapshot_after_source_replacement(self):
  from cluster_p1 import tls_material as m
  entered=threading.Event();release=threading.Event();original=m._build;results=[];failures=[]
  def build(paths,expected_ca):
   entered.set();self.assertTrue(release.wait(5))
   for name,path in zip(('cert.pem','key.pem','ca.pem'),paths):self.assertEqual(digest(pathlib.Path(path).read_bytes()),digest(self.data[name]))
   with self.assertRaises(OSError):
    with open(paths[1],'wb') as f:f.write(b'corrupt')
   return original(paths,expected_ca)
  def run():
   try:results.append(self.load())
   except BaseException as e:failures.append(e)
  with patch.object(m,'_build',build):
   t=threading.Thread(target=run);t.start()
   try:
    self.assertTrue(entered.wait(5))
    for name in self.data:
     path=self.root/name;path.unlink();path.write_bytes(b'replaced');path.chmod(0o600)
   finally:release.set();t.join(5)
  self.assertFalse(t.is_alive());self.assertEqual(failures,[]);self.assertEqual(len(results),1)
 def test_read_seal_build_failures_no_fd_leak(self):
  from cluster_p1 import tls_material as m
  for name in ('read','write','fcntl','_build'):
   target=m if name=='_build' else (m.fcntl if name=='fcntl' else m.os)
   before=len(os.listdir('/proc/self/fd'))
   with patch.object(target,name,side_effect=OSError('sensitive-injected-content')):
    with self.assertRaises(ValueError) as e:self.load()
   self.assertEqual(str(e.exception),'TLS material unavailable');self.assertEqual(len(os.listdir('/proc/self/fd')),before)
 def test_missing_descriptor_refuses(self):
  from cluster_p1.tls_material import load_context
  tls,raw,pin=self.args()
  for args in ((tls,None,None),(tls,raw,'0'*64)):
   with self.assertRaises(ValueError):load_context(*args)
 def test_implicit_runtime_root_not_pinned_refuses(self):
  if not self.baseline:self.skipTest('runtime does not inject roots')
  ca=x509.load_pem_x509_certificate(self.data['ca.pem']).public_bytes(serialization.Encoding.PEM)
  (self.root/'ca.pem').write_bytes(ca);self.body['ca_sha256']=digest(ca)
  with self.assertRaises(ValueError):self.load()
 def test_read_mutation_refuses(self):
  from cluster_p1 import tls_material as m
  original=os.read;changed=[]
  def read(fd,n):
   result=original(fd,n)
   if not changed and os.fstat(fd).st_ino==(self.root/'key.pem').stat().st_ino:
    changed.append(True)
    with (self.root/'key.pem').open('ab') as f:f.write(b'x')
   return result
  with patch.object(m.os,'read',read):
   with self.assertRaises(ValueError):self.load()
  self.assertEqual(changed,[True])
 def test_close_failure_does_not_return_context_and_closes_others(self):
  from cluster_p1 import tls_material as m
  before=len(os.listdir('/proc/self/fd'));close=os.close;calls=[]
  def fail(fd):
   close(fd);calls.append(fd)
   if len(calls)==1:raise OSError('sensitive-close-error')
  with patch.object(m.os,'close',fail):
   with self.assertRaises(ValueError) as caught:self.load()
  self.assertIsNone(caught.exception.__context__);self.assertEqual(len(os.listdir('/proc/self/fd')),before);self.assertGreater(len(calls),3)
 def test_directory_and_filename_refusals(self):
  self.root.chmod(0o755)
  with self.assertRaises(ValueError):self.load()
  self.root.chmod(0o700)
  for name in ('../key.pem','/key.pem','sub/key.pem'):
   self.body['private_key']=name
   with self.assertRaises(ValueError):self.load()
 def test_expiry_or_rollback_during_build_refuses(self):
  from cluster_p1 import tls_material as m
  original=m._build;start=datetime.datetime.now(datetime.timezone.utc)
  for end in (start+datetime.timedelta(days=1),start-datetime.timedelta(days=1)):
   class Clock:
    value=start
    @staticmethod
    def now(zone):return Clock.value
   def build(*a):
    result=original(*a);Clock.value=end;return result
   with patch.object(m,'datetime',Clock),patch.object(m,'_build',build):
    with self.assertRaises(ValueError):self.load()
 def test_missing_seal_capability_or_incomplete_mask_refuses(self):
  from cluster_p1 import tls_material as m
  before=len(os.listdir('/proc/self/fd'))
  with patch.object(m.os,'memfd_create',side_effect=OSError('unavailable')):
   with self.assertRaises(ValueError):self.load()
  with patch.object(m.fcntl,'fcntl',return_value=0):
   with self.assertRaises(ValueError):self.load()
  self.assertEqual(len(os.listdir('/proc/self/fd')),before)
 def test_second_source_open_failure_and_wrong_owner_refuse(self):
  from cluster_p1 import tls_material as m
  from types import SimpleNamespace
  before=len(os.listdir('/proc/self/fd'));opening=os.open;statting=os.fstat;ino=(self.root/'key.pem').stat().st_ino
  def opened(name,*a,**kw):
   if name=='key.pem':raise OSError('private path unavailable')
   return opening(name,*a,**kw)
  with patch.object(m.os,'open',opened):
   with self.assertRaises(ValueError):self.load()
  def info(fd):
   s=statting(fd)
   if s.st_ino==ino:
    d={k:getattr(s,k) for k in dir(s) if k.startswith('st_')};d['st_uid']=-1;return SimpleNamespace(**d)
   return s
  with patch.object(m.os,'fstat',info):
   with self.assertRaises(ValueError):self.load()
  self.assertEqual(len(os.listdir('/proc/self/fd')),before)
 def test_encrypted_key_never_prompts(self):
  key=ec.generate_private_key(ec.SECP256R1());(self.root/'key.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.BestAvailableEncryption(b'LAB-only')))
  with self.assertRaises(ValueError) as caught:self.load()
  self.assertIsNone(caught.exception.__context__)

if __name__=='__main__':unittest.main()
