"""Local ephemeral LAB certificates only; no handshake or network."""
import datetime,ssl,unittest
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
from validation.tls_material.test_material import Material,digest
class Server(Material):
 def setUp(self):
  super().setUp();self.replace_leaf()
 def replace_leaf(self,server=True):
  key=ec.generate_private_key(ec.SECP256R1());ca_key=ec.generate_private_key(ec.SECP256R1());now=datetime.datetime.now(datetime.timezone.utc);name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'issuer.example')])
  def build(pub,ca):
   b=x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(pub).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1)).not_valid_after(now+datetime.timedelta(hours=1)).add_extension(x509.BasicConstraints(ca=ca,path_length=None),critical=True)
   if not ca:b=b.add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]),critical=False).add_extension(x509.SubjectAlternativeName([x509.DNSName('issuer.example')]),critical=False)
   return b.sign(ca_key,hashes.SHA256())
  leaf=build(key.public_key(),False);ca=build(ca_key.public_key(),True)
  data={'cert.pem':leaf.public_bytes(serialization.Encoding.PEM),'key.pem':key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'ca.pem':ca.public_bytes(serialization.Encoding.PEM)+b''.join(ssl.DER_cert_to_PEM_cert(b).encode() for b in self.baseline)}
  for n,b in data.items():(self.root/n).write_bytes(b)
  self.body.update(certificate_sha256=digest(data['cert.pem']),ca_sha256=digest(data['ca.pem']),leaf_sha256=digest(leaf.public_bytes(serialization.Encoding.DER)))
 def test_server_context(self):
  from cluster_p1.tls_material import load_context
  c=load_context(*self.args(),server=True,hostname='issuer.example')
  self.assertEqual(c.verify_mode,ssl.CERT_REQUIRED);self.assertFalse(c.check_hostname);self.assertIsNone(c.keylog_filename)
 def test_wrong_name_eku_and_client_remain_closed(self):
  from cluster_p1.tls_material import load_context
  for name in (None,'wrong.example','*.example','127.0.0.1'):
   with self.assertRaises(ValueError):load_context(*self.args(),server=True,hostname=name)
  with self.assertRaises(ValueError):load_context(*self.args())
  self.replace_leaf(False)
  with self.assertRaises(ValueError):load_context(*self.args(),server=True,hostname='issuer.example')
def load_tests(loader,tests,pattern):return unittest.TestSuite(Server(n) for n in ('test_server_context','test_wrong_name_eku_and_client_remain_closed'))
if __name__=='__main__':unittest.main()
