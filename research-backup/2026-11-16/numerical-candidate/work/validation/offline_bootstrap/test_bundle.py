"""LAB-only ephemeral RAM keys and certificates, never serialized private keys.

Real cryptographic mechanisms, synthetic issuer/platform assertions. Passing
these tests cannot establish that any real issuer/platform was authorized.
"""
import unittest,hashlib,base64,time,json,tempfile
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
from cryptography import x509
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
try:from cluster_p1.offline_bootstrap import assess,DOMAIN,AUDIENCE
except ImportError:assess=None
from cluster_p1.online_issuance_transport import _json
from cluster_p1.role_launcher import LaunchConfig

def pub(key):return key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw).hex()
def hashed(raw):return hashlib.sha256(raw).hexdigest()
def cert(key,name,ca_key,ca_name,now,is_ca=False):
 subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])
 issuer=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,ca_name)])
 b=x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(datetime.fromtimestamp(now-60,timezone.utc)).not_valid_after(datetime.fromtimestamp(now+3600,timezone.utc))
 b=b.add_extension(x509.BasicConstraints(ca=is_ca,path_length=0 if is_ca else None),critical=True).add_extension(x509.KeyUsage(True,False,False,False,False,is_ca,is_ca,None,None),critical=True)
 if not is_ca:b=b.add_extension(x509.SubjectAlternativeName([x509.DNSName(name)]),critical=False).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH,ExtendedKeyUsageOID.SERVER_AUTH]),critical=False)
 return b.sign(ca_key,None).public_bytes(Encoding.DER)
class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(assess,'offline public configuration tool missing')
  self.now=int(time.time());self.keys={k:Ed25519PrivateKey.generate() for k in ('issuer','node','allocation')}
  ca=Ed25519PrivateKey.generate();ca_der=cert(ca,'LAB CA',ca,'LAB CA',self.now,True)
  nodes=[]
  for n in ('authority','a','b','c'):
   raw=cert(Ed25519PrivateKey.generate(),n+'.lab.invalid',ca,'LAB CA',self.now)
   nodes.append(dict(node=n,hostname=n+'.lab.invalid',certificate_der=base64.b64encode(raw).decode()))
  self.m=dict(version=1,experiment='LAB-one',task='LAB-task',owner='LAB-owner',plan_digest='a'*64,model_digest='b'*64,expires_at=self.now+180,
   issuer=dict(node='authority',issuer='LAB-user-issuer',audience='LAB-user-audience',public_key=pub(Ed25519PrivateKey.generate())),coordinator_node='authority',ca_der=base64.b64encode(ca_der).decode(),nodes=nodes,roles={})
  for role in ('A','B','C'):
   self.m['roles'][role]=dict(node=role.lower(),root='/LAB-nonexistent-'+role,descriptor_sha256=role.lower()*64,charge=32,control_budget=64,device='storage' if role=='C' else 'cuda:0',device_id='volume-C' if role=='C' else 'GPU-'+role,instance='instance-'+role,resource_id='resource-'+role,demands=dict(device=0 if role=='C' else 128,host=256,source=512,destination=512))
  self.trust=dict(version=1,audience=AUDIENCE,expires_at=self.now+600,ca_sha256=hashed(ca_der),keys=[dict(id=k,public_key=pub(self.keys[k]),kinds=[k],subjects=['LAB-user-issuer'] if k=='issuer' else (['authority','a','b','c'] if k=='node' else ['resource-A','resource-B','resource-C'])) for k in self.keys])
  self.reset_proofs()
 def reset_proofs(self):
  self.claims=[('issuer','LAB-user-issuer',dict(self.m['issuer']))]
  self.claims += [('node',n['node'],dict(node=n['node'],hostname=n['hostname'],certificate_sha256=hashed(base64.b64decode(n['certificate_der'])))) for n in self.m['nodes']]
  for role,p in self.m['roles'].items():
   c={k:p[k] for k in ('node','device','device_id','instance','resource_id')};c.update(capacity=dict(p['demands']),isolation='reserved-storage' if role=='C' else 'exclusive-device',proof_id='LAB-proof-'+role,lease_id='LAB-lease-'+role)
   self.claims.append(('allocation',p['resource_id'],c))
 def evidence(self,*,domain=DOMAIN):
  out=[]
  for kind,subject,claims in self.claims:
   s=dict(version=1,audience=AUDIENCE,kind=kind,subject=subject,manifest_sha256=hashed(_json(self.m)),issued_at=self.now-1,expires_at=self.now+120,claims=claims)
   out.append(dict(key_id=kind,statement=s,signature=self.keys[kind].sign(domain+_json(s)).hex()))
  return out
 def run_case(self,evidence=None,trust=None,pin=None,manifest=None):
  t=_json(self.trust) if trust is None else trust
  return assess(_json(self.m) if manifest is None else manifest,t,hashed(t) if pin is None else pin,_json(self.evidence() if evidence is None else evidence))
 def denied(self,r):self.assertEqual(r['status'],'REFUSED');self.assertFalse(r['execution_allowed']);self.assertEqual(r['candidates'],{})
 def test_missing_material_refuses_without_echo(self):
  r=assess(b'{}',None,None,None);self.denied(r);self.assertIn('independently_pinned_trust',r['missing'])
 def test_complete_signatures_produce_only_inert_launch_documents(self):
  r=self.run_case();self.assertEqual(r['status'],'CANDIDATE_ONLY');self.assertEqual(set(r['candidates']),{'A','B','C'})
  for k in ('execution_allowed','host_validated','physical_allocation_verified','revocation_checked'):self.assertIs(r[k],False)
  self.assertEqual(r['candidates']['B']['document']['descriptor_sha256'],'b'*64)
  with self.assertRaises(FileNotFoundError):LaunchConfig(_json(r['candidates']['B']['document']),r['candidates']['B']['sha256'])
 def test_candidate_reuses_real_LaunchConfig_schema(self):
  with tempfile.TemporaryDirectory() as root:
   for p in self.m['roles'].values():p['root']=root
   r=self.run_case();self.assertEqual(r['status'],'CANDIDATE_ONLY')
   for candidate in r['candidates'].values():LaunchConfig(_json(candidate['document']),candidate['sha256'])
   self.assertEqual(list(Path(root).iterdir()),[])
 def test_independent_trust_pin_required(self):self.denied(self.run_case(pin='0'*64))
 def test_no_claims_or_flags_can_replace_signatures(self):
  self.denied(self.run_case(evidence=[]))
  self.m['verified']=True;self.denied(self.run_case())
 def test_missing_issuer_node_allocation_reports_no_candidates(self):
  e=self.evidence()
  for kind in ('issuer','node','allocation'):
   r=self.run_case(evidence=[p for p in e if p['statement']['kind']!=kind]);self.denied(r);self.assertTrue(r['missing'])
 def test_false_exclusive_or_missing_proof_rejects(self):
  for val in (False,'shared-device'):
   self.reset_proofs();self.claims[-3][2]['isolation']=val;self.denied(self.run_case())
  self.reset_proofs();del self.claims[-3][2]['proof_id'];self.denied(self.run_case())
 def test_capacity_below_demand_or_wrong_device_rejects(self):
  self.claims[-3][2]['capacity']['device']=127;self.denied(self.run_case())
  self.reset_proofs();self.claims[-3][2]['device_id']='GPU-other';self.denied(self.run_case())
 def test_issuer_cannot_sign_platform_assertion(self):
  e=self.evidence();p=e[-1];p['key_id']='issuer';p['signature']=self.keys['issuer'].sign(DOMAIN+_json(p['statement'])).hex()
  self.denied(self.run_case(evidence=e))
 def test_wrong_subject_or_signature_rejects(self):
  e=self.evidence();e[-1]['statement']['subject']='resource-X';e[-1]['signature']=self.keys['allocation'].sign(DOMAIN+_json(e[-1]['statement'])).hex();self.denied(self.run_case(evidence=e))
  e=self.evidence();e[0]['signature']='0'*128;self.denied(self.run_case(evidence=e))
 def test_identity_domain_cannot_be_replayed(self):self.denied(self.run_case(evidence=self.evidence(domain=b'opencompute/local-identity/v1\0')))
 def test_manifest_substitution_invalidates_evidence(self):
  e=self.evidence();self.m['experiment']='LAB-two';self.denied(self.run_case(evidence=e))
 def test_replay_of_exact_material_never_authorizes_execution(self):
  e=self.evidence();a=self.run_case(evidence=e);b=self.run_case(evidence=e)
  self.assertEqual(a,b);self.assertFalse(b['execution_allowed'])
 def test_expired_future_or_overlong_evidence_rejects(self):
  for begin,end in ((self.now-10,self.now),(self.now+1,self.now+60),(self.now-1,self.now+400)):
   e=self.evidence();s=e[0]['statement'];s['issued_at']=begin;s['expires_at']=end;e[0]['signature']=self.keys['issuer'].sign(DOMAIN+_json(s)).hex();self.denied(self.run_case(evidence=e))
 def test_expired_trust_or_manifest_rejects(self):
  self.trust['expires_at']=self.now;self.denied(self.run_case());self.trust['expires_at']=self.now+600
  self.m['expires_at']=self.now;self.denied(self.run_case())
 def test_expiry_during_verification_rejects(self):
  with patch('cluster_p1.offline_bootstrap.time.time',side_effect=[self.now,self.now+121]):self.denied(self.run_case())
 def test_duplicate_proof_and_unknown_key_rejects(self):
  e=self.evidence();self.denied(self.run_case(evidence=e+[e[0]]));e[0]['key_id']='other';self.denied(self.run_case(evidence=e))
 def test_node_hostname_or_cert_substitution_rejects(self):
  self.m['nodes'][1]['hostname']='wrong.lab.invalid';self.denied(self.run_case())
 def test_same_cert_two_nodes_rejects(self):
  self.m['nodes'][1]['certificate_der']=self.m['nodes'][2]['certificate_der'];self.m['nodes'][1]['hostname']=self.m['nodes'][2]['hostname'];self.denied(self.run_case())
 def test_wrong_CA_pin_rejects(self):self.trust['ca_sha256']='0'*64;self.denied(self.run_case())
 def test_private_input_is_never_echoed(self):
  marker='-----BEGIN PRIVATE KEY-----SECRET-LAB-MARKER'
  self.m['private_key']=marker;r=self.run_case();self.denied(r);self.assertNotIn(marker,json.dumps(r))
  self.denied(self.run_case(manifest=marker.encode()))
 def test_duplicate_keys_and_oversize_denied(self):
  self.denied(self.run_case(manifest=b'{"version":1,"version":1}'));self.denied(self.run_case(manifest=b'x'*32769))
 def test_path_escape_and_boolean_budget_reject(self):
  self.m['roles']['A']['root']='/tmp/../escape';self.denied(self.run_case())
  self.m['roles']['A']['root']='/root-A';self.m['roles']['A']['charge']=True;self.denied(self.run_case())
 def test_pure_assessment_does_not_touch_host_or_claim(self):
  with patch.object(Path,'lstat',side_effect=AssertionError('host read')),patch.object(LaunchConfig,'claim',side_effect=AssertionError('claim')):
   self.assertEqual(self.run_case()['status'],'CANDIDATE_ONLY')
 def short_certificates(self):
  ca=Ed25519PrivateKey.generate();raw=cert(ca,'LAB CA',ca,'LAB CA',self.now,True)
  self.m['ca_der']=base64.b64encode(raw).decode();self.trust['ca_sha256']=hashed(raw)
  for node in self.m['nodes']:
   raw=cert(Ed25519PrivateKey.generate(),node['hostname'],ca,'LAB CA',self.now-3599)
   node['certificate_der']=base64.b64encode(raw).decode()
  self.reset_proofs()
 def test_valid_until_includes_certificate_expiry(self):
  self.short_certificates()
  with patch('cluster_p1.offline_bootstrap.time.time',return_value=self.now):r=self.run_case()
  self.assertEqual(r['status'],'CANDIDATE_ONLY');self.assertLessEqual(r['valid_until'],self.now+1)
 def test_certificate_expiring_during_validation_refuses(self):
  self.short_certificates()
  with patch('cluster_p1.offline_bootstrap.time.time',side_effect=[self.now,self.now+2]):self.denied(self.run_case())
 def test_observed_clock_rollback_refuses(self):
  with patch('cluster_p1.offline_bootstrap.time.time',side_effect=[self.now,self.now-2]):self.denied(self.run_case())
 def test_ip_literal_hostname_refused(self):
  self.m['nodes'][0]['hostname']='127.0.0.1';self.denied(self.run_case())
if __name__=='__main__':unittest.main()
