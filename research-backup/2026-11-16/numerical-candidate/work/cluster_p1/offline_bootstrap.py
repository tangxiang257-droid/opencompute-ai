"""Offline PUBLIC preparation only: no private key, filesystem, provider or grant.

Trust SHA must come from independent trusted local provisioning. Signatures are
scoped issuer assertions, not proof of physical allocation/current exclusivity or
online nonrevocation. Even complete material produces CANDIDATE_ONLY with execute
and host/provider verification FALSE. Runtime uses existing authority/resources.
"""
import base64,hashlib,re,time,ipaddress
from .online_issuance_transport import _json,_parse
from .role_launcher import launch_document

DOMAIN=b'opencompute/offline-bootstrap-evidence/v1\0'
AUDIENCE='opencompute-isolated-bootstrap-v1'
KINDS={'issuer','node','allocation'}
POOLS={'device','host','source','destination'}
RUNTIME_GAPS=['trusted_login_and_task_confirmation','current_node_identity_and_revocation',
 'live_platform_allocation_and_exclusivity','host_paths_and_permissions',
 'actual_descriptor_policy_and_capacity_requirements',
 'actual_device_completion','explicit_device_execution_authorization']
class Rejected(Exception):pass

def need(ok):
 if not ok:raise Rejected()
def exact(value,fields):need(type(value)is dict and set(value)==set(fields.split()))
def ident(v):need(type(v)is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',v) is not None)
def digest(v):need(type(v)is str and re.fullmatch('[a-f0-9]{64}',v) is not None)
def integer(v,minimum=0):need(type(v)is int and minimum<=v<2**53)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def der(value):
 need(type(value)is str and 0<len(value)<=8192)
 data=base64.b64decode(value,validate=True);need(base64.b64encode(data).decode()==value);return data

def _trust(raw,pin,now):
 need(type(raw)is bytes);digest(pin);need(sha(raw)==pin)
 t=_parse(raw);exact(t,'version audience expires_at ca_sha256 keys')
 need(type(t['version'])is int and t['version']==1 and t['audience']==AUDIENCE)
 integer(t['expires_at']);need(now<t['expires_at']);digest(t['ca_sha256'])
 need(type(t['keys'])is list and 1<=len(t['keys'])<=16);keys={}
 for key in t['keys']:
  exact(key,'id public_key kinds subjects');ident(key['id']);digest(key['public_key']);need(key['id'] not in keys)
  need(type(key['kinds'])is list and 1<=len(key['kinds'])<=3 and len(set(key['kinds']))==len(key['kinds']) and set(key['kinds'])<=KINDS)
  need(type(key['subjects'])is list and 1<=len(key['subjects'])<=16 and len(set(key['subjects']))==len(key['subjects']))
  for s in key['subjects']:ident(s)
  keys[key['id']]=key
 return t,keys

def _certificate(raw,now,*,ca=None,hostname=None):
 # Deliberately restricted direct-CA profile, NOT a general PKIX verifier.
 from cryptography import x509
 from cryptography.hazmat.primitives.asymmetric import ed25519,ed448,rsa,ec
 from cryptography.x509.oid import ExtensionOID,ExtendedKeyUsageOID
 c=x509.load_der_x509_certificate(raw);key=c.public_key()
 need(isinstance(key,(ed25519.Ed25519PublicKey,ed448.Ed448PublicKey)) or
      isinstance(key,rsa.RSAPublicKey) and key.key_size>=2048 or
      isinstance(key,ec.EllipticCurvePublicKey) and key.key_size>=256)
 need(c.not_valid_before_utc.timestamp()<=now<c.not_valid_after_utc.timestamp())
 if c.signature_hash_algorithm is not None:need(c.signature_hash_algorithm.name in ('sha256','sha384','sha512'))
 allowed={ExtensionOID.BASIC_CONSTRAINTS,ExtensionOID.KEY_USAGE,ExtensionOID.EXTENDED_KEY_USAGE,
          ExtensionOID.SUBJECT_ALTERNATIVE_NAME,ExtensionOID.SUBJECT_KEY_IDENTIFIER,ExtensionOID.AUTHORITY_KEY_IDENTIFIER}
 need(all(e.oid in allowed for e in c.extensions))
 basic=c.extensions.get_extension_for_class(x509.BasicConstraints).value
 usage=c.extensions.get_extension_for_class(x509.KeyUsage).value
 if ca is None:
  need(basic.ca and usage.key_cert_sign)
  need(all(e.oid!=ExtensionOID.EXTENDED_KEY_USAGE for e in c.extensions))
  c.verify_directly_issued_by(c)
 else:
  need(not basic.ca and usage.digital_signature and not usage.key_cert_sign)
  c.verify_directly_issued_by(ca)
  eku=c.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
  need(ExtendedKeyUsageOID.CLIENT_AUTH in eku and ExtendedKeyUsageOID.SERVER_AUTH in eku)
  names=c.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName)
  need(hostname in names)
 return c

def _manifest(raw,t,now):
 m=_parse(raw);exact(m,'version experiment task owner plan_digest model_digest expires_at issuer coordinator_node ca_der nodes roles')
 need(type(m['version'])is int and m['version']==1)
 for k in ('experiment','task','owner','coordinator_node'):ident(m[k])
 for k in ('plan_digest','model_digest'):digest(m[k])
 integer(m['expires_at']);need(now<m['expires_at']<=t['expires_at'])
 issuer=m['issuer'];exact(issuer,'node issuer audience public_key')
 for k in ('node','issuer','audience'):ident(issuer[k])
 digest(issuer['public_key'])
 ca_raw=der(m['ca_der']);need(sha(ca_raw)==t['ca_sha256']);ca=_certificate(ca_raw,now);cert_deadline=ca.not_valid_after_utc.timestamp()
 need(type(m['nodes'])is list and 4<=len(m['nodes'])<=5);nodes={};fingerprints=set()
 for n in m['nodes']:
  exact(n,'node hostname certificate_der');ident(n['node'])
  need(type(n['hostname'])is str and re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?',n['hostname']) is not None and '..' not in n['hostname'])
  try:ipaddress.ip_address(n['hostname'])
  except ValueError:pass
  else:raise Rejected()
  raw=der(n['certificate_der']);cert=_certificate(raw,now,ca=ca,hostname=n['hostname']);fp=sha(raw)
  cert_deadline=min(cert_deadline,cert.not_valid_after_utc.timestamp())
  need(n['node'] not in nodes and fp not in fingerprints);nodes[n['node']]=dict(n,certificate_sha256=fp);fingerprints.add(fp)
 need(type(m['roles'])is dict and set(m['roles'])=={'A','B','C'});candidates={};device_ids=set();role_nodes=set()
 for role,p in m['roles'].items():
  exact(p,'node root descriptor_sha256 charge control_budget device device_id instance resource_id demands')
  for k in ('node','device','device_id','instance','resource_id'):ident(p[k])
  need(p['node'] in nodes and p['node'] not in role_nodes);role_nodes.add(p['node']);digest(p['descriptor_sha256'])
  if role in ('A','B'):
   need(re.fullmatch(r'cuda:(0|[1-9][0-9]*)',p['device']) is not None and p['device_id'] not in device_ids);device_ids.add(p['device_id'])
  else:need(p['device']=='storage')
  exact(p['demands'],'device host source destination')
  for pool,v in p['demands'].items():integer(v,0 if role=='C' and pool=='device' else 1)
  need(role!='C' or p['demands']['device']==0)
  root=p['root'];need(type(root)is str and root.startswith('/') and '\\' not in root and '\x00' not in root and all(x not in ('.','..','') for x in root.split('/')[1:]))
  cfg=dict(version=1,role=role,node=p['node'],issuer_node=issuer['node'],issuer_certificate=nodes[issuer['node']]['certificate_sha256'],coordinator_node=m['coordinator_node'],descriptor_sha256=p['descriptor_sha256'],root=root,charge=p['charge'],control_budget=p['control_budget'])
  raw=_json(cfg);launch_document(raw,sha(raw));candidates[role]=dict(document=cfg,sha256=sha(raw))
 need(set(nodes)==role_nodes|{issuer['node'],m['coordinator_node']})
 return m,nodes,candidates,cert_deadline

def _proofs(raw,keys,manifest_sha,now):
 from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
 values=_parse(raw);need(type(values)is list and len(values)<=16);proofs={};deadline=2**53
 for envelope in values:
  exact(envelope,'key_id statement signature');ident(envelope['key_id']);need(envelope['key_id'] in keys)
  key=keys[envelope['key_id']];s=envelope['statement']
  exact(s,'version audience kind subject manifest_sha256 issued_at expires_at claims')
  need(type(s['version'])is int and s['version']==1 and s['audience']==AUDIENCE and s['kind'] in key['kinds'])
  ident(s['subject']);need(s['subject'] in key['subjects']);digest(s['manifest_sha256']);need(s['manifest_sha256']==manifest_sha)
  integer(s['issued_at']);integer(s['expires_at']);need(s['issued_at']<=now<s['expires_at'] and 0<s['expires_at']-s['issued_at']<=300)
  sig=envelope['signature'];need(type(sig)is str and re.fullmatch('[a-f0-9]{128}',sig) is not None)
  Ed25519PublicKey.from_public_bytes(bytes.fromhex(key['public_key'])).verify(bytes.fromhex(sig),DOMAIN+_json(s))
  pair=(s['kind'],s['subject']);need(pair not in proofs);proofs[pair]=s['claims'];deadline=min(deadline,s['expires_at'])
 return proofs,deadline

def assess(manifest_raw,trust_raw,expected_trust_sha256,evidence_raw):
 """Return bounded non-echoing diagnostics; never writes files or instantiates providers."""
 result=dict(version=1,status='REFUSED',execution_allowed=False,host_validated=False,
             physical_allocation_verified=False,revocation_checked=False,candidates={},
             missing=[],invalid=[],runtime_required=list(RUNTIME_GAPS))
 try:
  now=time.time();need(0<=now<2**53)
  if trust_raw is None or expected_trust_sha256 is None:result['missing'].append('independently_pinned_trust')
  if manifest_raw is None:result['missing'].append('public_manifest')
  if evidence_raw is None:result['missing'].append('signed_evidence')
  if result['missing']:return result
  try:t,keys=_trust(trust_raw,expected_trust_sha256,now)
  except Exception:result['invalid'].append('trust_bundle');return result
  try:m,nodes,candidates,cert_deadline=_manifest(manifest_raw,t,now)
  except Exception:result['invalid'].append('public_manifest');return result
  try:proofs,deadline=_proofs(evidence_raw,keys,sha(manifest_raw),now)
  except Exception:result['invalid'].append('signed_evidence');return result
  expected={('issuer',m['issuer']['issuer']):dict(m['issuer'])}
  for node,n in nodes.items():expected[('node',node)]=dict(node=node,hostname=n['hostname'],certificate_sha256=n['certificate_sha256'])
  for role,p in m['roles'].items():
   pair=('allocation',p['resource_id']);need(pair not in expected)
   expected[pair]=None
   if pair not in proofs:result['missing'].append('allocation_'+role);continue
   c=proofs[pair]
   try:
    exact(c,'node device device_id instance resource_id capacity isolation proof_id lease_id')
    for k in ('node','device','device_id','instance','resource_id'):need(c[k]==p[k])
    ident(c['proof_id']);ident(c['lease_id']);exact(c['capacity'],'device host source destination')
    for pool in POOLS:integer(c['capacity'][pool]);need(c['capacity'][pool]>=p['demands'][pool])
    need(c['isolation']==('reserved-storage' if role=='C' else 'exclusive-device'))
   except Exception:result['invalid'].append('allocation_'+role)
  for pair,claim in expected.items():
   if pair[0]=='allocation':continue
   label='issuer' if pair[0]=='issuer' else 'node_configuration'
   if pair not in proofs:result['missing'].append(label)
   elif _json(proofs[pair])!=_json(claim):result['invalid'].append(label)
  if set(proofs)-set(expected):result['invalid'].append('unexpected_evidence')
  if result['missing'] or result['invalid']:return result
  deadline=min(deadline,t['expires_at'],m['expires_at'],cert_deadline)
  need(now<=time.time()<deadline)
  result.update(status='CANDIDATE_ONLY',candidates=candidates,manifest_sha256=sha(manifest_raw),trust_sha256=expected_trust_sha256,evidence_sha256=sha(evidence_raw),valid_until=deadline)
  return result
 except Exception:
  result['candidates']={};result['invalid'].append('public_input');return result
