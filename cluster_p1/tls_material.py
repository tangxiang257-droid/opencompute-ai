"""Private Linux snapshot loader. Trust pins come from local bootstrap, not files.

No network, credential issuance, provider discovery or permission. All source
files live in one owned private directory; same-UID/root process compromise is
outside this boundary. Sealed memfd prevents source-path rereads by OpenSSL.
Python buffers/OpenSSL memory are not guaranteed erasable or hardware protected.
"""
import fcntl,hashlib,json,os,re,ssl,stat,sys
from datetime import datetime,timezone

_FIELDS={'version','directory','certificate','private_key','ca','certificate_sha256','ca_sha256','leaf_sha256'}
_LIMIT=131072
def _need(ok):
 if not ok:raise ValueError('TLS material unavailable')
def _state(s):return (s.st_dev,s.st_ino,s.st_mode,s.st_uid,s.st_nlink,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
def _open_directory(path,owned):
 _need(type(path)is str and path.startswith('/') and len(path)<=4096)
 parts=path.split('/')[1:];_need(0<len(parts)<=64 and all(p and p not in ('.','..') for p in parts))
 fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC);owned.append(fd)
 for part in parts:
  fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd);owned.append(fd)
 s=os.fstat(fd);_need(s.st_uid==os.geteuid() and stat.S_ISDIR(s.st_mode) and stat.S_IMODE(s.st_mode)==0o700)
 return fd
def _read(directory,name,owned):
 _need(type(name)is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',name) is not None)
 fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=directory);owned.append(fd)
 before=os.fstat(fd)
 _need(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and stat.S_IMODE(before.st_mode)==0o600 and before.st_nlink==1 and 0<before.st_size<=_LIMIT)
 raw=bytearray()
 while len(raw)<=_LIMIT:
  chunk=os.read(fd,min(16384,_LIMIT+1-len(raw)))
  if not chunk:break
  raw.extend(chunk)
 _need(len(raw)==before.st_size and _state(before)==_state(os.fstat(fd)))
 return bytes(raw)
def _sealed(raw,owned):
 _need(sys.platform=='linux')
 fd=os.memfd_create('tls-snapshot',os.MFD_CLOEXEC|os.MFD_ALLOW_SEALING);owned.append(fd);os.fchmod(fd,0o600)
 offset=0
 while offset<len(raw):
  n=os.write(fd,memoryview(raw)[offset:]);_need(n>0);offset+=n
 # Linux UAPI linux/fcntl.h: some Python builds omit these constant exports.
 # This still requires actual kernel seals; no alternate unsealed mechanism.
 seals=0x000f
 fcntl.fcntl(fd,1033,seals);_need(fcntl.fcntl(fd,1034)&seals==seals)
 return '/proc/self/fd/'+str(fd)
def _no_password():raise ValueError('TLS material unavailable')
def _build(paths,expected_ca,*,server=False):
 # Explicit context avoids system roots and environment-driven key logging.
 ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT);ctx.minimum_version=ssl.TLSVersion.TLSv1_2
 ctx.verify_mode=ssl.CERT_REQUIRED;ctx.check_hostname=not server
 ctx.load_verify_locations(cafile=paths[2]);ctx.load_cert_chain(paths[0],paths[1],password=_no_password)
 _need(ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname== (not server) and ctx.keylog_filename is None)
 _need(set(ctx.get_ca_certs(binary_form=True))==expected_ca)
 return ctx
def load_context(tls,raw,pin,*,server=False,hostname=None):
 """Synchronous bounded local load; return only after all owned FDs close.

Pins are exact cert PEM/bundle and CA PEM bytes plus first leaf certificate DER
SHA256, never SPKI. Missing capabilities or cleanup errors deny, without exposing
underlying exceptions, paths, key data or password prompts. No source fallback.
 """
 owned=[];failed=False;ctx=None
 try:
  from .channel import TLSConfig
  from cryptography import x509
  from cryptography.hazmat.primitives.serialization import Encoding
  from cryptography.x509.oid import ExtendedKeyUsageOID
  _need(type(server)is bool)
  if server:_need(type(hostname)is str and re.fullmatch(r'[a-z][a-z0-9-]*(?:\.[a-z0-9][a-z0-9-]*)+',hostname) is not None and len(hostname)<=253)
  else:_need(hostname is None)
  _need(type(tls)is TLSConfig and type(raw)is bytes and 0<len(raw)<=16384 and type(pin)is str and hashlib.sha256(raw).hexdigest()==pin)
  d=json.loads(raw);_need(type(d)is dict and set(d)==_FIELDS and type(d['version'])is int and d['version']==1)
  _need(json.dumps(d,sort_keys=True,separators=(',',':')).encode()==raw)
  for k in ('certificate_sha256','ca_sha256','leaf_sha256'):_need(type(d[k])is str and re.fullmatch('[a-f0-9]{64}',d[k]) is not None)
  names=[d[k] for k in ('certificate','private_key','ca')];_need(len(set(names))==3)
  _need((tls.certfile,tls.keyfile,tls.cafile)==tuple(d['directory']+'/'+n for n in names))
  directory=_open_directory(d['directory'],owned);directory_state=_state(os.fstat(directory))
  certificate,key,ca=(_read(directory,n,owned) for n in names)
  _need(_state(os.fstat(directory))==directory_state)
  _need(hashlib.sha256(certificate).hexdigest()==d['certificate_sha256'] and hashlib.sha256(ca).hexdigest()==d['ca_sha256'])
  _need(certificate.lstrip().startswith(b'-----BEGIN CERTIFICATE-----'))
  leaf=x509.load_pem_x509_certificate(certificate);now=datetime.now(timezone.utc)
  _need(hashlib.sha256(leaf.public_bytes(Encoding.DER)).hexdigest()==d['leaf_sha256'])
  _need(leaf.not_valid_before_utc<=now<leaf.not_valid_after_utc)
  _need(not leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca)
  _need((ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH) in leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value)
  if server:_need(hostname in leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName))
  expected_ca={c.public_bytes(Encoding.DER) for c in x509.load_pem_x509_certificates(ca)};_need(bool(expected_ca))
  paths=[_sealed(b,owned) for b in (certificate,key,ca)];ctx=_build(paths,expected_ca,server=True) if server else _build(paths,expected_ca)
 except BaseException:failed=True
 finally:
  for fd in reversed(owned):
   try:os.close(fd)
   except BaseException:failed=True
 # Outside handlers: don't retain a sensitive underlying exception chain.
 if not failed:
  try:_need(now<=datetime.now(timezone.utc)<leaf.not_valid_after_utc)
  except BaseException:failed=True
 if failed:raise ValueError('TLS material unavailable')
 return ctx
