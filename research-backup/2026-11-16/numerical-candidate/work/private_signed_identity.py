"""Private pinned-key identity adapter; no issuer is trusted by default.

Real Ed25519 verification, NOT a task execution grant or an IdP deployment.
LocalAdmission exclusively owns caching and durable LOCAL revocation. There is
no remote revocation feed or key discovery. Bootstrap must establish trust.
"""
import base64,json,re,time
from private_local_admission import IdentityAdapter,Observation,AdmissionRejected,_text

_DOMAIN=b'opencompute/local-identity/v1\0'
_FIELDS={'v','typ','iss','aud','sub','sid','iat','nbf','exp'}
def _need(ok):
    if not ok:raise AdmissionRejected()
def _decode(value):
    _need(type(value)is str and re.fullmatch('[A-Za-z0-9_-]+',value) is not None)
    raw=base64.b64decode(value+'='*(-len(value)%4),altchars=b'-_',validate=True)
    _need(base64.urlsafe_b64encode(raw).rstrip(b'=').decode()==value)
    return raw

class SignedIdentityAdapter(IdentityAdapter):
    def __init__(self,*,issuer,audience,public_key):
        _text(issuer);_text(audience)
        _need(type(public_key)is bytes and len(public_key)==32)
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        self._issuer=issuer;self._audience=audience
        self._key=Ed25519PublicKey.from_public_bytes(public_key)
    async def observe(self,credential):
        try:
            _need(type(credential)is str and 0<len(credential)<=4096)
            parts=credential.split('.');_need(len(parts)==2)
            raw,signature=map(_decode,parts);_need(len(raw)<=2048 and len(signature)==64)
            self._key.verify(signature,_DOMAIN+raw)
            def pairs(values):
                d={}
                for k,v in values:
                    _need(k not in d);d[k]=v
                return d
            d=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs)
            _need(type(d)is dict and set(d)==_FIELDS)
            _need(json.dumps(d,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()==raw)
            _need(type(d['v'])is int and d['v']==1 and d['typ']=='local-identity')
            _need(d['iss']==self._issuer and d['aud']==self._audience)
            _text(d['sub']);_text(d['sid'])
            _need(all(type(d[k])is int and 0<=d[k]<2**53 for k in ('iat','nbf','exp')))
            now=time.time()
            _need(d['iat']<=d['nbf']<=now<d['exp'] and 0<d['exp']-d['iat']<=300)
            return Observation(self._issuer,d['sub'],d['sid'],self._audience,d['exp'])
        except Exception:
            # Credential and parser/crypto details are never returned or logged.
            raise AdmissionRejected() from None
