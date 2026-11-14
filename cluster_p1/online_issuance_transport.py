"""Private original-connection online issuance over supplied bounded Channels.

No sockets, launcher, models, transferable execution permits or query-to-install
conversion. Each endpoint sees its own asymmetric ChannelIdentity. Fresh random
challenges bind a live exchange; they do not attest a boot or authenticate a role.
"""
from contextlib import contextmanager
import hashlib
import secrets
import json
import math
from .control_exchange import _need, _nothing
from .history_query import _HistoryEndpoint, _context


def _json(value):
    # Complete Task declarations contain finite numeric cost/deadline fractions.
    # Existing command codec intentionally excludes floats; leave it unchanged.
    nodes=0
    def check(v,depth=0):
        nonlocal nodes
        nodes+=1;_need(nodes<=2048 and depth<=8)
        if v is None or type(v)is bool:return
        if type(v)is int:_need(-(2**63)<=v<2**63);return
        if type(v)is float:_need(math.isfinite(v) and abs(v)<=1e15);return
        if type(v)is str:_need(len(v)<=8192);return
        if type(v)is list:
            _need(len(v)<=128)
            for x in v:check(x,depth+1)
            return
        _need(type(v)is dict and len(v)<=64)
        for k,x in v.items():
            _need(type(k)is str and 0<len(k)<=128);check(x,depth+1)
    check(value)
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    _need(len(raw)<=32768);return raw


def _parse(raw):
    _need(type(raw)is bytes and 0<len(raw)<=32768)
    def pairs(items):
        result={}
        for k,v in items:_need(k not in result);result[k]=v
        return result
    value=json.loads(raw,object_pairs_hook=pairs)
    _need(_json(value)==raw);return value


def _hex(value):
    _need(type(value)is str and len(value)==64 and all(c in '0123456789abcdef' for c in value))


def _copy(value):return _parse(_json(value))


def _hash(value):return hashlib.sha256(_json(value)).hexdigest()


class _IssuanceEndpoint(_HistoryEndpoint):
    def __init__(self,channel,scope,*,identity=None,authorize=None,resources=None):
        super().__init__(channel)
        _need(type(scope)is dict and set(scope)=={'task','session','epoch','stage','model'})
        _need(type(scope['epoch'])is int and scope['epoch']==1 and type(scope['stage'])is int and scope['stage'] in (0,1))
        _hex(scope['model']);self._scope=_copy(scope)
        self.identity=identity;self.authorize=authorize;self.resources=resources
        self._attempted=False;self._principal=None
        _need(not getattr(channel,'_online_issuance_claimed',False) and not channel._start_attempted)
        self.ready();channel._single_start=True;channel._online_issuance_claimed=True
    def ready(self):
        self.current();_need(not self._closed)
        for provider in (self.identity,self.authorize,self.resources):
            _need(callable(getattr(provider,'hold',None)))
    @contextmanager
    def fence(self,binding,action,*,expected_principal=None):
        self.ready()
        with self.channel.identity_fence(self._identity),_context(self.identity.hold(self._identity)) as principal:
            _need(type(principal)is str and 0<len(principal)<=128)
            if expected_principal is not None:_need(principal==expected_principal)
            if self._principal is None:self._principal=principal
            else:_need(principal==self._principal)
            with _context(self.authorize.hold(principal,self._identity,_copy(binding),action)) as allowed:
                _nothing(allowed);yield principal
    async def send(self,body,sequence):
        self.current()
        await self.channel.send(self.envelope(self._scope,sequence,_json(body)),max_frame_bytes=65536)
    async def receive(self,sequence):
        delivery=await self.channel.receive()
        try:
            self.current();self.validate(delivery.envelope,self._scope,sequence)
            body=_parse(delivery.envelope.payload)
            # No authorization scopes are held while returning credit.
            await delivery.release();self.current()
            return body
        finally:delivery.envelope=None
    def once(self):
        _need(not self._attempted);self._attempted=True;self.ready()

# Storage/factory integration below is deliberately explicit; no duck-typed
# receipt or fallback to the local coordinator may supply installation eligibility.


def _scope(descriptor):
    return dict(task=descriptor['task'],session=descriptor['nonce'],epoch=descriptor['epoch'],
                stage=descriptor['stage'],model=descriptor['declaration']['task']['model_hash'])


def _challenge(role,issuer,incarnation,descriptor,issuer_identity):
    _hex(role);_hex(issuer)
    return _hash(dict(domain='opencompute-online-issuance-v1',role_challenge=role,
        issuer_challenge=issuer,issuer_incarnation=incarnation,descriptor_digest=_hash(descriptor),issuer_identity=issuer_identity))


class IssuanceClient(_IssuanceEndpoint):
    """Role-local original connection and store, never a remote authority object.

    install returns only the storage module's live local handle. Subsequent local
    fixed factories consume it once; a response/history dict is never a factory
    input. completion.check is bootstrap readiness, NOT device-work completion.
    """
    def __init__(self,channel,store,descriptor,*,issuer_node,issuer_certificate,identity=None,authorize=None,resources=None,completion=None):
        from .online_issuance import RoleInstallStore
        _need(type(store)is RoleInstallStore);store.ready()
        _need(completion is not None and all(callable(getattr(completion,n,None)) for n in ('check','begin','finish','quarantine')))
        _hex(issuer_certificate);_need(type(issuer_node)is str and 0<len(issuer_node)<=128)
        self._issuer_identity=dict(node_id=issuer_node,certificate_sha256=issuer_certificate)
        self._store=store;self._descriptor=_copy(descriptor);self._completion=completion;self._pending=None;self._verified_delivery=None
        super().__init__(channel,_scope(descriptor),identity=identity,authorize=authorize,resources=resources)
        _need((self._identity.node_id,self._identity.certificate_sha256)==(issuer_node,issuer_certificate))
    @contextmanager
    def admission(self,action):
        d=self._descriptor
        with self.fence(d,action) as principal,_context(self.resources.hold(d['resource_id'],_copy(d))) as allowed:
            _nothing(allowed);_nothing(self._completion.check(_copy(d)));yield principal
    def live_guard(self):
        with self.admission('live-installation'):pass
    async def install(self):
        try:
            self.once()
            with self.admission('begin-install'):pass
            role=secrets.token_hex(32)
            await self.channel.start()
            hello=await self.receive(0)
            d=self._descriptor
            _need(type(hello)is dict and set(hello)=={'version','kind','issuer_challenge','issuer_incarnation','descriptor_digest','issuer_identity'}
                  and type(hello['version'])is int and hello['version']==1 and hello['kind']=='hello'
                  and _json(hello['issuer_identity'])==_json(self._issuer_identity)
                  and hello['issuer_incarnation']==d['issuer_incarnation'] and hello['descriptor_digest']==_hash(d))
            _hex(hello['issuer_challenge'])
            challenge=_challenge(role,hello['issuer_challenge'],hello['issuer_incarnation'],d,self._issuer_identity)
            with self.admission('pending'):
                self._pending=self._store.pending(d,self.channel,challenge=challenge)
                intent=_copy(self._pending.request)
            request=dict(version=1,kind='issue',role_challenge=role,issuer_challenge=hello['issuer_challenge'],
                         issuer_incarnation=hello['issuer_incarnation'],descriptor_digest=_hash(d),intent=intent)
            with self.admission('issue-send'):pass
            await self.send(request,0)
            reply=await self.receive(1)
            _need(type(reply)is dict and set(reply)=={'version','kind','reply_to','role_challenge','issuer_challenge','intent','slot_digest','issuer_incarnation','execution_allowed'}
                  and type(reply['version'])is int and reply['version']==1 and reply['kind']=='issued'
                  and reply['reply_to']==_hash(request) and reply['role_challenge']==role
                  and reply['issuer_challenge']==hello['issuer_challenge'])
            response={k:reply[k] for k in ('version','intent','slot_digest','issuer_incarnation','execution_allowed')}
            _need(type(response)is dict and set(response)=={'version','intent','slot_digest','issuer_incarnation','execution_allowed'}
                  and type(response['version'])is int and response['version']==1 and response['execution_allowed'] is False
                  and _json(response['intent'])==_json(intent) and response['slot_digest']==_hash(intent)
                  and response['issuer_incarnation']==d['issuer_incarnation'])
            # receive has released credit already. Current provider/connection
            # scopes cover the actual synchronous durable acceptance transaction.
            with self.admission('accept'):
                self._verified_delivery=(self._pending,_json(response))
                try:return self._store.accept(self._pending,response,self.channel,self)
                finally:self._verified_delivery=None
        except BaseException as error:await self._failed(error)


class IssuanceService(_IssuanceEndpoint):
    """Issuer-local authority/registration and one endpoint; no role store/factory."""
    def __init__(self,channel,authority,registration,descriptor,*,issuer_node,issuer_certificate,identity=None,authorize=None,resources=None):
        from .online_issuance import IssuanceAuthority
        _need(type(authority)is IssuanceAuthority);authority.ready()
        _hex(issuer_certificate);_need(type(issuer_node)is str and 0<len(issuer_node)<=128)
        self._issuer_identity=dict(node_id=issuer_node,certificate_sha256=issuer_certificate)
        self._authority=authority;self._registration=registration;self._descriptor=_copy(descriptor)
        super().__init__(channel,_scope(descriptor),identity=identity,authorize=authorize,resources=resources)
        _need(self._identity.node_id==descriptor['node'])
    @contextmanager
    def admission(self,action):
        d=self._descriptor
        with self.fence(d,action) as principal,_context(self.resources.hold(d['resource_id'],_copy(d))) as allowed:
            _nothing(allowed);yield principal
    async def serve_once(self):
        try:
            self.once();d=self._descriptor
            with self.admission('hello'):
                _need(self._authority.incarnation==d['issuer_incarnation'])
                issuer=secrets.token_hex(32)
            hello=dict(version=1,kind='hello',issuer_challenge=issuer,issuer_incarnation=self._authority.incarnation,descriptor_digest=_hash(d),issuer_identity=self._issuer_identity)
            await self.channel.start()
            with self.admission('hello-send'):pass
            await self.send(hello,0)
            request=await self.receive(0)
            _need(type(request)is dict and set(request)=={'version','kind','role_challenge','issuer_challenge','issuer_incarnation','descriptor_digest','intent'}
                  and type(request['version'])is int and request['version']==1 and request['kind']=='issue'
                  and request['issuer_challenge']==issuer and request['issuer_incarnation']==self._authority.incarnation
                  and request['descriptor_digest']==_hash(d))
            intent=request['intent'];_need(type(intent)is dict)
            _need(intent.get('challenge')==_challenge(request['role_challenge'],issuer,self._authority.incarnation,d,self._issuer_identity))
            peer=intent.get('issuer_peer');_need(type(peer)is dict)
            _need(all(peer.get(k)==v for k,v in self._issuer_identity.items()))
            _need(all(k in intent and _json(intent[k])==_json(v) for k,v in d.items()))
            with self.admission('claim'):
                response=self._authority.claim_stage(self._registration,_copy(intent),self._identity)
            with self.admission('issued-send'):pass
            reply=dict(response,kind='issued',reply_to=_hash(request),role_challenge=request['role_challenge'],issuer_challenge=issuer)
            await self.send(reply,1)
            # Ordinary status only, no role qualification or factory callback.
            return _copy(response)
        except BaseException as error:await self._failed(error)
