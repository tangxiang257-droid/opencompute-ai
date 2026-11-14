"""Private ASGI confirmation entry; never mounted or listening by default.

A trusted HTTPS host must supply authenticated USER-session -> credential mapping.
No headers are interpreted as credentials and no default/test identity exists.
One explicit same-origin POST consumes a reviewed challenge, not a portable grant.
"""
import asyncio,hashlib,html,inspect,json,secrets
from urllib.parse import parse_qsl,urlsplit
from private_task_consent import TaskConsent,Confirmation,need,digest
from cluster_p1.store import canonical

class _PostTrigger:
 def __init__(self):self.pending={}
 async def confirm(self,credential,receipt,body):
  key=(receipt.proposal_id,receipt.proposal_digest)
  expected=self.pending.pop(key,None)
  need(expected==(hashlib.sha256(credential.encode()).hexdigest(),body['owner']))
  return Confirmation(*key,body['owner'])

class ConfirmationApp:
 def __init__(self,reader,*,identity_fence=None,origin=None,credential_for_scope=None):
  u=urlsplit(origin or '');need(u.scheme=='https' and u.netloc and not u.username and not u.password and not u.path and not u.query and not u.fragment)
  self.origin=origin;self.host=u.netloc;self.credential_for_scope=credential_for_scope;self._resolver=credential_for_scope
  self.trigger=_PostTrigger();self.consent=TaskConsent(reader,identity_fence=identity_fence,trigger=self.trigger)
  self.challenges={};self.loop=asyncio.get_running_loop()
 def _form(self,raw,keys):
  need(type(raw)is bytes and len(raw)<=2048)
  pairs=parse_qsl(raw.decode('ascii'),keep_blank_values=True,strict_parsing=True)
  need(len(pairs)==len(keys) and {k for k,v in pairs}==keys)
  result=dict(pairs);need(all(type(v)is str and 0<len(v)<=128 for v in result.values()));return result
 async def _review(self,credential,pid,sha):
  c=self.consent
  async with c.authority._lock:
   ledger=c._current();owner=c.service._download_owner(credential)
   with c._user(credential,owner):
    row=c._row(ledger,owner,pid);body=json.loads(row[2])
    need(row[3]==sha and digest(body)==sha and 'receiver' in body)
   c._current();need(c.service._download_owner(credential)==owner);need(c._row(ledger,owner,pid)==row)
   return owner,body,row[5]
 async def __call__(self,scope,receive,send):
  status=403;page='Confirmation unavailable.'
  try:
   need(asyncio.get_running_loop() is self.loop and scope['type']=='http' and scope['scheme']=='https' and scope['path']=='/confirm')
   need(self.credential_for_scope is self._resolver and callable(self._resolver))
   headers=scope.get('headers',[])
   def header(name):
    v=[v for k,v in headers if k.lower()==name];need(len(v)==1);return v[0].decode('ascii')
   need(header(b'host')==self.host)
   credential=self._resolver(scope)
   if inspect.isawaitable(credential):credential=await credential
   need(type(credential)is str and 0<len(credential)<=8192)
   fingerprint=hashlib.sha256(credential.encode()).hexdigest()
   if scope['method']=='GET':
    form=self._form(scope.get('query_string',b''),{'id','digest'})
    owner,body,state=await self._review(credential,form['id'],form['digest'])
    self.challenges={k:v for k,v in self.challenges.items() if v[0]!=form['id']}
    need(len(self.challenges)<64)
    challenge=secrets.token_hex(32);self.challenges[challenge]=(form['id'],form['digest'],fingerprint,owner)
    fields=dict(form,challenge=challenge)
    hidden=''.join('<input type="hidden" name="'+k+'" value="'+html.escape(v,quote=True)+'">' for k,v in fields.items())
    buttons=('<button name="decision" value="confirm">Confirm this exact request</button>' if state=='PENDING' else '')+'<button name="decision" value="cancel">Revoke this task authorization</button>'
    page='<p>Status: '+html.escape(state)+'</p><h1>Confirm this task and result recipient</h1><p>Review the full plan, cost limit, privacy, expiry and recipient below. Stored input alone does not authorize execution.</p><pre>'+html.escape(json.dumps(body,indent=2,ensure_ascii=True))+'</pre><form method="post" action="/confirm">'+hidden+buttons+'</form>'
    status=200
   elif scope['method']=='POST':
    need(header(b'origin')==self.origin and header(b'content-type')=='application/x-www-form-urlencoded')
    raw=b'';fragments=0
    while True:
     fragments+=1;need(fragments<=8)
     message=await asyncio.wait_for(receive(),10);need(message['type']=='http.request');raw+=message.get('body',b'');need(len(raw)<=2048)
     if not message.get('more_body',False):break
    form=self._form(raw,{'id','digest','challenge','decision'})
    owner,body,state=await self._review(credential,form['id'],form['digest'])
    # Consume before confirmation/cancel; uncertain outcomes never restore it.
    expected=self.challenges.pop(form['challenge'],None)
    need(expected==(form['id'],form['digest'],fingerprint,owner))
    if form['decision']=='cancel':
     await self.consent.revoke(credential,body['task']);page='Task authorization revoked.'
    else:
     need(form['decision']=='confirm' and state=='PENDING');key=(form['id'],form['digest']);need(key not in self.trigger.pending)
     self.trigger.pending[key]=(fingerprint,owner)
     try:await self.consent.confirm(credential,*key)
     finally:self.trigger.pending.pop(key,None)
     page='Explicit confirmation recorded. This is not an execution or remote retry permit.'
    status=200
  except Exception:pass
  await send(dict(type='http.response.start',status=status,headers=[(b'content-type',b'text/html; charset=utf-8'),(b'cache-control',b'no-store'),(b'content-security-policy',b"default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"),(b'x-content-type-options',b'nosniff')]))
  await send(dict(type='http.response.body',body=page.encode()))
