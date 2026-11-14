"""Private unmounted ASGI review/explicit confirmation -> one-shot registration.

No listener, default login, execution permit, remote permission or C input copy.
Trusted HTTPS session mapping and policy configuration are external dependencies.
"""
import asyncio,hashlib,html,inspect,json,secrets,time
from urllib.parse import urlsplit
from private_task_consent import need,digest
from private_user_confirmation import ConfirmationApp
from private_experiment_admission import ExperimentAdmission,Confirmation

PATH='/experiments/confirm'
class _PostTrigger:
 def __init__(self):self.pending={}
 async def confirm(self,credential,receipt,body):
  key=(receipt.proposal_id,receipt.proposal_digest);expected=self.pending.pop(key,None)
  need(expected==(hashlib.sha256(credential.encode()).hexdigest(),body['owner']) and digest(body)==receipt.proposal_digest)
  return Confirmation(*key,body['owner'])

class ExperimentConfirmationApp:
 def __init__(self,path,consent,*,source_domain,origin=None,credential_for_scope=None,policy_authority=None,installation_authority=None):
  u=urlsplit(origin or '');need(u.scheme=='https' and u.netloc and not u.username and not u.password and not u.path and not u.query and not u.fragment)
  self.origin=origin;self.host=u.netloc;self._origin=origin;self._host=self.host
  self.credential_for_scope=credential_for_scope;self._resolver=credential_for_scope
  self.trigger=_PostTrigger();self._trigger=self.trigger
  self.admission=ExperimentAdmission(path,consent,source_domain=source_domain,trigger=self.trigger,policy_authority=policy_authority,installation_authority=installation_authority);self._admission=self.admission
  self.loop=asyncio.get_running_loop();self.challenges={};self.retired=False
 def _seal(self):
  self.retired=True;self.challenges.clear();self._trigger.pending.clear()
  self._admission.retired=True;self._admission.published.clear()
 def _pins(self):
  try:
   need(not self.retired and asyncio.get_running_loop() is self.loop and self.origin==self._origin and self.host==self._host)
   need(self.admission is self._admission and self.trigger is self._trigger and self.credential_for_scope is self._resolver and callable(self._resolver))
   self.admission._ready(False)
  except BaseException:self._seal();raise
 async def prepare(self,*args,**kw):
  self._pins();value=await self.admission.prepare(*args,**kw);self._pins();return value
 async def _review(self,credential,pid,sha):
  a=self.admission;c=a.consent
  async with c.authority._lock:
   self._pins();owner=c.service._download_owner(credential)
   with c._user(credential,owner):
    row=a._row(owner,pid);body=json.loads(row[0]);need(row[1]==sha and digest(body)==sha)
   self._pins();need(c.service._download_owner(credential)==owner and a._row(owner,pid)==row)
   return owner,body,row[3]
 def _form(self,raw,keys):return ConfirmationApp._form(self,raw,keys)
 def _page(self,form,body,state,challenge):
  fields=dict(form,challenge=challenge or '')
  hidden=''.join('<input type="hidden" name="'+k+'" value="'+html.escape(v,quote=True)+'">' for k,v in fields.items())
  c=body['c_input_copy'];r=body['receiver'];p=body['policy']
  page='<h1>Review this single-token experiment</h1><p>Status: '+html.escape(state)+'. Confirmation and registration do not start devices or grant remote execution.</p>'
  page+='<p>Cost cap: '+str(p['max_cost_microunits'])+' micro-units '+html.escape(p['currency'])+'; privacy: '+html.escape(p['privacy'])+'; result retention: '+str(r['retention_seconds'])+' seconds.</p>'
  if 'runtime_profile' in body:
   page+='<p>Experimental online profile: only explicitly public, non-sensitive input. Nodes can read input, activations and results. No billing or credits. Separately admitted start or commit actions may finish after cancellation. Silence is detected by lease expiry; cancellation does not prove a device stopped.</p>'
  page+='<p>Input and model are registered declarations; tokenizer/model correctness has not been verified.</p><details open><summary>Exact input, plan, roles, issuer and policy</summary><pre>'+html.escape(json.dumps(body,indent=2,ensure_ascii=True))+'</pre></details>'
  if challenge is None:return page+'<p>History only; no live confirmation or retry qualification.</p>'
  if state=='PENDING':
   page+='<form method="post" action="'+PATH+'">'+hidden+'<label><input type="checkbox" name="copy_input" value="allow-exact-snapshot" required>Allow this exact input snapshot '+html.escape(c['input_snapshot'])+' to be copied to '+html.escape(c['destination'])+' for '+html.escape(c['purpose'])+'.</label><button name="decision" value="confirm-register">Confirm this exact experiment and register once</button></form>'
  page+='<form method="post" action="'+PATH+'">'+hidden+'<button name="decision" value="cancel">Revoke this experiment authorization</button></form>'
  return page
 async def __call__(self,scope,receive,send):
  status=403;page='Experiment confirmation unavailable.';attempted=False
  try:
   self._pins();need(scope['type']=='http' and scope['scheme']=='https' and scope['path']==PATH)
   headers=scope.get('headers',[])
   def header(name):
    values=[v for k,v in headers if k.lower()==name];need(len(values)==1);return values[0].decode('ascii')
   need(header(b'host')==self.host)
   credential=self._resolver(scope)
   if inspect.isawaitable(credential):credential=await credential
   self._pins();need(type(credential)is str and 0<len(credential)<=8192)
   fingerprint=hashlib.sha256(credential.encode()).hexdigest()
   if scope['method']=='GET':
    form=self._form(scope.get('query_string',b''),{'id','digest'})
    owner,body,state=await self._review(credential,form['id'],form['digest']);self._pins()
    now=time.monotonic();self.challenges={k:v for k,v in self.challenges.items() if v[4]>now and v[0]!=form['id']};need(len(self.challenges)<64)
    challenge=None
    if self.admission.fresh and not self.admission.retired:
     challenge=secrets.token_hex(32);self.challenges[challenge]=(form['id'],form['digest'],fingerprint,owner,now+60)
    page=self._page(form,body,state,challenge);status=200
   elif scope['method']=='POST':
    need(header(b'origin')==self.origin and header(b'content-type')=='application/x-www-form-urlencoded')
    raw=b'';fragments=0
    while True:
     fragments+=1;need(fragments<=8);message=await asyncio.wait_for(receive(),10);self._pins()
     need(message['type']=='http.request');raw+=message.get('body',b'');need(len(raw)<=2048)
     if not message.get('more_body',False):break
    try:form=self._form(raw,{'id','digest','challenge','decision','copy_input'})
    except Exception:form=self._form(raw,{'id','digest','challenge','decision'})
    owner,body,state=await self._review(credential,form['id'],form['digest']);self._pins()
    # Consume before attempting confirmation or cancellation. Never restore.
    expected=self.challenges.pop(form['challenge'],None)
    need(expected is not None and expected[:4]==(form['id'],form['digest'],fingerprint,owner) and time.monotonic()<expected[4])
    if form['decision']=='cancel':
     need('copy_input' not in form);attempted=True;await self.admission.revoke(credential,body['source_task']);self._pins();page='Experiment authorization revoked. Already committed or sent data cannot be recalled.'
    else:
     need(form['decision']=='confirm-register' and form.get('copy_input')=='allow-exact-snapshot' and state=='PENDING')
     key=(form['id'],form['digest']);need(key not in self.trigger.pending);self.trigger.pending[key]=(fingerprint,owner)
     try:
      attempted=True
      await self.admission.confirm(credential,*key);self._pins()
      await self.admission.dispatch(credential,*key,self.admission._issuer);self._pins()
     finally:self._trigger.pending.pop(key,None)
     page='Explicit confirmation and one-shot registration recorded. Execution remains disabled. Use history after an uncertain response; do not retry with a new task or session.'
    status=200
  except asyncio.CancelledError:
   if attempted:self._seal()
   raise
  except Exception:pass
  await send(dict(type='http.response.start',status=status,headers=[(b'content-type',b'text/html; charset=utf-8'),(b'cache-control',b'no-store'),(b'content-security-policy',b"default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"),(b'x-content-type-options',b'nosniff')]))
  await send(dict(type='http.response.body',body=page.encode()))
