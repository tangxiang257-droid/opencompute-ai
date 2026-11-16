"""Explicit co-located C download route; no second ledger or execution grant.

The caller supplies a dedicated download policy. This borrows the original
IMPORTED C ledger and live experiment binding; restart/expiry cannot restore it.
"""
import json
from contextlib import contextmanager, ExitStack
from private_c_receiving_authority import CReceivingPolicy
from private_experiment_admission import need
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing

class LocalCResultRoute:
 @classmethod
 def install(cls, service, receiving, *, authorize=None, enabled=False):
  need(enabled is True and type(receiving) is CReceivingPolicy and callable(getattr(authorize,'hold',None)))
  e=receiving._experiment_current();need(service is e.importer.admission.consent.service)
  self=cls();self.service=service;self.receiving=receiving;self.e=e;self.authorize=authorize
  self.pins=(service,receiving,e,authorize,authorize.hold,e.importer.artifacts)
  self.task=e.body['execution_task'];self.owner=e.owner
  routes=getattr(service,'_c_result_routes',None)
  if routes is None:routes={};service._c_result_routes=routes
  need(self.task not in routes);self.routes=routes
  # Validate before installing; installation creates no persistent authority.
  self.manifest=self._current(e.credential, installed=False)
  routes[self.task]=self
  return self
 def _current(self, credential, *, installed=True):
  need((self.service,self.receiving,self.e,self.authorize,self.authorize.hold,self.e.importer.artifacts)==self.pins)
  need(self.receiving._experiment_current() is self.e)
  need(self.service._download_owner(credential)==self.owner)
  need(self.task==self.e.body['execution_task'] and self.owner==self.e.owner)
  need('export' in self.e.body['policy']['actions'])
  if installed:need(self.service._c_result_routes is self.routes and self.routes.get(self.task) is self)
  # _check validates the original complete slot, issuer, source and path pins;
  # use non-retiring form so an unrelated rejected reader cannot retire C.
  self.e._check()
  task,_=self.e.importer.artifacts._metadata(self.owner,self.task)
  need(task.state=='RESULT_STORED')
  rows=self.e.importer.artifacts._db.execute("SELECT digest FROM artifact_tickets WHERE owner_id=? AND task_id=? AND state='STORED'",(self.owner,self.task)).fetchall()
  need(len(rows)==1)
  if installed:need(rows[0][0]==self.manifest)
  return rows[0][0]
 @contextmanager
 def _hold(self, credential):
  e=self.e;c=e.importer.admission.consent
  with c._user(credential,self.owner),c.fence.lock,ExitStack() as stack:
   self._current(credential)
   for provider in (e.importer.identity,e.importer.capacity):
    nothing(stack.enter_context(_non_suppressing(provider.hold(json.loads(json.dumps(e.request)),'c-input-import'))))
   context=dict(version=1,purpose='result-download',owner=self.owner,task=self.task,context=e.body,context_digest=e.sha,import_binding=e.request)
   nothing(stack.enter_context(_non_suppressing(self.authorize.hold(json.loads(json.dumps(context)),'result-download'))))
   self._current(credential);yield;self._current(credential)
  self._current(credential)
 def final(self, credential, digest):
  with self._hold(credential):need(self._current(credential)==digest)
  need(self._current(credential)==digest)
 async def fetch(self, credential, sink, *, metadata, max_bytes):
  c=self.e.importer.admission.consent
  async with c.authority._lock:
   with self._hold(credential):
    before=self._current(credential)
    def checked(callback,*args):
     self._current(credential);callback(*args);self._current(credential)
    value=self.e.importer.artifacts.fetch(self.owner,self.task,lambda *args:checked(sink,*args),metadata=lambda value:checked(metadata,value),max_bytes=max_bytes)
    need(value==before)
   self.final(credential,value)
   return value
