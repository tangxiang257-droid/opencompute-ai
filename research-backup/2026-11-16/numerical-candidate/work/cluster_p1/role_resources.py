"""Fresh role-local logical budgets. External authority/capacity remain mandatory.
No physical allocation, device stop, refunds, distributed atomicity or clone protection.
"""
from contextlib import contextmanager
from dataclasses import dataclass,asdict
import json,os,stat,sqlite3,threading
from pathlib import Path
from .online_issuance import _descriptor,_intent,_DESCRIPTOR
from .projection_driver import Spec,Policy,nothing,integer
from .local_attempt import _non_suppressing
from .store import Conflict,canonical
from .control_journal import _binding,_identifier

_POOLS={'device','host','source','destination'}
def need(ok):
    if not ok:raise Conflict('role resource promise rejected')
@dataclass(frozen=True,eq=False,slots=True)
class Ticket:
    reservation_id:str

class RoleResources:
    def __init__(self,path,*,node,pool_limits,extra_host_bytes,authority=None,capacity=None):
        _identifier(node);need(integer(extra_host_bytes));self.extra_host_bytes=extra_host_bytes;need(type(pool_limits)is dict and set(pool_limits)==_POOLS and all(integer(v) for v in pool_limits.values()))
        self.node=node;self.limits=dict(pool_limits);self.authority=authority;self.capacity=capacity
        self.path=Path(path).absolute();self.pid=os.getpid();self.lock=threading.RLock();self.closed=False;self.tickets={};self.poisoned=set()
        try:fd=os.open(self.path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd);self.fresh=True
        except FileExistsError:self.fresh=False
        st=self.path.lstat();need(stat.S_ISREG(st.st_mode));self.inode=(st.st_dev,st.st_ino)
        self.db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,isolation_level=None,check_same_thread=False)
        self.db.execute('PRAGMA synchronous=FULL')
        try:
            with self.tx() as c:
                if self.fresh:
                    c.execute('CREATE TABLE config(node TEXT,limits TEXT,extra_host INTEGER)');c.execute('INSERT INTO config VALUES(?,?,?)',(node,canonical(self.limits),extra_host_bytes))
                    c.execute('CREATE TABLE promises(rid TEXT PRIMARY KEY,stage_key TEXT UNIQUE NOT NULL,descriptor TEXT,spec TEXT,policy TEXT,charge INTEGER,demand TEXT,state TEXT)')
                    c.execute('CREATE TABLE controls(parent TEXT PRIMARY KEY,rid TEXT UNIQUE,binding TEXT,charge INTEGER,state TEXT)')
                need(c.execute('SELECT node,limits,extra_host FROM config').fetchall()==[(node,canonical(self.limits),extra_host_bytes)])
        except BaseException:self.close();raise
    def check(self,live=False):
        need(not self.closed and self.pid==os.getpid());s=self.path.lstat();need(stat.S_ISREG(s.st_mode) and (s.st_dev,s.st_ino)==self.inode)
        if live:need(self.fresh)
    def ready(self):
        self.check(True);need(callable(getattr(self.authority,'hold',None)) and callable(getattr(self.capacity,'hold',None)))
    def _commit(self,c):c.commit()
    @contextmanager
    def tx(self):
        self.check()
        with self.lock:
            self.check();self.db.execute('BEGIN IMMEDIATE')
            try:yield self.db;self._commit(self.db)
            except BaseException:self.db.rollback();raise
    def _row(self,rid):
        row=self.db.execute('SELECT descriptor,spec,policy,charge,demand,state FROM promises WHERE rid=?',(rid,)).fetchone();need(row is not None);return row
    def _validate(self,d,s):
        need(type(s)is Spec and len(s.inputs)==1);s.__post_init__();decl=d['declaration'];session=decl['session']
        need(d['node']==self.node and d['resource_id'])
        need((s.owner,s.request,s.session,s.epoch,s.model,s.layout,s.plan_digest,s.hidden,s.vocab)==
             (d['owner'],d['task'],d['nonce'],d['epoch'],session['model_hash'],session['layout_hash'],decl['plan_digest'],decl['plan']['hidden_size'],decl['plan']['vocab_size']))
        need(all(getattr(s,k)==v for k,v in d['source_binding'].items()))
        if d['stage']==0:need(s.inputs==(s.token_digest,))
    @contextmanager
    def _external(self,d,demands,action):
        self.ready()
        with _non_suppressing(self.authority.hold(json.loads(canonical(d)),action)) as a,_non_suppressing(self.capacity.hold(json.loads(canonical(d)),dict(demands))) as c:
            nothing(a);nothing(c);yield
    def _unknown(self,rid):
        self.poisoned.add(rid)
        with self.tx() as c:
            self.check(True);c.execute("UPDATE promises SET state='UNKNOWN' WHERE rid=?",(rid,));c.execute("UPDATE controls SET state='UNKNOWN' WHERE parent=?",(rid,))
    def reserve(self,rid,descriptor,spec,policy,*,charge):
        self.ready();_identifier(rid);need(rid not in self.poisoned);d=_descriptor(descriptor);need(d['resource_id']==rid and integer(charge) and type(policy)is Policy)
        self._validate(d,spec);demands=policy.requirements(spec);demands['host']+=self.extra_host_bytes
        need(all(integer(v) for v in demands.values()))
        inserted=False
        try:
            with self.tx() as c:
                self.check(True);need(rid not in self.poisoned);need(c.execute('SELECT 1 FROM promises WHERE rid=?',(rid,)).fetchone() is None)
                need(c.execute('SELECT 1 FROM controls WHERE rid=?',(rid,)).fetchone() is None)
                used={k:0 for k in _POOLS}
                for (raw,) in c.execute('SELECT demand FROM promises'):
                    for k,v in json.loads(raw).items():used[k]+=v
                need(all(used[k]+demands[k]<=self.limits[k] for k in _POOLS))
                c.execute("INSERT INTO promises VALUES(?,?,?,?,?,?,?,'UNKNOWN')",(rid,canonical([d['owner'],d['task'],d['stage']]),canonical(d),canonical(asdict(spec)),canonical(asdict(policy)),charge,canonical(demands)));inserted=True
            with self._external(d,demands,'reserve'):pass
            with self.tx() as c:
                need(rid not in self.poisoned)
                c.execute("UPDATE promises SET state='HELD' WHERE rid=? AND state='UNKNOWN'",(rid,))
            need(rid not in self.poisoned)
            ticket=Ticket(rid);self.tickets[id(ticket)]=ticket;return ticket
        except BaseException:
            if inserted:self._unknown(rid)
            raise
    @contextmanager
    def hold(self,rid,binding,charge=None):
        self.ready();_identifier(rid)
        with self.lock:
            if charge is None:
                row=self._row(rid);d=json.loads(row[0])
                incoming=_descriptor(binding) if type(binding)is dict and set(binding)==_DESCRIPTOR else {k:v for k,v in _intent(binding).items() if k in _DESCRIPTOR}
                need(canonical(incoming)==row[0])
            else:
                need(integer(charge))
                m=self.db.execute('SELECT parent,binding,charge,state FROM controls WHERE rid=?',(rid,)).fetchone();need(m is not None and m[1:]==(_binding(binding),charge,'HELD'))
                rid=m[0];row=self._row(rid);d=json.loads(row[0])
            need(row[5]=='HELD' and rid not in self.poisoned);demands=json.loads(row[4])
        try:
            with self._external(d,demands,'hold'):
                with self.lock:need(self._row(rid)[5]=='HELD' and rid not in self.poisoned)
                yield
            with self.lock:need(self._row(rid)[5]=='HELD' and rid not in self.poisoned)
        except BaseException:self._unknown(rid);raise
    def bind_control(self,parent,rid,binding,charge):
        self.ready();_identifier(rid);need(integer(charge));raw=_binding(binding);b=json.loads(raw)
        with self.lock:
            row=self._row(parent);d=json.loads(row[0]);s=json.loads(row[1]);need(row[5]=='HELD' and parent not in self.poisoned and charge==row[3])
            expected=dict(owner=d['owner'],task=d['task'],session=d['nonce'],epoch=d['epoch'],stage=d['stage'],device=d['device'],instance=d['instance'],declaration_digest=d['declaration_digest'],model=s['model'],layout=s['layout'],plan=s['plan_digest'],**d['source_binding'])
            need(all(b[k]==v for k,v in expected.items()))
            if d['stage']==0:need(b['input_digest']==s['token_digest'])
        inserted=False
        try:
            with self.tx() as c:
                need(c.execute('SELECT 1 FROM controls WHERE parent=? OR rid=?',(parent,rid)).fetchone() is None)
                need(c.execute('SELECT 1 FROM promises WHERE rid=?',(rid,)).fetchone() is None)
                c.execute("INSERT INTO controls VALUES(?,?,?,?,'UNKNOWN')",(parent,rid,raw,charge));inserted=True
            with self.hold(parent,d):pass
            with self.tx() as c:
                need(self._row(parent)[5]=='HELD' and parent not in self.poisoned);c.execute("UPDATE controls SET state='HELD' WHERE parent=?",(parent,))
        except BaseException:
            if inserted:self._unknown(parent)
            raise
    @contextmanager
    def hold_control(self,parent,rid,binding,charge):
        self.ready();need(integer(charge))
        with self.lock:
            self.ready();m=self.db.execute('SELECT rid,binding,charge,state FROM controls WHERE parent=?',(parent,)).fetchone();need(m==(rid,_binding(binding),charge,'HELD'))
        with self.hold(rid,binding,charge):yield
    def verify_stage(self,spec,ticket,stage):
        self.ready();need(type(stage)is int and stage in (0,1));need(type(ticket)is Ticket and self.tickets.get(id(ticket)) is ticket)
        with self.lock:
            row=self._row(ticket.reservation_id);d=json.loads(row[0]);self._validate(d,spec);need(d['stage']==stage)
            baseline=json.loads(row[1]);actual=asdict(spec);actual['inputs']=list(actual['inputs']);baseline.pop('inputs');actual.pop('inputs');need(actual==baseline)
            mapping=self.db.execute('SELECT binding,state FROM controls WHERE parent=?',(ticket.reservation_id,)).fetchone()
            if mapping is not None:need(mapping[1]=='HELD' and spec.inputs==(json.loads(mapping[0])['input_digest'],))
        with self.hold(ticket.reservation_id,d):pass
    def device_capacity(self,ticket):
        """View of an existing promise only; not an allocation or permission."""
        self.ready()
        need(type(ticket)is Ticket and self.tickets.get(id(ticket)) is ticket)
        return _DeviceCapacity(self,ticket)
    def quarantine(self,spec,ticket,scope):
        self.check(True);need(type(ticket)is Ticket and self.tickets.get(id(ticket)) is ticket);self._validate(json.loads(self._row(ticket.reservation_id)[0]),spec);self._unknown(ticket.reservation_id)
    def query(self,rid):
        self.check()
        with self.lock:
            self.check();row=self.db.execute('SELECT state,demand,charge FROM promises WHERE rid=?',(rid,)).fetchone()
            return None if row is None else dict(state=row[0],demand=json.loads(row[1]),charge=row[2],execution_allowed=False,physical_capacity_verified=False)
    def close(self):
        need(self.pid==os.getpid())
        with self.lock:
            if not self.closed:self.closed=True;self.db.close()

class _DeviceCapacity:
    """Private exact-ticket view; real exclusive capacity remains external.

    The backend hold_device fences the SAME descriptor/resource ID, never creates
    a lease and never releases it on exit. No identity or execution grant is made.
    """
    def __init__(self,service,ticket):self.service=service;self.ticket=ticket;self.rid=ticket.reservation_id
    def _snapshot(self,spec,device,stage):
        r=self.service;t=self.ticket;r.ready()
        need(type(t)is Ticket and r.tickets.get(id(t)) is t and t.reservation_id==self.rid)
        need(type(stage)is int and stage in (0,1))
        with r.lock:
            row=r._row(t.reservation_id);d=json.loads(row[0]);r._validate(d,spec)
            need(row[5]=='HELD' and t.reservation_id not in r.poisoned)
            need(d['device']==device and d['stage']==stage)
            baseline=json.loads(row[1]);actual=asdict(spec);actual['inputs']=list(actual['inputs'])
            baseline.pop('inputs');actual.pop('inputs');need(actual==baseline)
            mapping=r.db.execute('SELECT rid,binding,charge,state FROM controls WHERE parent=?',(t.reservation_id,)).fetchone()
            need(mapping is not None and mapping[3]=='HELD' and mapping[2]==row[3])
            b=json.loads(mapping[1]);need(_binding(b)==mapping[1])
            expected=dict(owner=d['owner'],task=d['task'],session=d['nonce'],epoch=d['epoch'],stage=d['stage'],device=d['device'],instance=d['instance'],declaration_digest=d['declaration_digest'],model=spec.model,layout=spec.layout,plan=spec.plan_digest,**d['source_binding'])
            need(all(b[k]==v for k,v in expected.items()))
            need(spec.inputs==(b['input_digest'],))
            return row,mapping
    @contextmanager
    def hold_device(self,spec,device,stage,action):
        r=self.service;t=self.ticket
        # Authenticate the original local handle before any failure can poison it.
        need(type(t)is Ticket and r.tickets.get(id(t)) is t and t.reservation_id==self.rid)
        try:
            r.check(True)
            need(action in ('open','load','execute','materialize','finish','inspect-finished'))
            original=self._snapshot(spec,device,stage);row,mapping=original
            d=json.loads(row[0]);demands=json.loads(row[4])
            need(callable(getattr(r.capacity,'hold_device',None)))
            with r.hold(t.reservation_id,d):
                with _non_suppressing(r.capacity.hold_device(json.loads(canonical(d)),dict(demands),action)) as held:
                    nothing(held);need(self._snapshot(spec,device,stage)==original)
                    yield None
                    need(self._snapshot(spec,device,stage)==original)
                need(self._snapshot(spec,device,stage)==original)
            need(self._snapshot(spec,device,stage)==original)
        except BaseException as primary:
            try:r._unknown(t.reservation_id)
            except BaseException as cleanup:raise BaseExceptionGroup('device promise retirement uncertain',[primary,cleanup])
            raise
