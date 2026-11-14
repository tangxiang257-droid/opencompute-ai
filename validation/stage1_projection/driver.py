"""Isolated single-token stage-1 seam. NO real authority, device or sink adapter.

All dependencies default to missing/deny. Ordinary test doubles cannot establish
production permission, numerical correctness or device completion.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass,field
import hashlib,inspect,json,os,re,sqlite3,struct,threading,uuid
from cluster_p1.local_attempt import _non_suppressing

class Denied(ValueError):pass

def need(ok):
    if not ok:raise Denied('stage1 seam denied')

def canonical(x):return json.dumps(x,sort_keys=True,separators=(',',':'))
def synchronous(value):
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value) and inspect.getcoroutinestate(value)==inspect.CORO_CREATED:value.close()
        raise Denied('synchronous seam; awaitable is not completion evidence')
    return value

def nothing(value):need(synchronous(value) is None)

def integer(n):return type(n)is int and 0<n<2**63

@dataclass(frozen=True)
class Spec:
    owner:str
    request:str
    session:str
    epoch:int
    model:str
    layout:str
    input_snapshot:str
    preprocessing:str
    plan_digest:str
    token_digest:str
    hidden:int
    vocab:int
    inputs:tuple
    def __post_init__(self):
        for value in (self.owner,self.request,self.session):need(type(value)is str and re.fullmatch('[A-Za-z0-9_-]{1,64}',value) is not None)
        need(integer(self.epoch) and self.epoch<2**31)
        need(integer(self.hidden) and self.hidden*4<=65536 and integer(self.vocab))
        need(type(self.inputs)is tuple and 1<=len(self.inputs)<=3)
        for value in (self.model,self.layout,self.input_snapshot,self.preprocessing,self.plan_digest,self.token_digest,*self.inputs):
            need(type(value)is str and re.fullmatch('[a-f0-9]{64}',value) is not None)
        need(self.output_bytes+32768<=2097152)
    @property
    def output_bytes(self):return len(self.inputs)*self.vocab*4
    @property
    def digest(self):
        self.__post_init__()
        return hashlib.sha256(canonical({'stage':1,'batch':1,'dtype':'float32-le','axes':['token','vocab'],'object_token_counts':[1]*len(self.inputs),'spec':asdict(self)}).encode()).hexdigest()

@dataclass(frozen=True)
class Policy:
    resident:int
    kv_old_new:int
    workspace:int
    device_capacity:int
    host_capacity:int
    source_capacity:int
    destination_capacity:int
    def requirements(self,spec):
        need(type(spec)is Spec);spec.__post_init__()
        need(all(integer(v) for v in asdict(self).values()))
        need(self.source_capacity<=2097152 and self.destination_capacity<=2097152)
        block=min(spec.vocab,8192)*4
        demand={'device':self.resident+self.kv_old_new+self.workspace+spec.hidden*8+block,
                'host':spec.hidden*4+block*3+32768,
                'source':spec.output_bytes+32768,'destination':spec.output_bytes+32768}
        for name in demand:need(demand[name]<=getattr(self,name+'_capacity'))
        return demand

class Journal:
    """Fresh-only LOCAL stage qualification; no reopen/resume/global replay claim."""
    def __init__(self,path,spec):
        need(type(spec)is Spec);raw=spec.digest
        fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
        self.lock=threading.RLock();self.db=sqlite3.connect(path,isolation_level=None,check_same_thread=False)
        with self.tx() as c:
            c.execute('CREATE TABLE stage(spec TEXT NOT NULL,instance TEXT,next INTEGER NOT NULL,retired INTEGER NOT NULL)')
            c.execute('INSERT INTO stage VALUES(?,NULL,0,0)',(raw,))
            c.execute('CREATE TABLE tokens(token INTEGER PRIMARY KEY,input TEXT NOT NULL,state TEXT NOT NULL,output TEXT)')
    @contextmanager
    def tx(self):
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:yield self.db;self.db.commit()
            except BaseException:self.db.rollback();raise
    def bind(self,spec,instance):
        with self.tx() as c:
            need(c.execute('UPDATE stage SET instance=? WHERE instance IS NULL AND spec=? AND retired=0',(instance,spec.digest)).rowcount==1)
    def check(self,spec,instance,token):
        with self.lock:row=self.db.execute('SELECT spec,instance,next,retired FROM stage').fetchone()
        need(row==(spec.digest,instance,token,0))
    def admit(self,spec,instance,token,digest):
        with self.tx() as c:
            need(c.execute('SELECT spec,instance,next,retired FROM stage').fetchone()==(spec.digest,instance,token,0))
            need(c.execute('SELECT 1 FROM tokens WHERE token=?',(token,)).fetchone() is None)
            c.execute("INSERT INTO tokens VALUES(?,?,'ADMITTED',NULL)",(token,digest))
    def commit(self,spec,instance,token,digest):
        with self.tx() as c:
            need(c.execute('SELECT spec,instance,next,retired FROM stage').fetchone()==(spec.digest,instance,token,0))
            need(c.execute("UPDATE tokens SET state='COMMITTED',output=? WHERE token=? AND input=? AND state='ADMITTED'",(digest,token,spec.inputs[token])).rowcount==1)
            need(c.execute('UPDATE stage SET next=? WHERE next=? AND retired=0',(token+1,token)).rowcount==1)
    def committed(self,spec,instance,token,digest):
        self.check(spec,instance,token+1)
        with self.lock:row=self.db.execute('SELECT state,output FROM tokens WHERE token=?',(token,)).fetchone()
        need(row==('COMMITTED',digest))
    def retire(self):
        with self.tx() as c:c.execute('UPDATE stage SET retired=1')
    def retired(self):
        with self.lock:return bool(self.db.execute('SELECT retired FROM stage').fetchone()[0])
    def close(self):self.db.close()

@dataclass(frozen=True)
class Result:
    token:int
    output_digest:str
    next_token:int
    execution_allowed:bool=field(default=False,init=False)
    device_completion_verified:bool=field(default=False,init=False)

class Driver:
    """Dependency method contracts are documented in README, NOT authentication.

    reserve_request and work scopes have no release path here. Failure cleanup
    transfers ownership to quarantine adapters, never asserts physical stopping.
    """
    def __init__(self,journal,spec,policy,*,authority=None,resources=None,backend=None,completion=None,sink=None):
        need(type(journal)is Journal and type(spec)is Spec and type(policy)is Policy)
        policy.requirements(spec)
        self.journal=journal;self.spec=spec;self.policy=policy;self.instance=uuid.uuid4().hex
        journal.bind(spec,self.instance)
        self.authority=authority;self.resources=resources;self.backend=backend;self.completion=completion;self.sink=sink
        self._stop=threading.Event();self._lock=threading.Lock();self._thread=None
        self._next=0;self._ticket=None;self._reservation_started=False;self._scope=None;self._backend_touched=False;self._admitted=False
    def retire(self):self._stop.set();self.journal.retire()
    def check(self,token):
        need(not self._stop.is_set());self.journal.check(self.spec,self.instance,token)
    def run(self,token,value):
        if not self._lock.acquire(blocking=False):self.retire();raise Denied('busy stage retired')
        try:
            self.check(self._next)
            need(type(token)is int and token==self._next and 0<=token<len(self.spec.inputs))
            need(all(x is not None for x in (self.authority,self.resources,self.backend,self.completion,self.sink)))
            if self._thread is None:self._thread=threading.get_ident()
            need(self._thread==threading.get_ident())
            need(type(value)is bytes and len(value)==self.spec.hidden*4 and hashlib.sha256(value).hexdigest()==self.spec.inputs[token])
            need(all((x[0]&0x7f800000)!=0x7f800000 for x in struct.iter_unpack('<I',value)))
            self.policy.requirements(self.spec)
            with _non_suppressing(synchronous(self.authority.hold_call(self.spec,token))) as declared:
                nothing(declared)  # never accept a bool or synthetic Permit
                self.check(token)
                if not self._reservation_started:
                    self._reservation_started=True
                    self._ticket=synchronous(self.resources.reserve_request(self.spec,self.policy))
                    need(self._ticket is not None and type(self._ticket)is not bool)
                nothing(self.resources.verify_token(self.spec,self._ticket,token))
                self.check(token);self.journal.admit(self.spec,self.instance,token,self.spec.inputs[token]);self._admitted=True
                self._scope=uuid.uuid4().hex
                nothing(self.completion.open_scope(self.spec,token,self._scope))
                self.check(token);self._backend_touched=True;nothing(self.backend.check_binding(self.spec))
                handle=synchronous(self.backend.prepare_token(self._scope,value,token))
                self.check(token);need(handle is not None and type(handle)is not bool)
                digest=hashlib.sha256()
                for column in range(0,self.spec.vocab,8192):
                    self.check(token);count=min(8192,self.spec.vocab-column)
                    pending=synchronous(self.backend.project_slice(self._scope,handle,column,count))
                    self.check(token)
                    block=synchronous(self.completion.materialize(self._scope,pending,count*4))
                    need(type(block)is bytes and len(block)==count*4)
                    need(all((x[0]&0x7f800000)!=0x7f800000 for x in struct.iter_unpack('<I',block)))
                    self.check(token);nothing(self.sink.append(token,column,block));digest.update(block)
                    del block,pending
                self.check(token);nothing(self.completion.finish_scope(self._scope))
                self.check(token);nothing(self.sink.seal(token,digest.hexdigest()))
                self.check(token);nothing(self.backend.install_cache(self._scope,handle));self.check(token)
            self.check(token)
            attempts=0;ack=False;active=True
            def commit():
                nonlocal attempts,ack
                attempts+=1
                if not active or threading.get_ident()!=self._thread or attempts!=1:
                    self.retire();raise Denied('finalize callback ownership')
                self.check(token)
                self.journal.commit(self.spec,self.instance,token,digest.hexdigest());ack=True
            try:nothing(self.authority.finalize(self.spec,token,digest.hexdigest(),commit))
            finally:active=False
            need(attempts==1 and ack);self.journal.committed(self.spec,self.instance,token,digest.hexdigest())
            need(not self._stop.is_set())
            self._next+=1;self._scope=None;self._admitted=False
            return Result(token,digest.hexdigest(),self._next)
        except BaseException as error:
            cleanup=[]
            try:self.retire()
            except BaseException as e:cleanup.append(e)
            if self._scope is not None:
                try:nothing(self.completion.quarantine(self._scope))
                except BaseException as e:cleanup.append(e)
            if self._reservation_started:
                try:nothing(self.resources.quarantine(self.spec,self._ticket,self._scope))
                except BaseException as e:cleanup.append(e)
            if self._admitted:
                try:nothing(self.sink.quarantine(self.spec,token))
                except BaseException as e:cleanup.append(e)
            if self._backend_touched:
                try:nothing(self.backend.retire(self.spec))
                except BaseException as e:cleanup.append(e)
            if cleanup:raise BaseExceptionGroup('stage failed; quarantine may be unknown',[error,*cleanup]) from None
            raise
        finally:self._lock.release()
