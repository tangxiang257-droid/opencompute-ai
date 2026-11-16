"""Private SYNTHETIC producer interface contract, never a model execution grant.

No production import or public entrypoint. Trusted local callbacks and private
SQLite storage only; existing StageOwner/BLOB paths are deliberately untouched.
"""
from contextlib import contextmanager
from dataclasses import dataclass,field
import hashlib
import inspect
import json
import re
import struct
import threading
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.stage_ownership import _sync
from validation.logits_spool.prototype import Plan,Spool,Rejected,need,canonical,CHUNK,MANIFEST

MAX_INPUT=65536
MAX_WORKING=262144

@dataclass(frozen=True,slots=True)
class Spec:
    plan:Plan
    hidden:int
    input_digest:str
    def __post_init__(self):
        need(type(self.plan)is Plan);self.plan.__post_init__()
        need(type(self.hidden)is int and 1<=self.hidden<=2**31)
        need(0<self.input_bytes<=MAX_INPUT)
        need(type(self.input_digest)is str and re.fullmatch('[a-f0-9]{64}',self.input_digest) is not None)
        need(self.working_bytes<=MAX_WORKING)
    @property
    def input_bytes(self):return sum(self.plan.counts)*self.hidden*4
    @property
    def output_bytes(self):return self.plan.total
    @property
    def source_charge(self):return self.plan.charge
    @property
    def destination_charge(self):return self.plan.charge
    @property
    def working_bytes(self):return self.input_bytes+3*CHUNK+MANIFEST
    @property
    def digest(self):
        self.__post_init__()
        return hashlib.sha256(canonical({'plan':json.loads(self.plan.encoded()),'hidden':self.hidden,
            'actual_hidden_digest':self.input_digest,'input_bytes':self.input_bytes,
            'source_charge':self.source_charge,'destination_charge':self.destination_charge,
            'working_bytes':self.working_bytes,'interface':'synthetic-private-producer-v1'}).encode()).hexdigest()

@dataclass(frozen=True,slots=True)
class Permit:
    """A trusted test callback's declaration; NOT production authentication."""
    owner:str
    spec_digest:str
    execution_allowed:bool=field(default=False,init=False)

@dataclass(frozen=True,slots=True)
class Result:
    manifest_digest:str
    output_bytes:int
    source_charge:int
    destination_charge:int
    working_bytes:int
    execution_allowed:bool=field(default=False,init=False)
    payload_verified:bool=field(default=False,init=False)

@contextmanager
def _deny(spec):
    raise Rejected('default deny; synthetic callback required')
    yield  # contextmanager shape, unreachable

class PrivateProducer:
    def __init__(self,spec,spool,body,*,authorize=None):
        need(type(spec)is Spec and type(spool)is Spool);spec.__post_init__()
        need(spool.plan==spec.plan)
        need(callable(body) and not inspect.iscoroutinefunction(body))
        need(authorize is None or callable(authorize))
        self.spec=spec;self.spool=spool;self.body=body;self.authorize=authorize or _deny
        self._stop=threading.Event()
        # A separate logical working-window pool. Disk books remain separate.
        # No caller can supply a larger cap, or charge just a reference length.
        with spool.source.tx() as c:
            c.execute('CREATE TABLE IF NOT EXISTS private_producer_v1(id TEXT PRIMARY KEY,spec TEXT NOT NULL,state TEXT NOT NULL,working INTEGER NOT NULL,retired INTEGER NOT NULL)')
            row=c.execute('SELECT spec FROM private_producer_v1 WHERE id=?',(spool.rid,)).fetchone()
            if row is None:c.execute("INSERT INTO private_producer_v1 VALUES(?,?,'FRESH',0,0)",(spool.rid,spec.digest))
            else:need(row==(spec.digest,))
    def _check(self):
        need(not self._stop.is_set())
        with self.spool.source.lock:
            row=self.spool.source.db.execute('SELECT spec,retired FROM private_producer_v1 WHERE id=?',(self.spool.rid,)).fetchone()
        need(row==(self.spec.digest,0))
    def retire(self):
        self._stop.set()
        with self.spool.source.tx() as c:
            c.execute("UPDATE private_producer_v1 SET retired=1,state=CASE WHEN state='DONE' THEN state ELSE 'RETIRED' END WHERE id=?",(self.spool.rid,))
            c.execute("UPDATE reservation SET retired=1,state=CASE WHEN state='SEALED' THEN state ELSE 'ISOLATED' END WHERE id=?",(self.spool.rid,))
    def _consume(self):
        with self.spool.source.tx() as c:
            used=c.execute('SELECT coalesce(sum(working),0) FROM private_producer_v1').fetchone()[0]
            need(used+self.spec.working_bytes<=MAX_WORKING)
            changed=c.execute("UPDATE private_producer_v1 SET state='CONSUMED',working=? WHERE id=? AND spec=? AND state='FRESH' AND retired=0",
                (self.spec.working_bytes,self.spool.rid,self.spec.digest))
            need(changed.rowcount==1)
    def run(self,value):
        try:
            self._check();s=self.spec
            need(type(value)is bytes and len(value)==s.input_bytes)
            need(hashlib.sha256(value).hexdigest()==s.input_digest)
            need(all(bits[0]&0x7f800000 != 0x7f800000 for bits in struct.iter_unpack('<I',value)))
            with _non_suppressing(self.authorize(s)) as permit:
                need(type(permit)is Permit and permit.owner==s.plan.binding.owner and permit.spec_digest==s.digest)
                self._check()
                # Independent source/destination confirmations, no atomic remote claim.
                self.spool.check(('RESERVED',))
                self._consume();self._check()
                def produce(write):
                    self._check()
                    def checked_write(token,column,block):
                        self._check();write(token,column,block);self._check()
                    need(_sync(self.body(value,checked_write)) is None)
                    self._check()
                self.spool.produce(produce)
                self._check()
            # A context-exit failure is still a failed invocation after possible seal.
            self._check()
            r=self.spool.source.confirm(self.spool.rid,s.plan,'source',('SEALED',))
            with self.spool.source.tx() as c:
                changed=c.execute("UPDATE private_producer_v1 SET state='DONE' WHERE id=? AND spec=? AND state='CONSUMED' AND retired=0",(self.spool.rid,s.digest))
                need(changed.rowcount==1)
            return Result(hashlib.sha256(r['manifest'].encode()).hexdigest(),s.output_bytes,
                          s.source_charge,s.destination_charge,s.working_bytes)
        except BaseException:
            self.retire()
            raise
