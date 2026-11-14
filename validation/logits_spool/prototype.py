"""ISOLATED ordinary-byte protocol prototype. Never a compute/execution adapter.

SQLite books are independent synthetic local promises, not distributed atomic
reservations or authentication. Failures retain all charges; no recovery/GC API.
"""
from dataclasses import asdict, dataclass
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import struct
import threading

CHUNK=32768
MANIFEST=32768
MAX_RESULT=2*1024*1024

class Rejected(ValueError):pass

def need(ok):
    if not ok:raise Rejected('prototype boundary rejected')

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

@dataclass(frozen=True)
class Binding:
    owner:str
    task:str
    session_nonce:str
    attempt:str
    epoch:int
    connection:str
    model:str
    layout:str
    input_snapshot:str
    token_digest:str
    preprocessing:str
    plan_digest:str
    def __post_init__(self):
        need(type(self.epoch)is int and 1<=self.epoch<2**31)
        for name in ('owner','task','session_nonce','attempt','connection'):
            value=getattr(self,name)
            need(type(value)is str and re.fullmatch(r'[A-Za-z0-9_-]{1,64}',value) is not None)
        for name in ('model','layout','input_snapshot','token_digest','preprocessing','plan_digest'):
            value=getattr(self,name)
            need(type(value)is str and re.fullmatch('[a-f0-9]{64}',value) is not None)

@dataclass(frozen=True)
class Plan:
    binding:Binding
    counts:tuple
    vocab:int
    def __post_init__(self):
        need(type(self.binding)is Binding)
        self.binding.__post_init__()
        need(type(self.vocab)is int and 1<=self.vocab<=2**31)
        need(type(self.counts)is tuple and 1<=len(self.counts)<=64)
        need(all(type(n)is int and 1<=n<=1048576 for n in self.counts))
        need(self.total<=MAX_RESULT and sum((n*self.vocab*4+CHUNK-1)//CHUNK for n in self.counts)<=127)
    @property
    def total(self):return sum(self.counts)*self.vocab*4
    @property
    def charge(self):return self.total+MANIFEST
    def encoded(self):
        self.__post_init__()
        return canonical({'binding':asdict(self.binding),'counts':self.counts,'vocab':self.vocab,
                          'stage':1,'dtype':'float32-le','axes':['batch','token','vocab'],
                          'batch':1,'semantics':'full_logits','prototype_version':1})

class Book:
    """One shared local logical-byte pool across roles. Not production policy.

    Reopening allows query/idempotent reservation, never a second consumption.
    Copying/rolling back this DB defeats this local-only claim; trusted FS needed.
    """
    def __init__(self,path,capacity):
        need(type(capacity)is int and 0<capacity<=MAX_RESULT)
        self.lock=threading.RLock()
        self.db=sqlite3.connect(path,timeout=5,isolation_level=None,check_same_thread=False)
        try:
            with self.tx() as c:
                c.execute('CREATE TABLE IF NOT EXISTS policy(cap INTEGER NOT NULL)')
                if c.execute('SELECT count(*) FROM policy').fetchone()[0]==0:c.execute('INSERT INTO policy VALUES(?)',(capacity,))
                need(c.execute('SELECT cap FROM policy').fetchall()==[(capacity,)])
                c.execute('CREATE TABLE IF NOT EXISTS reservation(id TEXT PRIMARY KEY, request_key TEXT UNIQUE, binding TEXT NOT NULL, role TEXT NOT NULL, charge INTEGER NOT NULL, state TEXT NOT NULL, manifest TEXT, retired INTEGER NOT NULL DEFAULT 0)')
        except BaseException:
            self.db.close();raise
    @contextmanager
    def tx(self):
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                yield self.db
                self.db.commit()
            except BaseException:
                self.db.rollback();raise
    def reserve(self,rid,plan,role):
        need(type(rid)is str and re.fullmatch(r'[A-Za-z0-9_-]{1,64}',rid) is not None)
        need(role in ('source','destination'))
        raw=plan.encoded();key=canonical([plan.binding.owner,plan.binding.task])
        with self.tx() as c:
            old=c.execute('SELECT binding,role,charge FROM reservation WHERE id=?',(rid,)).fetchone()
            if old is not None:
                need(old==(raw,role,plan.charge));return  # idempotence is not permission
            need(c.execute('SELECT 1 FROM reservation WHERE request_key=?',(key,)).fetchone() is None)
            used=c.execute('SELECT coalesce(sum(charge),0) FROM reservation').fetchone()[0]
            need(used+plan.charge<=c.execute('SELECT cap FROM policy').fetchone()[0])
            c.execute('INSERT INTO reservation VALUES(?,?,?,?,?,\'RESERVED\',NULL,0)',(rid,key,raw,role,plan.charge))
    def record(self,rid):
        with self.lock:
            self.db.row_factory=sqlite3.Row
            row=self.db.execute('SELECT * FROM reservation WHERE id=?',(rid,)).fetchone()
            self.db.row_factory=None
            need(row is not None);return dict(row)
    def used(self):
        with self.lock:return self.db.execute('SELECT coalesce(sum(charge),0) FROM reservation').fetchone()[0]
    def confirm(self,rid,plan,role,states=('RESERVED',)):
        r=self.record(rid)
        need(r['binding']==plan.encoded() and r['role']==role and r['charge']==plan.charge and r['state'] in states and r['retired']==0)
        return r
    def transition(self,rid,plan,role,before,after,manifest=None):
        raw=plan.encoded()
        with self.tx() as c:
            changed=c.execute('UPDATE reservation SET state=?,manifest=coalesce(?,manifest) WHERE id=? AND binding=? AND role=? AND state=? AND retired=0',
                              (after,manifest,rid,raw,role,before))
            need(changed.rowcount==1)
    def isolate(self,rid):
        with self.tx() as c:c.execute("UPDATE reservation SET retired=1,state=CASE WHEN state='SEALED' THEN state ELSE 'ISOLATED' END WHERE id=?",(rid,))
    def close(self):self.db.close()

@dataclass(frozen=True)
class Frame:
    object_index:int
    sequence:int
    offset:int
    block:bytes

class Spool:
    """Synchronous write acknowledgment; consumer refs/RSS are not bounded here."""
    def __init__(self,root,source,destination,rid,plan):
        need(type(plan)is Plan);plan.__post_init__()
        self.root=Path(root);self.path=self.root/(hashlib.sha256(rid.encode()).hexdigest()+'.spool')
        self.source=source;self.destination=destination;self.rid=rid;self.plan=plan
        self.hook=lambda phase:None
        self.retired=False
    def check(self,states=('ADMITTED',)):
        self.source.confirm(self.rid,self.plan,'source',states)
        self.destination.confirm(self.rid,self.plan,'destination')
    def produce(self,body):
        need(not self.retired)
        # Two separate checks/commits. Not an atomic cross-node promise.
        self.check(('RESERVED',))
        self.source.transition(self.rid,self.plan,'source','RESERVED','ADMITTED')
        token=column=written=0
        owner=threading.get_ident();active=True
        try:
            self.check();self.root.mkdir(mode=0o700,parents=False,exist_ok=False)
            with self.path.open('xb') as f:
                def write(t,start,block):
                    nonlocal token,column,written
                    need(active and threading.get_ident()==owner)
                    self.check()
                    need(type(t)is int and type(start)is int and (t,start)==(token,column))
                    need(type(block)is bytes and 0<len(block)<=CHUNK and len(block)%4==0)
                    n=len(block)//4
                    need(token<sum(self.plan.counts) and column+n<=self.plan.vocab)
                    need(all(bits[0]&0x7f800000 != 0x7f800000 for bits in struct.iter_unpack('<I',block)))
                    need(f.write(block)==len(block));written+=len(block);column+=n
                    if column==self.plan.vocab:token+=1;column=0
                    self.hook('written');self.check()
                try:body(write)
                finally:active=False
                need((token,column,written)==(sum(self.plan.counts),0,self.plan.total))
                self.check();self.hook('before_fsync');f.flush();os.fsync(f.fileno())
                os.fchmod(f.fileno(),0o400)
                info=os.fstat(f.fileno());original=[info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns]
            directory=os.open(self.root,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
            try:os.fsync(directory)
            finally:os.close(directory)
            objects=[];full=hashlib.sha256()
            need(self.identity()==original)
            with self.path.open('rb') as f:
                info=os.fstat(f.fileno());need([info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns]==original)
                for count in self.plan.counts:
                    size=count*self.plan.vocab*4;remaining=size;digest=hashlib.sha256();chunks=[]
                    while remaining:
                        block=f.read(min(CHUNK,remaining));need(bool(block))
                        remaining-=len(block);digest.update(block);full.update(block);chunks.append(hashlib.sha256(block).hexdigest())
                    objects.append({'bytes':size,'sha256':digest.hexdigest(),'chunks':chunks})
                need(not f.read(1))
            manifest=canonical({'plan':json.loads(self.plan.encoded()),'objects':objects,
                                'sha256':full.hexdigest(),'file_identity':original})
            need(len(manifest.encode())<=MANIFEST)
            self.check();self.hook('before_commit');need(self.identity()==original);self.check()
            self.source.transition(self.rid,self.plan,'source','ADMITTED','SEALED',manifest)
            self.hook('after_commit')
        except BaseException:
            self.retired=True
            self.source.isolate(self.rid)  # SEALED remains historical after lost response
            raise
    def identity(self):
        s=self.path.stat(follow_symlinks=False)
        return [s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]
    def frames(self):
        owner=threading.get_ident()
        need(not self.retired)
        self.check(('SEALED',))
        record=self.source.record(self.rid);manifest=json.loads(record['manifest'])
        need(manifest['plan']==json.loads(self.plan.encoded()))
        self.source.transition(self.rid,self.plan,'source','SEALED','READING')
        try:
            need(self.identity()==manifest['file_identity'])
            with self.path.open('rb') as f:
                s=os.fstat(f.fileno())
                need([s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]==manifest['file_identity'])
                digest=hashlib.sha256()
                while block:=f.read(CHUNK):digest.update(block)
                need(digest.hexdigest()==manifest['sha256']);f.seek(0)
                for index,obj in enumerate(manifest['objects']):
                    offset=sequence=0;digest=hashlib.sha256()
                    while offset<obj['bytes']:
                        need(threading.get_ident()==owner)
                        self.check(('READING',));need(self.identity()==manifest['file_identity'])
                        block=f.read(min(CHUNK,obj['bytes']-offset))
                        need(len(block)==min(CHUNK,obj['bytes']-offset));digest.update(block)
                        need(hashlib.sha256(block).hexdigest()==obj['chunks'][sequence])
                        yield Frame(index,sequence,offset,block)
                        need(threading.get_ident()==owner)
                        offset+=len(block);sequence+=1
                    need(digest.hexdigest()==obj['sha256'])
                self.check(('READING',));need(self.identity()==manifest['file_identity'])
        except BaseException:
            self.retired=True;self.source.isolate(self.rid);raise
