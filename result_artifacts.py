"""Isolated v4 ordinary prefill-logit artifacts. No computation/launch authority.

RESULT_STORED certifies bounded bytes and declared bindings, never correct inference.
Owner is supplied by a trusted internal caller; there is no public authentication.
Linux private directories, stable trusted namespace and honest storage required.
"""
from dataclasses import dataclass, asdict
import hashlib
import json
import os
import secrets
import stat
import struct

from inert_attempt_ledger import InertAttemptLedger
from snapshot_task_ledger import LedgerRejected, _need, _id
from token_input_snapshot import read_input, _root
import ledger_database as boundary

class DownloadRefused(Exception):
    """Expected local download policy/collector refusal, not storage corruption."""

_MANIFEST_MAX = 32768
_CHUNK = 65536


@dataclass(frozen=True, slots=True)
class Limits:
    file_bytes: int
    task_bytes: int
    owner_bytes: int
    system_bytes: int
    system_files: int
    writers: int


def _limits(v):
    _need(type(v) is Limits)
    for key, value in asdict(v).items():
        _need(type(value) is int and value > 0)
    _need(v.file_bytes <= 16*1024*1024 and v.task_bytes <= 64*1024*1024)
    _need(v.task_bytes <= v.owner_bytes <= v.system_bytes <= 1024**4)
    _need(v.system_files <= 100000 and v.writers <= 64)


def _encode(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    _need(len(raw) <= _MANIFEST_MAX)
    return raw


def _digest(domain, raw):
    return hashlib.sha256(domain + b'\0' + raw).hexdigest()


def _hash(value):
    _need(type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value))


_SCHEMA = tuple(s.replace("'STORED','CANCELLED','STOP_PENDING'", "'STORED','CANCELLED','STOP_PENDING','RESULT_STORED'")
                .replace("NEW.state IN ('CANCELLED','STOP_PENDING')", "NEW.state IN ('CANCELLED','STOP_PENDING','RESULT_STORED')")
                for s in InertAttemptLedger.SCHEMA) + (
    'CREATE TABLE artifact_policy(id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)',
    "CREATE TABLE artifact_tickets(task_id TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), owner_id TEXT NOT NULL, idem_key TEXT NOT NULL, fingerprint TEXT NOT NULL, writer_id TEXT NOT NULL UNIQUE, charge INTEGER NOT NULL CHECK(charge>0), files INTEGER NOT NULL CHECK(files>0), state TEXT NOT NULL CHECK(state IN ('WRITING','STORED')), manifest BLOB, digest TEXT, UNIQUE(owner_id,idem_key))",
    "CREATE TRIGGER artifact_identity BEFORE UPDATE OF task_id,owner_id,idem_key,fingerprint,writer_id,charge,files ON artifact_tickets BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER artifact_transition BEFORE UPDATE ON artifact_tickets WHEN NOT (OLD.state='WRITING' AND NEW.state='STORED' AND OLD.manifest IS NULL AND NEW.manifest IS NOT NULL AND NEW.digest IS NOT NULL) BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER artifact_completion BEFORE UPDATE OF state ON stored_tasks WHEN NEW.state='RESULT_STORED' AND NOT EXISTS(SELECT 1 FROM artifact_tickets r JOIN inert_attempts a ON a.task_id=r.task_id WHERE r.task_id=NEW.task_id AND r.owner_id=NEW.owner_id AND r.state='STORED' AND a.state='PENDING') BEGIN SELECT RAISE(ABORT,'binding'); END",
)
for _table in ('artifact_tickets', 'artifact_policy'):
    _SCHEMA += (f"CREATE TRIGGER {_table}_no_delete BEFORE DELETE ON {_table} BEGIN SELECT RAISE(ABORT,'immutable'); END",)
_SCHEMA += ("CREATE TRIGGER artifact_policy_no_update BEFORE UPDATE ON artifact_policy BEGIN SELECT RAISE(ABORT,'immutable'); END",)
del _table


class _Ledger(InertAttemptLedger):
    VERSION = 4
    SCHEMA = _SCHEMA
    STATES = frozenset(('STORED', 'CANCELLED', 'STOP_PENDING', 'RESULT_STORED'))

    def __init__(self, connection, snapshot_fd, *, result_fd, limits, initialize=False):
        _limits(limits)
        self._limits = limits
        fd = _root(result_fd)
        try:
            info=os.fstat(fd)
            self._result_identity=(info.st_dev,info.st_ino)
            if initialize:_need(not os.listdir(fd))
        finally:os.close(fd)
        self._policy = _encode({'limits':asdict(limits),'result_directory':list(self._result_identity)}).decode('ascii')
        self._results = result_fd
        super().__init__(connection, snapshot_fd, initialize=initialize)
        _need(self._db.execute('SELECT body FROM artifact_policy WHERE id=1').fetchone() == (self._policy,))

    def _initialize(self):
        super()._initialize()
        self._db.execute('INSERT INTO artifact_policy VALUES(1,?)', (self._policy,))

    def _verify_schema(self):
        super()._verify_schema()
        info=os.fstat(self._results)
        _need((info.st_dev,info.st_ino)==self._result_identity)
        _need(self._db.execute('SELECT body FROM artifact_policy WHERE id=1').fetchone() == (self._policy,))

    def _fence(self, owner, task_id, isolate):
        with self._transaction():
            before = self._record(self._db, owner, task_id)
            if before.task_state == 'RESULT_STORED':
                if isolate and before.attempt_state == 'PENDING':
                    changed=self._db.execute("UPDATE inert_attempts SET state='ISOLATED' WHERE task_id=? AND state='PENDING'",(task_id,))
                    _need(changed.rowcount==1)
                return self._record(self._db,owner,task_id)
            if before.attempt_state == 'PENDING' or (isolate and before.attempt_state == 'REVOKED'):
                target = 'ISOLATED' if isolate else 'REVOKED'
                changed = self._db.execute('UPDATE inert_attempts SET state=? WHERE task_id=? AND state=?', (target,task_id,before.attempt_state))
                _need(changed.rowcount == 1)
            if before.task_state == 'STORED':
                target = 'STOP_PENDING' if isolate else 'CANCELLED'
                changed = self._db.execute("UPDATE stored_tasks SET state=? WHERE task_id=? AND state='STORED'", (target,task_id))
                _need(changed.rowcount == 1)
            return self._record(self._db,owner,task_id)

    def _binding(self, owner, task_id, layout, entries, *, historical=False):
        _hash(layout)
        task, binding = self._metadata(owner, task_id)
        stored = read_input(self._directory, task.snapshot, binding)
        a = self._db.execute('SELECT session_id,epoch,plan_claim,snapshot_ref,state FROM inert_attempts WHERE task_id=?', (task_id,)).fetchone()
        _need(a is not None and a[3] == task.snapshot.digest)
        _need(a[4]=='PENDING' or (historical and task.state=='RESULT_STORED' and a[4]=='ISOLATED'))
        _need(type(entries) is tuple and 1 <= len(entries) <= 64)
        end = total = 0
        tiles = []
        for index, e in enumerate(entries):
            _need(type(e) is tuple and len(e) == 3)
            start, count, sha = e
            _need(type(start) is int and type(count) is int and start == end and count > 0)
            _hash(sha)
            end += count
            _need(end <= len(stored.tokens))
            size = count * binding.vocab_size * 4
            _need(size <= self._limits.file_bytes)
            total += size
            tiles.append({'tile':index,'start':start,'count':count,'shape':[1,count,binding.vocab_size], 'bytes':size,'sha256':sha,'file':str(index)+'.bin'})
        _need(end == len(stored.tokens))
        charge = total + _MANIFEST_MAX
        _need(charge <= self._limits.task_bytes)
        assignment = self._db.execute('SELECT stage_id,node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id', (task_id,)).fetchall()
        _need(len(assignment) == 2)
        record = {'version':1,'contract':'prefill_logits_v1','owner':owner,'task':task_id,
                  'request':binding.request_id,'session':a[0],'epoch':a[1],'plan_claim':a[2],
                  'model_claim':binding.model_digest,'input_ref':task.snapshot.digest,
                  'token_sha256':stored.token_blob_sha256,'preprocessing_version':binding.preprocessing_version,
                  'preprocessing_sha256':binding.preprocessing_sha256,'layout_claim':layout,
                  'assignments':[list(x) for x in assignment],'dtype':'float32-le','axes':['batch','token','vocab'],
                  'tiles':tiles,'token_count':end,'total_bytes':total,'binding_status':'declared_not_execution_verified'}
        raw = _encode(record)
        return task, record, raw, charge

    def _reserve(self, owner, task_id, key, layout, entries):
        _id(key)
        with self._transaction():
            task, record, raw, charge = self._binding(owner, task_id, layout, entries, historical=True)
            fp = _digest(b'opencompute/result-request/v1', raw)
            old = self._db.execute('SELECT idem_key,fingerprint,writer_id,state,manifest,digest FROM artifact_tickets WHERE task_id=? AND owner_id=?', (task_id,owner)).fetchone()
            if old:
                _need(old[0] == key and old[1] == fp and old[3] == 'STORED' and task.state == 'RESULT_STORED')
                return old[2], record, bytes(old[4]), old[5], True
            _need(task.state == 'STORED')
            charged,owned,file_count,writing = self._db.execute(
                "SELECT COALESCE(SUM(charge),0),COALESCE(SUM(CASE WHEN owner_id=? THEN charge ELSE 0 END),0),COALESCE(SUM(files),0),COALESCE(SUM(state='WRITING'),0) FROM artifact_tickets", (owner,)).fetchone()
            _need(charged+charge <= self._limits.system_bytes)
            _need(owned+charge <= self._limits.owner_bytes)
            files = 2*(len(entries)+1)+1
            _need(file_count+files <= self._limits.system_files)
            _need(writing < self._limits.writers)
            writer = secrets.token_hex(32)
            self._db.execute('INSERT INTO artifact_tickets VALUES(?,?,?,?,?,?,?,\'WRITING\',NULL,NULL)', (task_id,owner,key,fp,writer,charge,files))
            # Quota reservation + exclusive writer claim are one atomic operation.
        return writer, record, raw, None, False

    def _directory_for(self, writer, create=False):
        _hash(writer)
        root = _root(self._results)
        try:
            if create:
                os.mkdir(writer, mode=0o700, dir_fd=root)
                os.fsync(root)
            fd = os.open(writer, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC, dir_fd=root)
            try:
                checked = _root(fd)
                os.close(checked)
                return fd
            except BaseException:
                os.close(fd)
                raise
        finally:os.close(root)

    def _write(self, directory, name, chunks, size, sha, *, floats=False):
        fd = os.open('.pending-'+name, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC, 0o600, dir_fd=directory)
        digest = hashlib.sha256();count = pieces = 0
        try:
            for block in chunks:
                pieces += 1
                _need(pieces <= (size+_CHUNK-1)//_CHUNK + 1)
                _need(type(block) is bytes and 0 < len(block) <= _CHUNK)
                count += len(block);_need(count <= size)
                # Aligned chunks avoid carry buffers and bound validation memory.
                if floats:
                    _need(len(block)%4 == 0)
                    _need(all((x[0] & 0x7f800000) != 0x7f800000 for x in struct.iter_unpack('<I',block)))
                digest.update(block)
                view=memoryview(block)
                while view:
                    written=os.write(fd,view);_need(written>0);view=view[written:]
            _need(count == size and digest.hexdigest() == sha)
            os.fchmod(fd,0o400);os.fsync(fd)
        finally:os.close(fd)
        os.link('.pending-'+name,name,src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
        os.unlink('.pending-'+name,dir_fd=directory);os.fsync(directory)

    def _read_file(self, directory, name, size, sha, sink=None):
        fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=directory)
        try:
            info=os.fstat(fd)
            _need(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o400 and info.st_nlink==1 and info.st_size==size)
            digest=hashlib.sha256();count=0
            while True:
                block=os.read(fd,min(_CHUNK,size-count+1))
                if not block:break
                count+=len(block);_need(count<=size);digest.update(block)
                if sink is not None:sink(block)
            after=os.fstat(fd)
            _need(count==size and digest.hexdigest()==sha)
            _need(all(getattr(info,k)==getattr(after,k) for k in ('st_dev','st_ino','st_size','st_mode','st_uid','st_nlink','st_mtime_ns','st_ctime_ns')))
        finally:os.close(fd)

    def _verify_files(self, writer, record, raw):
        directory=self._directory_for(writer)
        try:
            _need(set(os.listdir(directory))=={'manifest.json'}|{e['file'] for e in record['tiles']})
            self._read_file(directory,'manifest.json',len(raw),hashlib.sha256(raw).hexdigest())
            for entry in record['tiles']:
                self._read_file(directory,entry['file'],entry['bytes'],entry['sha256'])
        finally:os.close(directory)

    def publish(self, owner, task_id, key, contract, layout, entries, sources, *, _guard=None):
        _need(contract=='prefill_logits_v1' and type(contract) is str)
        writer,record,raw,digest,retry=self._reserve(owner,task_id,key,layout,entries)
        if retry:
            self._verify_files(writer,record,raw)
            if _guard is not None:_guard()
            return digest
        directory=self._directory_for(writer,True)
        try:
            _need(type(sources) is tuple and len(sources)==len(entries))
            for entry,source in zip(record['tiles'],sources):
                self._write(directory,entry['file'],source,entry['bytes'],entry['sha256'],floats=True)
            self._write(directory,'manifest.json',(raw,),len(raw),hashlib.sha256(raw).hexdigest())
        finally:os.close(directory)
        self._verify_files(writer,record,raw)
        return self._commit_published(owner,task_id,layout,entries,writer,raw,_guard=_guard)

    def _commit_published(self, owner, task_id, layout, entries, writer, raw, *, _guard=None):
        # Caller verifies published files; this transaction also re-reads input
        # files through _binding. It is synchronous, not a DB-only operation.
        digest=_digest(b'opencompute/result-manifest/v1',raw)
        with self._transaction():
            task,again,canonical,charge=self._binding(owner,task_id,layout,entries)
            _need(task.state=='STORED' and canonical==raw)
            if _guard is not None:_guard()  # Trusted synchronous authority check.
            changed=self._db.execute("UPDATE artifact_tickets SET state='STORED',manifest=?,digest=? WHERE task_id=? AND owner_id=? AND writer_id=? AND state='WRITING'",(raw,digest,task_id,owner,writer))
            _need(changed.rowcount==1)
            changed=self._db.execute("UPDATE stored_tasks SET state='RESULT_STORED' WHERE task_id=? AND owner_id=? AND state='STORED'",(task_id,owner))
            _need(changed.rowcount==1)
        return digest

    def fetch(self, owner, task_id, sink, *, max_bytes=None, metadata=None):
        # sink(tile_id, bytes): partial output is provisional until successful return.
        with self._transaction():
            task,binding=self._metadata(owner,task_id);_need(task.state=='RESULT_STORED')
            row=self._db.execute('SELECT writer_id,manifest,digest FROM artifact_tickets WHERE task_id=? AND owner_id=? AND state=\'STORED\'',(task_id,owner)).fetchone()
            _need(row is not None and type(row[1]) is bytes and len(row[1])<=_MANIFEST_MAX)
            writer,raw,digest=row
            _need(_digest(b'opencompute/result-manifest/v1',raw)==digest)
            record=json.loads(raw)
            entries=tuple((e['start'],e['count'],e['sha256']) for e in record['tiles'])
            _,expected,canonical,_=self._binding(owner,task_id,record['layout_claim'],entries,historical=True)
            _need(canonical==raw)
        if max_bytes is not None and (type(max_bytes) is not int or not 0 < max_bytes <= 4194304 or expected['total_bytes'] > max_bytes):
            raise DownloadRefused()
        self._verify_files(writer,expected,raw)
        directory=self._directory_for(writer)
        try:
            for entry in expected['tiles']:
                self._read_file(directory,entry['file'],entry['bytes'],entry['sha256'],lambda b:sink(entry['tile'],b))
        finally:os.close(directory)
        if metadata is not None:
            projection={k:expected[k] for k in ('contract','dtype','axes','total_bytes')}
            projection['tiles']=[{k:e[k] for k in ('tile','start','count','shape','bytes','sha256')} for e in expected['tiles']]
            try:metadata(projection)
            except Exception:raise DownloadRefused() from None
        return digest


class ResultStore:
    """Managed internal facade. Caller owns input/result FDs and authenticates owner."""
    def __init__(self, managed):self._managed=managed
    def _call(self, name, *args):
        try:return self._managed._invoke(name,args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def create(self,*args):
        try:return self._call('create',*args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def record_pending(self,*args):
        try:return self._call('record_pending',*args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def cancel(self,*args):
        try:return self._call('cancel',*args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def quarantine(self,*args):
        try:return self._call('quarantine',*args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def publish(self,*args):
        try:return self._call('publish',*args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def fetch(self,*args):
        try:return self._call('fetch',*args)
        except Exception:pass
        del args
        raise LedgerRejected()
    def close(self):self._managed.close()


def _open(location, input_fd, result_fd, limits, initialize):
    try:
        def factory(connection,snapshot_fd,*,initialize):
            return _Ledger(connection,snapshot_fd,result_fd=result_fd,limits=limits,initialize=initialize)
        return ResultStore(boundary._open(location,input_fd,initialize,_factory=factory))
    except Exception:pass
    del location,input_fd,result_fd,limits
    raise LedgerRejected()


def initialize_new(location,input_fd,result_fd,limits):
    try:return _open(location,input_fd,result_fd,limits,True)
    except Exception:pass
    del location,input_fd,result_fd,limits
    raise LedgerRejected()
def open_existing(location,input_fd,result_fd,limits):
    try:return _open(location,input_fd,result_fd,limits,False)
    except Exception:pass
    del location,input_fd,result_fd,limits
    raise LedgerRejected()
