"""Private local control history; no authentication or physical resource proof.

resources.hold verifies an ALREADY existing, stable-reservation-id promise. It
must not create, release or refund resources. A real bootstrap/resource service
is required; missing providers deny. SQLite promises and queries grant no work.
All states retain charges. Independent databases are not one atomic budget.
capacity/charge are logical bytes, not measured GPU memory or physical capacity.
Trusted bootstrap must size them and bind the provider's existing promise.
"""
from contextlib import contextmanager
import json,os,re,sqlite3,threading
from pathlib import Path
from .local_attempt import _non_suppressing
from .store import Conflict,canonical

_FIELDS={'owner','task','session','epoch','stage','input_snapshot','model','layout',
         'plan','input_digest','device','instance','peer','declaration_digest','token_digest','preprocessing'}
_DIGESTS={'input_snapshot','model','layout','plan','input_digest','peer','declaration_digest','token_digest','preprocessing'}

def _identifier(value):
    if type(value)is not str or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',value)is None:
        raise ValueError('bounded stable identifier required')
    return value

def _binding(value):
    if type(value)is not dict or set(value)!=_FIELDS:raise TypeError('complete exact binding required')
    if type(value['epoch'])is not int or value['epoch']!=1:raise ValueError('fixed fresh epoch one required')
    if type(value['stage'])is not int or value['stage'] not in (0,1):raise ValueError('stage required')
    for k,v in value.items():
        if k in _DIGESTS:
            if type(v)is not str or re.fullmatch('[a-f0-9]{64}',v)is None:raise ValueError('binding digest required')
        elif k not in ('epoch','stage'):_identifier(v)
    return canonical(value)

class ControlJournal:
    """One trusted SQLite budget with permanent owner/task/stage uniqueness.

    Stable IDs are immutable. Only the fresh creator can prepare/consume/commit.
    Reopening is history-only even for RESERVED; fail may conservatively retire.
    Same-UID filesystem, caller and provider are trusted. Copies/rollback/new
    databases defeat local uniqueness; this is not a global admission registry.
    """
    def __init__(self,path,capacity,*,resources=None,require_receiving_gate=False):
        if type(capacity)is not int or not 0<capacity<2**63:raise ValueError('positive capacity required')
        if type(require_receiving_gate)is not bool:raise TypeError("explicit gate policy required")
        self.require_receiving_gate=require_receiving_gate;version=2 if require_receiving_gate else 1
        self.resources=resources;self._lock=threading.RLock();self._closed=False
        path=Path(path).absolute()
        try:
            fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd);self._writable=True
        except FileExistsError:self._writable=False
        self._db=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=5,isolation_level=None,check_same_thread=False)
        try:
            self._db.execute('PRAGMA synchronous=FULL')
            with self._tx() as c:
                if self._writable:
                    c.execute('CREATE TABLE control_policy(version INTEGER NOT NULL,capacity INTEGER NOT NULL)')
                    c.execute('INSERT INTO control_policy VALUES(?,?)',(version,capacity))
                if c.execute('SELECT version,capacity FROM control_policy').fetchall()!=[(version,capacity)]:raise Conflict('fixed resource policy mismatch')
                if self._writable:
                    c.execute("CREATE TABLE control_calls(command_id TEXT PRIMARY KEY,reservation_id TEXT NOT NULL UNIQUE,binding TEXT NOT NULL,stage_key TEXT NOT NULL UNIQUE,charge INTEGER NOT NULL CHECK(charge>0),state TEXT NOT NULL CHECK(state IN ('UNKNOWN','RESERVED','CONSUMED','COMMITTED')),output_digest TEXT,retired INTEGER NOT NULL CHECK(retired IN (0,1)))")
                    c.execute("CREATE TRIGGER control_binding_immutable BEFORE UPDATE OF command_id,reservation_id,binding,stage_key,charge ON control_calls BEGIN SELECT RAISE(ABORT,'immutable control binding'); END")
                if self._writable and require_receiving_gate:
                    c.execute("CREATE TABLE control_receiving_gate(command_id TEXT PRIMARY KEY,binding TEXT NOT NULL,nonce TEXT NOT NULL,recipient TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('ACKED','CONSUMED')))")
                c.execute('SELECT command_id,reservation_id,binding,charge,state,output_digest,retired FROM control_calls LIMIT 0')
        except BaseException:self.close();raise
    def _commit(self,c):c.commit()
    @contextmanager
    def _tx(self):
        with self._lock:
            if self._closed:raise Conflict('journal closed')
            self._db.execute('BEGIN IMMEDIATE')
            try:yield self._db;self._commit(self._db)
            except BaseException:self._db.rollback();raise
    def _provider(self):
        if self.resources is None or not callable(getattr(self.resources,'hold',None)):
            raise Conflict('real resource verification provider missing')
    def require_ready(self):
        """Side-effect-free shape/liveness preflight, never a resource grant."""
        with self._lock:
            if self._closed:raise Conflict('journal closed')
            self._fresh()
            self._provider()
    def _fresh(self):
        if not self._writable:raise Conflict('reopened journal is history-only; no execution qualification')
    def _hold(self,rid,raw,charge):
        with _non_suppressing(self.resources.hold(rid,json.loads(raw),charge)) as value:
            if value is not None:raise Conflict('resource hold must not return a permit')
    @staticmethod
    def _receipt(row):
        if row is None:return None
        command,rid,raw,charge,state,digest,retired=row
        return dict(command_id=command,reservation_id=rid,binding=json.loads(raw),charge=charge,
                    state=state,output_digest=digest,retired=bool(retired),
                    execution_allowed=False,resource_verified=False)
    @staticmethod
    def _row(c,command):
        return c.execute('SELECT command_id,reservation_id,binding,charge,state,output_digest,retired FROM control_calls WHERE command_id=?',(command,)).fetchone()
    def _bound(self,c,command,rid,raw):
        row=self._row(c,command)
        if row is None or row[1:3]!=(rid,raw):raise Conflict('fixed command/reservation binding mismatch')
        return row
    def query(self,command_id):
        _identifier(command_id)
        with self._lock:
            if self._closed:raise Conflict('journal closed')
            return self._receipt(self._row(self._db,command_id))
    def prepare(self,command_id,reservation_id,binding,charge):
        self._fresh()
        _identifier(command_id);_identifier(reservation_id);raw=_binding(binding)
        binding=json.loads(raw)
        if type(charge)is not int or not 0<charge<2**63:raise ValueError('positive charge required')
        self._provider()
        # Persist ownership+budget BEFORE asking the external provider anything.
        # UNKNOWN intent is charged permanently even if hold fails or we crash.
        with self._tx() as c:
            old=self._row(c,command_id)
            if old is not None:
                if old[1:4]!=(reservation_id,raw,charge):raise Conflict('command cannot be rebound')
                return self._receipt(old)
            stage_key=canonical([binding['owner'],binding['task'],binding['stage']])
            if c.execute('SELECT 1 FROM control_calls WHERE reservation_id=? OR stage_key=?',(reservation_id,stage_key)).fetchone():raise Conflict('reservation or stage already occupied')
            used=c.execute('SELECT coalesce(sum(charge),0) FROM control_calls').fetchone()[0]
            cap=c.execute('SELECT capacity FROM control_policy').fetchone()[0]
            if used+charge>cap:raise Conflict('shared budget exhausted')
            c.execute("INSERT INTO control_calls VALUES(?,?,?,?,?,'UNKNOWN',NULL,0)",(command_id,reservation_id,raw,stage_key,charge))
        try:
            self._hold(reservation_id,raw,charge)
            with self._tx() as c:
                if c.execute("UPDATE control_calls SET state='RESERVED' WHERE command_id=? AND state='UNKNOWN' AND retired=0",(command_id,)).rowcount!=1:raise Conflict('reservation intent retired')
            return self.query(command_id)
        except BaseException as error:self._failed(command_id,reservation_id,binding,error)
    def acknowledge_receiver(self,command,reservation,binding,nonce,recipient):
        """Local durable evidence only. Only the live wire gate may supply this.

        This method grants no operation: service also requires its original live
        gate handle. Reopening, copying rows or querying cannot rebuild a handle.
        """
        self._fresh();raw=_binding(binding)
        if not self.require_receiving_gate or binding['stage']!=1:raise Conflict('receiver policy required')
        for value in (nonce,recipient):
            if type(value)is not str or re.fullmatch('[a-f0-9]{64}',value)is None:raise Conflict('receiver challenge required')
        with self._tx() as c:
            row=self._bound(c,command,reservation,raw)
            if row[4]!='RESERVED' or row[6]:raise Conflict('receiver ACK too late')
            c.execute("INSERT INTO control_receiving_gate VALUES(?,?,?,?,'ACKED')",(command,raw,nonce,recipient))
    def _receiver(self,c,command,raw,state):
        if self.require_receiving_gate:
            row=c.execute('SELECT binding,state FROM control_receiving_gate WHERE command_id=?',(command,)).fetchone()
            if row!=(raw,state):raise Conflict('original C ACK required')
    def consume(self,command_id,reservation_id,binding):
        self._fresh()
        _identifier(command_id);_identifier(reservation_id);raw=_binding(binding);self._provider()
        binding=json.loads(raw)
        claimed=False
        try:
            with self._tx() as c:
                row=self._bound(c,command_id,reservation_id,raw)
                if row[4]!='RESERVED' or row[6]:raise Conflict('stage already consumed or unavailable')
                self._receiver(c,command_id,raw,"ACKED")
                if self.require_receiving_gate:c.execute("UPDATE control_receiving_gate SET state='CONSUMED' WHERE command_id=?",(command_id,))
                c.execute("UPDATE control_calls SET state='CONSUMED' WHERE command_id=?",(command_id,));claimed=True
            self._hold(reservation_id,raw,row[3])
            result=self.query(command_id)
            if result['state']!='CONSUMED' or result['retired']:raise Conflict('consumption retired during resource check')
            return result
        except BaseException as error:
            if claimed:self._failed(command_id,reservation_id,binding,error)
            raise
    def revalidate_consumed(self,command_id,reservation_id,binding):
        """Last synchronous check before invoke; never consume or resume work.

        Caller must still own the original live invocation and authorization
        fence, and must not await between this return and starting its body.
        History alone cannot establish that ownership. No charge is changed.
        """
        self._fresh()
        _identifier(command_id);_identifier(reservation_id);raw=_binding(binding)
        binding=json.loads(raw);self._provider()
        with self._lock:
            if self._closed:raise Conflict('journal closed')
            row=self._bound(self._db,command_id,reservation_id,raw)
            if row[4]!='CONSUMED' or row[6]:raise Conflict('live consumed stage required')
            self._receiver(self._db,command_id,raw,'CONSUMED')
        try:
            self._hold(reservation_id,raw,row[3])
            with self._lock:
                if self._closed:raise Conflict('journal closed')
                row=self._bound(self._db,command_id,reservation_id,raw)
                if row[4]!='CONSUMED' or row[6]:raise Conflict('consumption changed during revalidation')
                return self._receipt(row)
        except BaseException as error:self._failed(command_id,reservation_id,binding,error)
    def complete(self,command_id,reservation_id,binding,output_digest):
        self._fresh()
        _identifier(command_id);_identifier(reservation_id);raw=_binding(binding)
        binding=json.loads(raw)
        if type(output_digest)is not str or re.fullmatch('[a-f0-9]{64}',output_digest)is None:raise ValueError('output digest required')
        changed=False
        try:
            with self._tx() as c:
                row=self._bound(c,command_id,reservation_id,raw)
                if row[4]=='COMMITTED' and row[5]==output_digest:return self._receipt(row)
                if row[4]!='CONSUMED' or row[6]:raise Conflict('completion without live consumption')
                c.execute("UPDATE control_calls SET state='COMMITTED',output_digest=? WHERE command_id=?",(output_digest,command_id));changed=True
            return self.query(command_id)
        except BaseException as error:
            if changed:self._failed(command_id,reservation_id,binding,error)
            raise
    def fail(self,command_id,reservation_id,binding):
        _identifier(command_id);_identifier(reservation_id);raw=_binding(binding)
        with self._tx() as c:
            self._bound(c,command_id,reservation_id,raw)
            c.execute("UPDATE control_calls SET state=CASE WHEN state='COMMITTED' THEN state ELSE 'UNKNOWN' END,retired=1 WHERE command_id=?",(command_id,))
        return self.query(command_id)
    def _failed(self,command,rid,binding,error):
        try:self.fail(command,rid,binding)
        except BaseException as cleanup:raise BaseExceptionGroup('control outcome uncertain; charges retained',[error,cleanup]) from None
        raise error
    def close(self):
        with self._lock:
            if not self._closed:self._closed=True;self._db.close()
