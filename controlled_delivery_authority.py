"""Opt-in single-loop SQLite/byte-delivery authority, never execution authority.

Deployment MUST exclude all bypass writers (including legacy v5 store handles).
Directory flock excludes only cooperating instances, not arbitrary SQL or same-UID
code. No network listener, credentials, threads, migrations or production wiring.
"""
import asyncio
from dataclasses import dataclass
import fcntl
import hashlib
import os
import time
import struct
import secrets

import framed_result_transport as framing

import ledger_database as boundary
import structured_result_binding as storage
from snapshot_task_ledger import LedgerRejected, _id
from token_input_snapshot import InputBinding, SnapshotRef, _encode as encode_input
from isolated_tls_delivery import ReceiveBinding, Scope, LocalAuthority, Denied, accept_receiver


@dataclass(frozen=True, slots=True)
class ControlledDeployment:
    # Trusted service configuration, NEVER request input or auto-detected assurance.
    no_bypass_writers: bool = False


class RegistrationConflict(LedgerRejected):
    """Expected immutable-input conflict, not a storage health signal."""


class _Ledger(storage._Ledger):
    # Fresh schema only: persistent one-shot transport registration, not execution.
    VERSION = 6
    SCHEMA = storage._Ledger.SCHEMA + (
        'CREATE TABLE delivery_registrations(task_id TEXT PRIMARY KEY REFERENCES structural_plans(task_id), bundle_digest TEXT NOT NULL)',
        "CREATE TRIGGER delivery_registration_no_update BEFORE UPDATE ON delivery_registrations BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER delivery_registration_no_delete BEFORE DELETE ON delivery_registrations BEGIN SELECT RAISE(ABORT,'immutable'); END",
    )


    def _legacy_input_conflict(self, connection, owner, key):
        # Old versions retain their content-based replay semantics.
        self._shared(connection)
        return False


class DraftUnavailable(LedgerRejected):pass
class DraftVersionConflict(LedgerRejected):pass


class _MiddleLedger(_Ledger):
    """Fresh opt-in v9; local byte storage only, no migration or execution."""
    VERSION=9
    SCHEMA=_Ledger.SCHEMA+(
        "CREATE TABLE middle_objects(id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES structural_plans(task_id), owner TEXT NOT NULL, tile INTEGER NOT NULL, binding BLOB NOT NULL, consumer TEXT NOT NULL, size INTEGER NOT NULL CHECK(size>0 AND size<=1048576), sha TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('RESERVED','RECEIVING','READY','CLAIMED','CLEANUP_PENDING','CLEANED','ISOLATED')), charge INTEGER NOT NULL CHECK(charge=size OR (charge=0 AND state='CLEANED')), UNIQUE(task_id,tile))",
        "CREATE TRIGGER middle_identity BEFORE UPDATE OF id,task_id,owner,tile,binding,consumer,size,sha ON middle_objects BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER middle_transition BEFORE UPDATE ON middle_objects WHEN NOT ((NEW.state='ISOLATED' AND OLD.state<>'CLEANED' AND NEW.charge=OLD.charge) OR (OLD.state='RESERVED' AND NEW.state='RECEIVING' AND NEW.charge=OLD.charge) OR (OLD.state='RECEIVING' AND NEW.state='READY' AND NEW.charge=OLD.charge) OR (OLD.state='READY' AND NEW.state='CLAIMED' AND NEW.charge=OLD.charge) OR (OLD.state='CLAIMED' AND NEW.state='CLEANUP_PENDING' AND NEW.charge=OLD.charge) OR (OLD.state='CLEANUP_PENDING' AND NEW.state='CLEANED' AND NEW.charge=0)) BEGIN SELECT RAISE(ABORT,'transition'); END",
        "CREATE TRIGGER middle_no_delete BEFORE DELETE ON middle_objects BEGIN SELECT RAISE(ABORT,'immutable'); END",
    )

    def _binding(self,*args,**kwargs):
        result=super()._binding(*args,**kwargs)
        task,_,_,charge=result
        middle=self._db.execute('SELECT COALESCE(SUM(charge),0) FROM middle_objects').fetchone()[0]
        final=self._db.execute('SELECT COALESCE(SUM(charge),0) FROM artifact_tickets').fetchone()[0]
        existing=self._db.execute('SELECT 1 FROM artifact_tickets WHERE task_id=?',(task.task_id,)).fetchone()
        if middle+final+(0 if existing else charge)>2097152:raise Denied()
        return result


class MiddleReadHandle:
    """Opaque local capability. No public fd or whole-object payload accessor.

    Trusted same-process consumers must not fork, duplicate or introspect private
    descriptors. This is not a sandbox against hostile Python or same-UID code.
    """
    __slots__=('_authority','_id','_owner','_fd','_offset','_closed','_failed')
    def __init__(self,authority,key,owner,fd):
        self._authority=authority;self._id=key;self._owner=owner;self._fd=fd
        self._offset=0;self._closed=False;self._failed=False
    async def read(self,maximum=16384):
        return await self._authority._read_middle(self,maximum)
    async def close(self):
        return await self._authority._close_middle(self)


class _DraftLedger(_Ledger):
    """Explicit fresh v8 only; no implicit migration of v6/v7 databases."""
    VERSION=8
    SCHEMA=_Ledger.SCHEMA+(
        "CREATE TABLE prompt_drafts(draft_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, body BLOB NOT NULL CHECK(typeof(body)='blob' AND length(body)<=8192), version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version BETWEEN 1 AND 2147483647), deleted INTEGER NOT NULL CHECK(deleted IN (0,1)))",
        "CREATE TRIGGER draft_identity BEFORE UPDATE OF draft_id,owner_id ON prompt_drafts BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER draft_transition BEFORE UPDATE ON prompt_drafts WHEN OLD.deleted=1 OR NEW.version<>OLD.version+1 OR (NEW.deleted=1 AND length(NEW.body)<>0) BEGIN SELECT RAISE(ABORT,'invalid'); END",
        "CREATE TRIGGER draft_no_delete BEFORE DELETE ON prompt_drafts BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TABLE draft_creations(owner_id TEXT NOT NULL, operation TEXT NOT NULL CHECK(operation='create'), idem_key TEXT NOT NULL CHECK(length(idem_key)=32), content_digest BLOB NOT NULL CHECK(typeof(content_digest)='blob' AND length(content_digest)=32), draft_id TEXT NOT NULL UNIQUE REFERENCES prompt_drafts(draft_id), PRIMARY KEY(owner_id,operation,idem_key))",
        "CREATE TRIGGER draft_creation_owner BEFORE INSERT ON draft_creations WHEN NOT EXISTS(SELECT 1 FROM prompt_drafts d WHERE d.draft_id=NEW.draft_id AND d.owner_id=NEW.owner_id AND d.version=1 AND d.deleted=0) BEGIN SELECT RAISE(ABORT,'binding'); END",
        "CREATE TRIGGER draft_creation_no_update BEFORE UPDATE ON draft_creations BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER draft_creation_no_delete BEFORE DELETE ON draft_creations BEGIN SELECT RAISE(ABORT,'immutable'); END",
    )

    def _create_draft(self, owner, key, raw):
        """Shared transaction helper; never begins/commits its own transaction."""
        _id(owner)
        if not self._db.in_transaction or type(key) is not str or len(key)!=32 or any(c not in '0123456789abcdef' for c in key):raise Denied()
        if type(raw) is not bytes or len(raw)>8192:raise Denied()
        raw.decode('utf-8')
        digest=hashlib.sha256(b'opencompute/draft-create/utf8/v1\0'+raw).digest()
        row=self._db.execute("SELECT content_digest,draft_id FROM draft_creations WHERE owner_id=? AND operation='create' AND idem_key=?",(owner,key)).fetchone()
        if row is not None:
            if row[0]!=digest:raise DraftVersionConflict()
            saved=self._db.execute('SELECT version,deleted FROM prompt_drafts WHERE draft_id=? AND owner_id=?',(row[1],owner)).fetchone()
            if saved is None:raise LedgerRejected()
            return {'draft_id':row[1],'version':saved[0],'deleted':bool(saved[1]),'replayed':True}
        if self._db.execute('SELECT count(*) FROM prompt_drafts').fetchone()[0]>=4096 or self._db.execute('SELECT count(*) FROM prompt_drafts WHERE owner_id=?',(owner,)).fetchone()[0]>=256:raise DraftUnavailable()
        draft_id=secrets.token_hex(16)
        self._db.execute('INSERT INTO prompt_drafts VALUES(?,?,?,1,0)',(draft_id,owner,raw))
        self._db.execute("INSERT INTO draft_creations VALUES(?,'create',?,?,?)",(owner,key,digest,draft_id))
        return {'draft_id':draft_id,'version':1,'deleted':False,'replayed':False}


class _DraftSourceLedger(_DraftLedger):
    """Fresh v10 internal ordinary-data provenance; NOT tokenizer acceptance."""
    VERSION=10
    SCHEMA=_DraftLedger.SCHEMA+(
        "CREATE TABLE draft_task_sources(task_id TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), draft_id TEXT NOT NULL REFERENCES prompt_drafts(draft_id), draft_version INTEGER NOT NULL CHECK(draft_version>0), text_sha256 TEXT NOT NULL, contract_sha256 TEXT NOT NULL, snapshot_digest TEXT NOT NULL, status TEXT NOT NULL CHECK(status='unverified_internal_data'))",
        "CREATE TRIGGER draft_task_sources_no_update BEFORE UPDATE ON draft_task_sources BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER draft_task_sources_no_delete BEFORE DELETE ON draft_task_sources BEGIN SELECT RAISE(ABORT,'immutable'); END",
    )

    def _commit_draft_input(self,connection,owner,key,draft_id,version,raw,
                            contract_sha256,binding,reference):
        """Internal shared transaction only. No tokenizer, publication or commit.

        Caller publishes ordinary input outside this transaction. This helper
        rechecks the CURRENT exact owner/version/text even on an idempotent retry.
        It never treats supplied contract hashes as verified tokenizer assets.
        """
        self._shared(connection)
        _id(owner);_id(key);_id(draft_id)
        if type(version) is not int or not 1<=version<=2147483647:raise Denied()
        if type(raw) is not bytes or len(raw)>8192:raise Denied()
        if (type(contract_sha256) is not str or len(contract_sha256)!=64
                or any(c not in '0123456789abcdef' for c in contract_sha256)):raise Denied()
        if type(binding) is not InputBinding or type(reference) is not SnapshotRef:raise Denied()
        if binding.owner_id!=owner or binding.preprocessing_sha256!=contract_sha256:raise Denied()
        actual=connection.execute('SELECT body,version,deleted FROM prompt_drafts WHERE draft_id=? AND owner_id=?',(draft_id,owner)).fetchone()
        if actual!=(raw,version,0):raise DraftVersionConflict()
        expected=(draft_id,version,hashlib.sha256(raw).hexdigest(),contract_sha256,reference.digest,'unverified_internal_data')
        prior=connection.execute('SELECT task_id FROM input_requests WHERE owner_id=? AND idem_key=?',(owner,key)).fetchone()
        if prior is not None:
            saved=connection.execute('SELECT draft_id,draft_version,text_sha256,contract_sha256,snapshot_digest,status FROM draft_task_sources WHERE task_id=?',prior).fetchone()
            if saved!=expected:raise RegistrationConflict()
        task=self._commit_published_input(connection,owner,key,binding,reference)
        if prior is None:
            connection.execute('INSERT INTO draft_task_sources VALUES(?,?,?,?,?,?,?)',(task.task_id,*expected))
        return task


class _UnifiedLedger(_DraftSourceLedger, _MiddleLedger):
    """Fresh v11: one common ledger, draft provenance plus middle lifecycle.

    MRO retains _MiddleLedger._binding's joint result/middle budget check.
    Shared task/attempt/plan/result tables occur exactly once. No migration.
    """
    VERSION = 11
    SCHEMA = _DraftSourceLedger.SCHEMA + _MiddleLedger.SCHEMA[len(_Ledger.SCHEMA):]


class Authority:
    def __init__(self):
        raise Denied()

    def _check(self):
        if asyncio.get_running_loop() is not self._loop or self._poisoned:raise Denied()
        # Resource retention after shutdown starts is not permission for new
        # work. Fence legacy APIs too, including a failed backend close. Internal
        # close teardown deliberately does not call this operational gate.
        owner=getattr(self,'_text_registration_owner',None)
        if owner is not None and owner._closing:raise Denied()

    def _middle_ledger(self):
        self._check()
        if not self._middle_enabled:raise Denied()
        managed=self._store._managed
        boundary._check(managed._directory,managed._trust,managed._identity)
        return managed._ledger

    def _middle_plan(self,ledger,owner,task):
        # Must share the caller's transaction. No independent commit.
        ledger._shared(ledger._db)
        row=ledger._db.execute('SELECT bundle FROM structural_plans WHERE task_id=?',(task,)).fetchone()
        if row is None:raise Denied()
        metadata,bundle,receipt,assignments,mapping=ledger._validated(owner,task,row[0])
        record=ledger._record(ledger._db,owner,task)
        saved=ledger._db.execute('SELECT session_id,epoch,plan_claim,snapshot_ref,state FROM inert_attempts WHERE task_id=?',(task,)).fetchone()
        a=bundle['attempt']
        if metadata.state!='STORED' or record.attempt_state!='PENDING' or saved!=(a['session_id'],a['epoch'],receipt.plan_digest,receipt.snapshot_ref,'PENDING'):raise Denied()
        holds=ledger._db.execute('SELECT node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id',(task,)).fetchall()
        if tuple(holds)!=assignments:raise Denied()
        return bundle,receipt,mapping

    def _middle_row(self,ledger,owner,key):
        row=ledger._db.execute('SELECT task_id,tile,binding,consumer,size,sha,state,charge FROM middle_objects WHERE id=? AND owner=?',(key,owner)).fetchone()
        if row is None:raise Denied()
        return row

    async def _isolate_middle(self,ledger,key):
        # Failure paths use the same authority lock/connection as normal writes.
        # If cancelled again before this completes, the pre-existing charge is
        # retained and restart will quarantine the unfinished state.
        async with self._lock:
            with ledger._transaction():ledger._db.execute("UPDATE middle_objects SET state='ISOLATED' WHERE id=? AND state<>'CLEANED'",(key,))

    async def reserve_middle(self,owner,task,tile,sha):
        """Reserve A.output -> B.input ordinary bytes; never a compute permit.

        Fixed independent quota: 1 MiB/object, 2 MiB and two active slots per
        database. Includes orphan/isolated objects. No waiting or automatic GC.
        """
        if type(tile) is not int or type(sha) is not str or len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):raise Denied()
        async with self._lock:
            ledger=self._middle_ledger()
            with ledger._transaction():
                bundle,receipt,mapping=self._middle_plan(ledger,owner,task)
                if not 0<=tile<len(mapping):raise Denied()
                start,count=mapping[tile];size=4*count*bundle['layout']['hidden_size']
                charge,slots=ledger._db.execute('SELECT COALESCE(SUM(charge),0),SUM(charge>0) FROM middle_objects').fetchone()
                final=ledger._db.execute('SELECT COALESCE(SUM(charge),0) FROM artifact_tickets').fetchone()[0]
                if not 0<size<=1048576 or charge+final+size>2097152 or (slots or 0)>=2:raise Denied()
                if ledger._db.execute('SELECT 1 FROM middle_objects WHERE task_id=? AND tile=?',(task,tile)).fetchone():raise Denied()
                a=bundle['attempt']
                binding=storage.artifacts._encode({'version':1,'task':task,'attempt':receipt.attempt_digest,'epoch':a['epoch'],'plan':receipt.plan_digest,'layout':receipt.layout_digest,'model':receipt.model_record_digest,'input':receipt.plan_input_digest,'edge':'stage0.output->stage1.input','role':'hidden_states','dtype':'float32','shape':[1,count,bundle['layout']['hidden_size']],'range':[start,start+count],'tile':tile,'bundle':receipt.bundle_digest})
                key=secrets.token_hex(32)
                ledger._db.execute('INSERT INTO middle_objects VALUES(?,?,?,?,?,?,?,?,?,?)',(key,task,owner,tile,binding,a['assignments'][1]['agent_id'],size,sha,'RESERVED',size))
                return key

    async def middle_status(self,owner,key):
        async with self._lock:
            ledger=self._middle_ledger()
            with ledger._transaction():
                row=self._middle_row(ledger,owner,key)
                return {'state':row[6],'charge':row[7],'size':row[4]}

    async def register_middle_receiver(self,owner,obj,reader,writer,config):
        """Temporary loopback TLS only. No public identity provider or listener.

        The object selects its bound port, not a caller-provided delivery mode.
        Certificate pins identify the sender; the local handle consumer remains
        a separately checked trusted in-process principal, not a TLS peer claim.
        """
        receiver=None;reserved=False;pending=None;key=None
        try:
            async with self._lock:
                ledger=self._middle_ledger()
                if not self._middle_network_enabled:raise Denied()
                if writer.get_extra_info('peername')[0]!='127.0.0.1' or writer.get_extra_info('sockname')[0]!='127.0.0.1':raise Denied()
                if len(self._receivers)+len(self._pending)>=16:raise Denied()
                with ledger._transaction():
                    row=self._middle_row(ledger,owner,obj)
                    bundle,receipt,mapping=self._middle_plan(ledger,owner,row[0])
                    if row[6]!='RESERVED':raise Denied()
                    a=bundle['attempt'];source=a['assignments'][0]['agent_id']
                    if source not in self._pins or config.peers!={source:self._pins[source]}:raise Denied()
                    start,count=mapping[row[1]]
                    scope=Scope(row[0],a['epoch'],receipt.plan_digest,receipt.layout_digest,'stage0->stage1','hidden_states',a['session_id'],receipt.attempt_digest,source,self._pins[source],'unconnected')
                    binding=ReceiveBinding(scope,0,receipt.model_record_digest,bundle['layout']['layout_schema_version'],'float32',((start,start+count,(1,count,bundle['layout']['hidden_size'])),))
                    # Receiver construction validates the actual TLS cert before
                    # persisting the one-shot RECEIVING state or granting credit.
                    receiver=framing.ObjectReceiver(reader,writer,config,binding,obj,row[4],row[5],middle_tile=row[1])
                    ledger._db.execute("UPDATE middle_objects SET state='RECEIVING' WHERE id=?",(obj,))
                reserved=True
                receiver._middle_object=obj
                key=object();self._receivers[key]=(receiver,owner,row[0],receipt.bundle_digest)
                pending=object();self._pending[pending]=(asyncio.current_task(),writer)
            await receiver._channel.start()
            async with self._lock:
                self._check();receiver.check()
                with ledger._transaction():self._middle_plan(ledger,owner,row[0])
                return key
        except BaseException:
            if reserved:await self._isolate_middle(ledger,obj)
            writer.close()
            if receiver is not None:
                await receiver.close()
                async with self._lock:
                    if key is not None:self._receivers.pop(key,None)
            raise Denied() from None
        finally:
            if pending is not None:self._pending.pop(pending,None)

    async def middle_context(self,owner,key):
        async with self._lock:
            self._middle_ledger()
            receiver,actual,*_=self._receivers[key]
            if actual!=owner or not hasattr(receiver,'_middle_object'):raise Denied()
            return receiver.context

    async def receive_middle(self,owner,key):
        fd=directory=None
        async with self._lock:
            ledger=self._middle_ledger()
            receiver,actual,task,digest=self._receivers[key]
            if actual!=owner or not hasattr(receiver,'_middle_object') or receiver.active:raise Denied()
            obj=receiver._middle_object
            with ledger._transaction():
                row=self._middle_row(ledger,owner,obj)
                self._middle_plan(ledger,owner,task)
                if row[6]!='RECEIVING':raise Denied()
            receiver.active=True;self._middle_busy.add(obj)
            pending=object();self._pending[pending]=(asyncio.current_task(),receiver._channel.w)
        try:
            directory=ledger._directory_for(obj,True)
            fd=os.open('.pending-payload',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
            sha=hashlib.sha256()
            while receiver.written<receiver.context.total_bytes:
                receiver.held=await receiver._channel.receive()
                async with self._lock:
                    self._check();receiver.check()
                    with ledger._transaction():
                        _,receipt,_=self._middle_plan(ledger,owner,task)
                        current=self._middle_row(ledger,owner,obj)
                        if receipt.bundle_digest!=digest or current[6]!='RECEIVING':raise Denied()
                    block=receiver.validate(receiver.held)
                # No await/background write while bytes are in ordinary file I/O.
                view=memoryview(block)
                while view:
                    count=os.write(fd,view)
                    if count<=0:raise Denied()
                    view=view[count:]
                sha.update(block);receiver.written+=len(block);receiver.frames+=1
                block=view=None
                await receiver.release_written_frame()
                await self._after_object_frame()
            if sha.hexdigest()!=row[5]:raise Denied()
            os.fchmod(fd,0o400);os.fsync(fd)
            old,fd=fd,None;os.close(old)
            os.link('.pending-payload','payload',src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
            os.unlink('.pending-payload',dir_fd=directory);os.fsync(directory)
            ledger._read_file(directory,'payload',row[4],row[5])
            await self._after_result_files()
            async with self._lock:
                self._check();receiver.check()
                with ledger._transaction():
                    self._middle_plan(ledger,owner,task)
                    changed=ledger._db.execute("UPDATE middle_objects SET state='READY' WHERE id=? AND state='RECEIVING'",(obj,))
                    if changed.rowcount!=1:raise Denied()
            return obj
        except BaseException:
            await self._isolate_middle(ledger,obj)
            raise Denied() from None
        finally:
            try:
                try:
                    if fd is not None:
                        old,fd=fd,None;os.close(old)
                finally:
                    if directory is not None:
                        old,directory=directory,None;os.close(old)
            except BaseException:
                await self._isolate_middle(ledger,obj)
                raise Denied() from None
            finally:
                receiver._fence()
                if receiver.held is not None:receiver.held.envelope=None;receiver.held=None
                try:
                    # No authority lock while awaiting stream/task closure.
                    await receiver.close()
                    async with self._lock:self._receivers.pop(key,None)
                except BaseException:
                    await self._isolate_middle(ledger,obj)
                    raise Denied() from None
                finally:
                    self._middle_busy.discard(obj);self._pending.pop(pending,None)

    async def store_middle(self,owner,key,chunks):
        # Local-only bounded source, never a network callback or async iterator.
        if (type(chunks) is not tuple or not 1<=len(chunks)<=16
                or any(type(b) is not bytes or not 0<len(b)<=65536 for b in chunks)
                or any(len(b)!=65536 for b in chunks[:-1])):raise Denied()
        async with self._lock:
            ledger=self._middle_ledger()
            with ledger._transaction():
                row=self._middle_row(ledger,owner,key)
                self._middle_plan(ledger,owner,row[0])
                if row[6]!='RESERVED' or sum(map(len,chunks))!=row[4]:raise Denied()
                ledger._db.execute("UPDATE middle_objects SET state='RECEIVING' WHERE id=?",(key,))
            self._middle_busy.add(key)
        directory=None
        try:
            # Synchronous ordinary I/O outside authority lock, no background writer.
            directory=ledger._directory_for(key,True)
            ledger._write(directory,'payload',chunks,row[4],row[5])
            ledger._read_file(directory,'payload',row[4],row[5])
            async with self._lock:
                self._check()
                with ledger._transaction():
                    self._middle_plan(ledger,owner,row[0])
                    ledger._db.execute("UPDATE middle_objects SET state='READY' WHERE id=? AND state='RECEIVING'",(key,))
        except BaseException:
            await self._isolate_middle(ledger,key)
            raise Denied() from None
        finally:
            try:
                if directory is not None:os.close(directory)
            except BaseException:
                await self._isolate_middle(ledger,key)
                raise Denied() from None
            finally:self._middle_busy.discard(key)

    async def claim_middle(self,owner,key,consumer):
        async with self._lock:
            ledger=self._middle_ledger()
            if key in self._middle_busy:raise Denied()
            with ledger._transaction():
                row=self._middle_row(ledger,owner,key)
                self._middle_plan(ledger,owner,row[0])
                if row[6]!='READY' or consumer!=row[3]:raise Denied()
                ledger._db.execute("UPDATE middle_objects SET state='CLAIMED' WHERE id=?",(key,))
            self._middle_busy.add(key)
        directory=fd=None
        try:
            directory=ledger._directory_for(key)
            ledger._read_file(directory,'payload',row[4],row[5])
            fd=os.open('payload',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
            # Controlled namespace/no bypass writers is mandatory; retain exact fd.
            async with self._lock:
                self._check()
                with ledger._transaction():self._middle_plan(ledger,owner,row[0])
                h=MiddleReadHandle(self,key,owner,fd);fd=None
                self._middle_handles[key]=h
                return h
        except BaseException:
            await self._isolate_middle(ledger,key)
            raise Denied() from None
        finally:
            try:
                try:
                    if fd is not None:os.close(fd)
                finally:
                    if directory is not None:os.close(directory)
            except BaseException:
                if key in self._middle_handles:self._middle_handles[key]._failed=True
                await self._isolate_middle(ledger,key)
                raise Denied() from None
            finally:self._middle_busy.discard(key)

    async def _read_middle(self,h,maximum):
        if type(maximum) is not int or not 1<=maximum<=16384:raise Denied()
        async with self._lock:
            ledger=self._middle_ledger()
            if h._closed or h._failed or h._id in self._middle_busy or self._middle_handles.get(h._id) is not h:raise Denied()
            with ledger._transaction():
                row=self._middle_row(ledger,h._owner,h._id)
                self._middle_plan(ledger,h._owner,row[0])
                if row[6]!='CLAIMED':raise Denied()
            self._middle_busy.add(h._id)
        try:
            # Ordinary bounded read outside lock, no background I/O. Cancellation
            # may win before delivery: bytes read here are still internal.
            block=os.pread(h._fd,min(maximum,row[4]-h._offset),h._offset)
            if not block and h._offset<row[4]:h._failed=True;raise Denied()
            async with self._lock:
                self._check()
                with ledger._transaction():self._middle_plan(ledger,h._owner,row[0])
                h._offset+=len(block)
                return block
        except BaseException:
            h._failed=True
            await self._isolate_middle(ledger,h._id)
            raise Denied() from None
        finally:self._middle_busy.discard(h._id)

    async def _close_middle(self,h):
        async with self._lock:
            ledger=self._middle_ledger()
            if h._failed or h._id in self._middle_busy:raise Denied()
            if h._closed:return
            if self._middle_handles.get(h._id) is not h:raise Denied()
            with ledger._transaction():
                row=self._middle_row(ledger,h._owner,h._id)
                if row[6]!='CLAIMED':raise Denied()
                ledger._db.execute("UPDATE middle_objects SET state='CLEANUP_PENDING' WHERE id=?",(h._id,))
            h._closed=True;self._middle_busy.add(h._id)
        directory=None
        try:
            # Close exact owned descriptor BEFORE unlink. Never retry uncertain fd.
            fd,h._fd=h._fd,None;os.close(fd)
            directory=ledger._directory_for(h._id)
            os.unlink('payload',dir_fd=directory);os.fsync(directory)
            old,directory=directory,None;os.close(old)
            os.rmdir(h._id,dir_fd=ledger._results);os.fsync(ledger._results)
            async with self._lock:
                self._check()
                with ledger._transaction():ledger._db.execute("UPDATE middle_objects SET state='CLEANED',charge=0 WHERE id=?",(h._id,))
                self._middle_handles.pop(h._id)
        except BaseException:
            h._failed=True
            await self._isolate_middle(ledger,h._id)
            raise Denied() from None
        finally:
            try:
                if directory is not None:os.close(directory)
            finally:self._middle_busy.discard(h._id)

    async def draft(self, owner, action, *, draft_id=None, text=None, version=None, after=None, limit=10, key=None):
        return await self._draft(owner,action,draft_id=draft_id,text=text,version=version,after=after,limit=limit,key=key)

    async def _draft(self, owner, action, *, draft_id=None, text=None, version=None, after=None, limit=10, key=None, _authorize=None, _allow_text=False):
        """Plain UTF-8 storage only. No task/plan/transport operation occurs."""
        _id(owner)
        if action not in ('create','list','read','update','delete'):raise Denied()
        if action=='create' and (type(key) is not str or len(key)!=32 or any(c not in '0123456789abcdef' for c in key)):raise Denied()
        if action!='create' and key is not None:raise Denied()
        for value in (draft_id,after):
            if value is not None and (type(value) is not str or len(value)!=32 or any(c not in '0123456789abcdef' for c in value)):raise Denied()
        if action in ('read','update','delete') and draft_id is None:raise Denied()
        if action in ('update','delete') and (type(version) is not int or not 1<=version<2147483647):raise Denied()
        if type(limit) is not int or not 1<=limit<=20:raise Denied()
        raw=None
        if action in ('create','update'):
            if type(text) is not str or len(text)>8192:raise Denied()
            raw=text.encode('utf-8')
            if len(raw)>8192:raise Denied()
        async with self._lock:
            if _authorize is not None:
                current=_authorize()
                if type(current) is not str or current!=owner:raise Denied()
            self._check();managed=self._store._managed;ledger=managed._ledger
            allowed=type(ledger) in (_DraftLedger,_DraftSourceLedger,_UnifiedLedger)
            if _allow_text:
                from text_input_ledger import TextInputLedger
                allowed=type(ledger) is TextInputLedger and getattr(self,'_text_registration_owner',None) is not None
            if not allowed:raise Denied()
            try:
                boundary._check(managed._directory,managed._trust,managed._identity)
                with ledger._transaction():
                    db=ledger._db
                    if action=='create':
                        return ledger._create_draft(owner,key,raw)
                    if action=='list':
                        rows=db.execute('SELECT draft_id,version,length(body) FROM prompt_drafts WHERE owner_id=? AND deleted=0 AND draft_id>? ORDER BY draft_id LIMIT ?',(owner,after or '',limit+1)).fetchall()
                        return {'items':[{'draft_id':r[0],'version':r[1],'bytes':r[2]} for r in rows[:limit]],'next_cursor':rows[limit-1][0] if len(rows)>limit else None}
                    row=db.execute('SELECT body,version FROM prompt_drafts WHERE draft_id=? AND owner_id=? AND deleted=0',(draft_id,owner)).fetchone()
                    if row is None:raise DraftUnavailable()
                    if action=='read':return {'draft_id':draft_id,'version':row[1],'text':row[0].decode('utf-8')}
                    if row[1]!=version:raise DraftVersionConflict()
                    # Reserve the last representable version for a tombstone.
                    # Supported updates must never create an undeletable draft.
                    if action=='update' and version>=2147483646:raise DraftVersionConflict()
                    changed=db.execute('UPDATE prompt_drafts SET body=?,version=version+1,deleted=? WHERE draft_id=? AND owner_id=? AND version=? AND deleted=0',(b'' if action=='delete' else raw,int(action=='delete'),draft_id,owner,version))
                    if changed.rowcount!=1:raise DraftVersionConflict()
                    return {'draft_id':draft_id,'version':version+1,'deleted':action=='delete'}
            except (DraftUnavailable,DraftVersionConflict):raise
            except BaseException:self._poison();raise

    def _poison(self):
        gate=getattr(self,'_task_consent_runtime_fence',None)
        if gate is not None:gate.invalidate()
        self._poisoned=True
        for receiver, *_ in self._receivers.values():
            try:receiver._fence()
            except BaseException:
                # Claim entry is already poisoned, even if cleanup itself fails.
                receiver._fenced=True;receiver._event.set()
                receiver._channel.w.close()

    def _state(self, owner, task):
        try:
            managed=self._store._managed
            boundary._check(managed._directory,managed._trust,managed._identity)
            ledger=managed._ledger
            with ledger._transaction():
                row=ledger._db.execute('SELECT bundle FROM structural_plans WHERE task_id=?',(task,)).fetchone()
                if row is None:raise Denied()
                metadata,bundle,receipt,assignments,mapping=ledger._validated(owner,task,row[0])
                record=ledger._record(ledger._db,owner,task)
                if metadata.state!='STORED' or record.attempt_state!='PENDING':raise Denied()
                a=bundle['attempt']
                saved=ledger._db.execute('SELECT session_id,epoch,plan_claim,snapshot_ref FROM inert_attempts WHERE task_id=?',(task,)).fetchone()
                holds=ledger._db.execute('SELECT node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id',(task,)).fetchall()
                if saved!=(a['session_id'],a['epoch'],receipt.plan_digest,receipt.snapshot_ref) or tuple(holds)!=assignments:raise Denied()
            return bundle,receipt,mapping
        except BaseException:
            self._poison();raise

    async def require_owner(self, owner, task):
        """Non-disclosing membership check; a miss must not fence other owners."""
        async with self._lock:
            self._check()
            try:
                managed=self._store._managed
                boundary._check(managed._directory,managed._trust,managed._identity)
                ledger=managed._ledger
                with ledger._transaction():
                    row=ledger._db.execute('SELECT 1 FROM stored_tasks WHERE task_id=? AND owner_id=?',(task,owner)).fetchone()
            except BaseException:self._poison();raise
            if row is None:raise Denied()

    async def task_records(self, owner, *, after=None, limit=10):
        return await self._task_records(owner,after=after,limit=limit)

    async def _task_records(self, owner, *, after=None, limit=10, _authorize=None):
        """Owner filter precedes keyset pagination. No chronological/snapshot claim."""
        if type(limit) is not int or not 1<=limit<=20:raise Denied()
        if after is not None and (type(after) is not str or len(after)!=32 or
                any(c not in '0123456789abcdef' for c in after)):raise Denied()
        async with self._lock:
            if _authorize is not None:
                current=_authorize()
                if type(current) is not str or current!=owner:raise Denied()
            self._check()
            managed=self._store._managed
            try:
                boundary._check(managed._directory,managed._trust,managed._identity)
                ledger=managed._ledger
                with ledger._transaction():
                    rows=ledger._db.execute('SELECT task_id FROM stored_tasks WHERE owner_id=? AND task_id>? ORDER BY task_id ASC LIMIT ?',
                        (owner,after or '',limit+1)).fetchall()
                    records=tuple(ledger._record(ledger._db,owner,row[0]) for row in rows[:limit])
                return records, records[-1].task_id if len(rows)>limit else None
            except BaseException:self._poison();raise

    async def task_record(self, owner, task):
        """Owner-scoped metadata snapshot; never reads input or claims stop proof."""
        await self.require_owner(owner,task)
        async with self._lock:
            self._check()
            managed=self._store._managed
            try:
                boundary._check(managed._directory,managed._trust,managed._identity)
                ledger=managed._ledger
                with ledger._transaction():
                    return ledger._record(ledger._db,owner,task)
            except BaseException:self._poison();raise

    async def registered_input(self, owner, task):
        await self.require_owner(owner,task)
        async with self._lock:
            self._check()
            ledger=self._store._managed._ledger
            try:
                with ledger._transaction():
                    row=ledger._db.execute('SELECT state FROM stored_tasks WHERE task_id=? AND owner_id=?',(task,owner)).fetchone()
            except BaseException:self._poison();raise
            if row!=('STORED',):raise Denied()
            try:return self._store._managed.read(owner,task)
            except BaseException:self._poison();raise

    async def create(self, owner, key, binding, tokens):
        async with self._lock:
            self._check()
            # Exact same pure validation as snapshot publication, including bool/
            # subclass rejection. Equality alone is not validation on replay.
            _id(owner);_id(key)
            encode_input(binding,tokens)
            if binding.owner_id!=owner:raise LedgerRejected()
            # Same authority lock across preflight and publication/commit; excludes
            # only supported writers. No cross-process/bypass-writer guarantee.
            existing=None;conflict=False
            try:
                managed=self._store._managed
                boundary._check(managed._directory,managed._trust,managed._identity)
                ledger=managed._ledger
                with ledger._transaction():
                    # Versioned provenance discrimination precedes even equal-input
                    # replay. Expected conflicts are returned, not storage failures.
                    conflict=ledger._legacy_input_conflict(ledger._db,owner,key)
                    if not conflict:
                        prior=ledger._db.execute('SELECT task_id FROM input_requests WHERE owner_id=? AND idem_key=?',(owner,key)).fetchone()
                        if prior is not None:
                            existing,saved=ledger._metadata(owner,prior[0])
                            stored=storage.read_input(ledger._directory,existing.snapshot,saved)
                            conflict=saved!=binding or stored.tokens!=tokens
                        else:
                            conflict=ledger._db.execute('SELECT 1 FROM stored_tasks WHERE owner_id=? AND request_id=?',(owner,binding.request_id)).fetchone() is not None
            except BaseException:self._poison();raise
            if conflict:raise RegistrationConflict()
            if existing is not None:return existing
            try:return self._store.create(owner,key,binding,tokens)
            except BaseException:self._poison();raise

    async def bind_plan(self, owner, task, raw):
        async with self._lock:
            self._check()
            ledger=self._store._managed._ledger
            try:
                with ledger._transaction():
                    row=ledger._db.execute('SELECT state FROM stored_tasks WHERE task_id=? AND owner_id=?',(task,owner)).fetchone()
            except BaseException:self._poison();raise
            if row!=('STORED',):raise Denied()
            try:return self._store.bind_plan(owner,task,raw)
            except BaseException:self._poison();raise

    def _derive(self, owner, task, config):
        bundle,receipt,mapping=self._state(owner,task)
        # Only A hidden_states -> B edge. No arbitrary direction/port arguments.
        assignments=bundle['attempt']['assignments']
        stage=0 if self._mode=='hidden_states' else 1
        source=assignments[stage]['agent_id']
        destination=assignments[1]['agent_id'] if stage==0 else 'result-store'
        if stage==1 and self._mode!='framed_result_set_v1' and len(mapping)!=1:raise Denied()
        if destination!=self._recipient or source not in self._pins:raise Denied()
        if config.peers!={source:self._pins[source]}:raise Denied()
        scope=Scope(task,1,receipt.plan_digest,receipt.layout_digest,
                    'stage0->stage1' if stage==0 else 'stage1->result-store',
                    'hidden_states' if stage==0 else 'logits',bundle['attempt']['session_id'],
                    receipt.attempt_digest,source,self._pins[source],'unconnected')
        width=bundle['layout']['hidden_size' if stage==0 else 'vocab_size']
        tiles=tuple((start,start+count,(1,count,width)) for start,count in mapping)
        binding=ReceiveBinding(scope,stage,receipt.model_record_digest,
                               bundle['layout']['layout_schema_version'],'float32',tiles)
        return binding,receipt.bundle_digest

    async def register(self, owner, task, reader, writer, config):
        receiver=None;pending=None
        try:
            async with self._lock:
                self._check()
                if self._mode in ('framed_prefill_logits_v1','framed_result_set_v1'):raise Denied()
                if len(self._receivers)+len(self._pending)>=16:raise Denied()
                binding,digest=self._derive(owner,task,config)
                ledger=self._store._managed._ledger
                # Consume once before touching application transport; failures burn it.
                try:existing=ledger._db.execute('SELECT 1 FROM delivery_registrations WHERE task_id=?',(task,)).fetchone()
                except BaseException:self._poison();raise
                if existing:raise Denied()
                try:
                    with ledger._transaction():
                        ledger._db.execute('INSERT INTO delivery_registrations VALUES(?,?)',(task,digest))
                except BaseException:self._poison();raise
                pending=object()
                self._pending[pending]=(asyncio.current_task(),writer)
            receiver=await accept_receiver(reader,writer,config,authority=LocalAuthority(),
                binding=binding,deadline=time.monotonic()+60,capacity=65536)
            async with self._lock:
                self._check()
                current,now=self._derive(owner,task,config)
                if current!=binding or now!=digest or receiver._fenced:raise Denied()
                key=object()
                self._receivers[key]=(receiver,owner,task,digest)
                return key
        except BaseException:
            if receiver is not None:receiver._fence()
            writer.close()
            if receiver is not None:await receiver.close()
            raise
        finally:
            if pending is not None:self._pending.pop(pending,None)

    async def describe(self, owner, key):
        async with self._lock:
            self._check()
            receiver,actual,*_=self._receivers[key]
            if actual!=owner:raise Denied()
            return receiver._binding  # Immutable structural policy, not a permit.

    async def _claim(self, owner, key, tile):
        while True:
            async with self._lock:
                self._check()
                receiver,actual,task,digest=self._receivers[key]
                if hasattr(receiver,'_middle_object'):raise Denied()
                if actual!=owner:raise Denied()
                try:
                    _,receipt,_=self._state(owner,task)
                    if receipt.bundle_digest!=digest:raise Denied()
                    if type(tile) is not int or tile!=receiver._next_claim or receiver._claimed or receiver._fenced:raise Denied()
                    receiver._event.clear()
                    if tile in receiver._queued:return receiver._claim_nowait(tile)
                except BaseException:
                    self._poison();raise
            # Recheck DB and local state under the same lock after waking.
            await receiver._event.wait()

    async def claim(self, owner, key, tile):
        if self._mode!='hidden_states':raise Denied()
        return await self._claim(owner,key,tile)

    def _fence_task(self, task):
        gate=getattr(self,'_task_consent_runtime_fence',None)
        if gate is not None:gate.invalidate(task)
        for receiver,_,actual,_ in self._receivers.values():
            if actual==task:receiver._fence()

    def _after_commit(self):
        """No external callback; explicit fault boundary for scoped tests."""

    async def cancel(self, owner, task, *, isolate=False):
        async with self._lock:
            self._check()
            # Invalid owner cannot change DB state; validation failure poisons this
            # trusted-internal authority (availability cost, no public API).
            ledger=self._store._managed._ledger
            try:
                with ledger._transaction():ledger._metadata(owner,task)
            except BaseException:
                # Includes bad-owner requests: conservative fail-closed authority.
                self._poison();raise
            try:
                self._fence_task(task)
                result=(self._store.quarantine if isolate else self._store.cancel)(owner,task)
                self._after_commit()
                return result
            except BaseException:self._poison();raise

    async def close(self):
        if asyncio.get_running_loop() is not self._loop:raise Denied()
        # An experimental lifecycle owner must reap its direct worker and fence
        # pending registrations before shared descriptors can be released. Join
        # its shielded close without holding our lock. Only that owner's private
        # close task may enter descriptor teardown. Unowned v11 is unchanged.
        owner=getattr(self,'_text_registration_owner',None)
        if owner is not None and asyncio.current_task() is not owner._close_task:
            await owner.close()
            return
        async with self._lock:
            gate=getattr(self,'_task_consent_runtime_fence',None)
            if gate is not None:gate.invalidate()
            if self._directory is None:return
            # Never close the shared connection underneath file work in progress.
            if self._middle_busy:raise Denied()
            if self._middle_enabled:
                ledger=self._store._managed._ledger
                with ledger._transaction():
                    ledger._db.execute("UPDATE middle_objects SET state='ISOLATED' WHERE state<>'CLEANED'")
                for h in self._middle_handles.values():
                    h._closed=True;h._failed=True
                    if h._fd is not None:
                        fd,h._fd=h._fd,None
                        try:os.close(fd)
                        except BaseException:self._cleanup_failed=True;self._poison();raise
                # Descriptor closure is NOT object cleanup; all charges remain.
                self._middle_handles.clear()
            self._poison()
            receivers=tuple(v[0] for v in self._receivers.values())
            pending=tuple(self._pending.values())
        # No authority lock across network cleanup. Keep flock until cleanup ends.
        for task,writer in pending:
            writer.close();task.cancel()
        if pending:await asyncio.gather(*(p[0] for p in pending),return_exceptions=True)
        for receiver in receivers:await receiver.close()
        async with self._lock:
            if self._cleanup_failed:raise Denied()
            if self._directory is not None:
                self._store.close()
                directory,self._directory=self._directory,None
                os.close(directory)

    async def _after_result_files(self):
        # Yield outside the lock so cancellation can win before final DB decision.
        ready=asyncio.Event()
        self._loop.call_soon(ready.set)
        await ready.wait()

    def _existing_result(self, owner, task, idem):
        try:
            managed=self._store._managed
            boundary._check(managed._directory,managed._trust,managed._identity)
            ledger=self._store._managed._ledger
            with ledger._transaction():
                metadata,_=ledger._metadata(owner,task)
                if metadata.state!='RESULT_STORED':return None
                row=ledger._db.execute("SELECT idem_key FROM artifact_tickets WHERE task_id=? AND owner_id=? AND state='STORED'",(task,owner)).fetchone()
                if row!=(idem,):raise Denied()
            # Reverify manifest, owner/bindings and actual files, no unverified receipt.
            return ledger.fetch(owner,task,lambda tile,block:None)
        except BaseException:
            self._poison();raise

    async def result_reference(self, owner, task, idem):
        async with self._lock:
            self._check()
            if self._mode not in ('prefill_logits_v1','framed_prefill_logits_v1','framed_result_set_v1'):raise Denied()
            result=self._existing_result(owner,task,idem)
            if result is None:raise Denied()
            return result

    async def receive_result(self, owner, key, idem):
        """One complete bounded logits byte frame; identity only from registry/DB.

        Owns its claimed Receipt through finally; no caller-supplied metadata,
        payload, hash, shape, task or attempt can substitute the received record.
        """
        held=None;receiver=None
        async with self._lock:
            self._check()
            if self._mode!='prefill_logits_v1':raise Denied()
            receiver,actual,task,bundle_digest=self._receivers[key]
            if actual!=owner:raise Denied()
            prior=self._existing_result(owner,task,idem)
            if prior is not None:return prior
        try:
            held=await self._claim(owner,key,0)
            async with self._lock:
                self._check()
                _,identity,mapping=self._state(owner,task)
                if identity.bundle_digest!=bundle_digest or len(mapping)!=1:raise Denied()
                payload=held.payload
                entries=((mapping[0][0],mapping[0][1],hashlib.sha256(payload).hexdigest()),)
                ledger=self._store._managed._ledger
                writer,record,raw,prior,retry=ledger._reserve(owner,task,idem,identity.layout_digest,entries)
                if retry:raise Denied()  # Concurrent consumption never reuses ownership.
            # No DB writes, worker thread or lock while writing ordinary files.
            directory=ledger._directory_for(writer,True)
            try:
                entry=record['tiles'][0]
                ledger._write(directory,entry['file'],(payload,),entry['bytes'],entry['sha256'],floats=True)
                ledger._write(directory,'manifest.json',(raw,),len(raw),hashlib.sha256(raw).hexdigest())
            finally:os.close(directory)
            ledger._verify_files(writer,record,raw)
            await self._after_result_files()
            async with self._lock:
                self._check()
                # Revalidation, ticket completion and terminal update share one
                # SQLite transaction, within the same authority lock as cancel.
                if receiver._fenced or time.monotonic()>=receiver._gate._deadline:raise Denied()
                managed=self._store._managed
                boundary._check(managed._directory,managed._trust,managed._identity)
                ledger._verify_files(writer,record,raw)
                def guard():
                    self._check()
                    if receiver._fenced or time.monotonic()>=receiver._gate._deadline:raise Denied()
                result=ledger._commit_published(owner,task,identity.layout_digest,entries,writer,raw,_guard=guard)
                self._after_commit()
                return result
        except BaseException:
            self._poison();raise
        finally:
            if held is not None:
                # Single-frame result connection ends; no ambiguous credit return.
                try:receiver._fence()
                finally:await held.release()

    async def register_framed_result(self, owner, task, reader, writer, config, idem, sha256):
        if self._mode!='framed_prefill_logits_v1':raise Denied()
        return await self._register_framed(owner,task,reader,writer,config,idem,(sha256,))

    async def register_result_set(self, owner, task, reader, writer, config, idem, digests):
        if self._mode!='framed_result_set_v1':raise Denied()
        return await self._register_framed(owner,task,reader,writer,config,idem,digests)

    async def _register_framed(self, owner, task, reader, writer, config, idem, digests):
        """Digest is an untrusted content declaration, never task/model authority."""
        receiver=None;pending=None;reserved=False
        try:
            async with self._lock:
                self._check()
                if len(self._receivers)+len(self._pending)>=16:raise Denied()
                if self._mode not in ('framed_prefill_logits_v1','framed_result_set_v1') or self._framed_busy:raise Denied()
                binding,bundle_digest=self._derive(owner,task,config)
                if type(digests) is not tuple or len(digests)!=len(binding.tiles):raise Denied()
                total=0;entries=[]
                for (start,end,shape),sha256 in zip(binding.tiles,digests):
                    size=4
                    for dim in shape:size*=dim
                    total+=size;entries.append((start,end-start,sha256))
                entries=tuple(entries)
                if not 1<=total<=framing.MAX_OBJECT:raise Denied()
                ledger=self._store._managed._ledger
                if ledger._db.execute('SELECT 1 FROM delivery_registrations WHERE task_id=?',(task,)).fetchone():raise Denied()
                try:
                    object_id,record,raw,_,retry=ledger._reserve(owner,task,idem,binding.scope.layout,entries)
                    if retry:raise Denied()
                    with ledger._transaction():
                        ledger._db.execute('INSERT INTO delivery_registrations VALUES(?,?)',(task,bundle_digest))
                except BaseException:self._poison();raise
                # Both durable quota and registration exist before Channel/credit.
                self._framed_busy=True;reserved=True
                if self._mode=='framed_result_set_v1':
                    set_digest=hashlib.sha256(b'opencompute/result-set/v1\0'+raw).hexdigest()
                    receiver=framing.ResultSetReceiver(reader,writer,config,binding,object_id,entries,set_digest)
                else:receiver=framing.ObjectReceiver(reader,writer,config,binding,object_id,total,digests[0])
                key=object()
                self._receivers[key]=(receiver,owner,task,bundle_digest)
                receiver.publication=(entries,object_id,record,raw,idem)
                pending=object();self._pending[pending]=(asyncio.current_task(),writer)
            await receiver._channel.start()
            async with self._lock:
                self._check();receiver.check()
                _,identity,_=self._state(owner,task)
                if identity.bundle_digest!=bundle_digest:raise Denied()
                return key
        except BaseException:
            if reserved:self._poison()
            if receiver is not None:receiver._fence()
            writer.close()
            if receiver is not None:await receiver.close()
            raise
        finally:
            if pending is not None:self._pending.pop(pending,None)

    async def object_context(self, owner, key):
        async with self._lock:
            self._check()
            receiver,actual,_,_=self._receivers[key]
            if self._mode!='framed_prefill_logits_v1' or actual!=owner:raise Denied()
            return receiver.context

    async def result_set_contexts(self, owner, key):
        async with self._lock:
            self._check()
            receiver,actual,_,_=self._receivers[key]
            if self._mode!='framed_result_set_v1' or actual!=owner:raise Denied()
            return receiver.contexts

    async def _after_result_tile(self, index):
        # A published Tile is partial storage, never task success.
        pass

    async def _after_object_frame(self):
        # Explicit no-op test observation point, never an injected runtime callback.
        pass

    def _close_object_fd(self, fd):
        # Never retry an uncertain close on a potentially reused descriptor.
        try:os.close(fd)
        except BaseException:
            self._cleanup_failed=True;self._poison();raise

    async def receive_framed_result(self, owner, key):
        fd=directory=None;receiver=None
        async with self._lock:
            self._check()
            if self._mode not in ('framed_prefill_logits_v1','framed_result_set_v1'):raise Denied()
            receiver,actual,task,bundle_digest=self._receivers[key]
            if actual!=owner or receiver.active or hasattr(receiver,'_middle_object'):raise Denied()
            try:receiver.check()
            except BaseException:self._poison();raise
            receiver.active=True
            pending=object();self._pending[pending]=(asyncio.current_task(),receiver._channel.w)
            entries,object_id,record,raw,idem=receiver.publication
            ledger=self._store._managed._ledger
        try:
            directory=ledger._directory_for(object_id,True)
            for tile_index,entry in enumerate(record['tiles']):
                fd=os.open('.pending-'+entry['file'],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
                digest=hashlib.sha256()
                while receiver.written<receiver.context.total_bytes:
                    # Network/credit waits never own the authority lock.
                    receiver.held=await receiver._channel.receive()
                    async with self._lock:
                        self._check();receiver.check()
                        _,identity,_=self._state(owner,task)
                        if identity.bundle_digest!=bundle_digest:raise Denied()
                        block=receiver.validate(receiver.held)
                    # Synchronous bounded ordinary write: no background write can
                    # survive cancellation/fence or race file cleanup.
                    if not all((x[0]&0x7f800000)!=0x7f800000 for x in struct.iter_unpack('<I',block)):raise Denied()
                    view=memoryview(block)
                    while view:
                        count=os.write(fd,view)
                        if count<=0:raise Denied()
                        view=view[count:]
                    digest.update(block);receiver.written+=len(block);receiver.frames+=1
                    block=view=None
                    await receiver.release_written_frame()
                    await self._after_object_frame()
                    self._check();receiver.check()
                if digest.hexdigest()!=receiver.context.sha256:raise Denied()
                os.fchmod(fd,0o400);os.fsync(fd)
                old,fd=fd,None;self._close_object_fd(old)
                os.link('.pending-'+entry['file'],entry['file'],src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
                os.unlink('.pending-'+entry['file'],dir_fd=directory);os.fsync(directory)
                ledger._read_file(directory,entry['file'],entry['bytes'],entry['sha256'])
                await self._after_result_tile(tile_index)
                self._check();receiver.check()
                if isinstance(receiver,framing.ResultSetReceiver):await receiver.published_tile()
            ledger._write(directory,'manifest.json',(raw,),len(raw),hashlib.sha256(raw).hexdigest())
            old,directory=directory,None;self._close_object_fd(old)
            ledger._verify_files(object_id,record,raw)
            await self._after_result_files()
            async with self._lock:
                self._check();receiver.check()
                managed=self._store._managed
                boundary._check(managed._directory,managed._trust,managed._identity)
                ledger._verify_files(object_id,record,raw)
                def guard():self._check();receiver.check()
                result=ledger._commit_published(owner,task,receiver.context.binding.scope.layout,entries,object_id,raw,_guard=guard)
                self._after_commit()
                return result
        except BaseException:self._poison();raise
        finally:
            # No asynchronous/background writes survive cleanup. Retain partial
            # files and quota. Uncertain descriptor cleanup poisons and pins flock.
            try:
                try:
                    if fd is not None:
                        old,fd=fd,None;self._close_object_fd(old)
                finally:
                    if directory is not None:
                        old,directory=directory,None;self._close_object_fd(old)
                receiver._fence()
                if receiver.held is not None:
                    receiver.held.envelope=None;receiver.held=None
                self._framed_busy=False
            except BaseException:
                self._cleanup_failed=True;self._poison();raise
            finally:self._pending.pop(pending,None)

    async def _download_result(self, owner, task, sink, *, _authorize, max_bytes, metadata):
        """Private owner-only read; expected refusals never fence other users."""
        async with self._lock:
            if _authorize()!=owner:raise Denied()
            self._check()
            if self._mode not in ('framed_prefill_logits_v1','framed_result_set_v1'):raise Denied()
            managed=self._store._managed;ledger=managed._ledger
            try:
                boundary._check(managed._directory,managed._trust,managed._identity)
                with ledger._transaction():
                    row=ledger._db.execute('SELECT state FROM stored_tasks WHERE task_id=? AND owner_id=?',(task,owner)).fetchone()
                    tickets=ledger._db.execute("SELECT idem_key FROM artifact_tickets WHERE task_id=? AND owner_id=? AND state='STORED'",(task,owner)).fetchall()
            except BaseException:self._poison();raise
            if row!=('RESULT_STORED',) or len(tickets)!=1:raise Denied()
            # fetch independently revalidates owner, committed ticket, canonical
            # manifest/bindings and every file; no user-provided upload idem.
            try:result=ledger.fetch(owner,task,sink,max_bytes=max_bytes,metadata=metadata)
            except storage.artifacts.DownloadRefused:raise Denied() from None
            except BaseException:self._poison();raise
            if _authorize()!=owner:raise Denied()
            return result

    async def read_result(self, owner, task, idem, sink):
        """Trusted local sink; partial bytes provisional until successful return."""
        async with self._lock:
            self._check()
            if self._mode not in ('framed_prefill_logits_v1','framed_result_set_v1'):raise Denied()
            result=self._existing_result(owner,task,idem)
            if result is None:raise Denied()
            try:return self._store._managed._ledger.fetch(owner,task,sink)
            except BaseException:self._poison();raise

    async def authorize_execution(self,*args):raise Denied()
    async def publish(self,*args):raise Denied()
    async def record_pending(self,*args):raise Denied()


def open_authority(location,input_fd,result_fd,limits,*,deployment=None,
                   recipient=None,certificate_pins=None,initialize=False,delivery_mode='hidden_states',enable_drafts=False,enable_middle=False,enable_middle_loopback=False,enable_draft_sources=False,enable_unified=False):
    """Trusted bootstrap only. No production caller. Fresh connections on each open."""
    return _assemble_authority(**locals())


def _assemble_authority(location,input_fd,result_fd,limits,*,deployment=None,
                   recipient=None,certificate_pins=None,initialize=False,delivery_mode='hidden_states',enable_drafts=False,enable_middle=False,enable_middle_loopback=False,enable_draft_sources=False,enable_unified=False,_fresh_ledger=None):
    """Trusted bootstrap only. No production caller. Fresh connections on each open."""
    directory=None;store=None
    try:
        loop=asyncio.get_running_loop()
        if _fresh_ledger is not None and (initialize is not True or enable_unified is not True):raise Denied()
        if type(enable_unified) is not bool:raise Denied()
        # A new explicit assembly, never reinterpret combinations of old flags.
        if enable_unified:
            if enable_drafts is not False or enable_middle is not False or enable_draft_sources is not False:raise Denied()
        if type(enable_drafts) is not bool or type(enable_middle) is not bool or (enable_middle and enable_drafts):raise Denied()
        if type(enable_draft_sources) is not bool or (enable_draft_sources and not enable_drafts):raise Denied()
        if type(enable_middle_loopback) is not bool or (enable_middle_loopback and not (enable_middle or enable_unified)):raise Denied()
        if type(deployment) is not ControlledDeployment or deployment.no_bypass_writers is not True:raise Denied()
        if type(recipient) is not str or not recipient or type(certificate_pins) is not dict or not 1<=len(certificate_pins)<=16:raise Denied()
        for node,pin in certificate_pins.items():
            if type(node) is not str or not node or type(pin) is not str or len(pin)!=64 or any(c not in '0123456789abcdef' for c in pin):raise Denied()
        if type(initialize) is not bool or delivery_mode not in ('hidden_states','prefill_logits_v1','framed_prefill_logits_v1','framed_result_set_v1'):raise Denied()
        directory=boundary._directory(location)
        fcntl.flock(directory,fcntl.LOCK_EX|fcntl.LOCK_NB)
        def factory(connection,snapshot_fd,*,initialize):
            cls=(_fresh_ledger or _UnifiedLedger) if enable_unified else (_DraftSourceLedger if enable_draft_sources else (_MiddleLedger if enable_middle else (_DraftLedger if enable_drafts else _Ledger)))
            return cls(connection,snapshot_fd,result_fd=result_fd,limits=limits,initialize=initialize)
        store=storage.BoundResultStore(boundary._open(location,input_fd,initialize,_factory=factory))
        if (os.fstat(directory).st_dev,os.fstat(directory).st_ino)!=(os.fstat(store._managed._directory).st_dev,os.fstat(store._managed._directory).st_ino):raise Denied()
        result=object.__new__(Authority)
        result._loop=loop;result._lock=asyncio.Lock();result._store=store
        result._directory=directory;result._poisoned=False;result._receivers={};result._pending={}
        result._middle_enabled=enable_middle or enable_unified;result._middle_busy=set();result._middle_handles={}
        result._middle_network_enabled=enable_middle_loopback
        result._pins=dict(certificate_pins);result._recipient=recipient;result._mode=delivery_mode;result._framed_busy=False;result._cleanup_failed=False
        if result._middle_enabled:
            with store._managed._ledger._transaction():
                store._managed._ledger._db.execute("UPDATE middle_objects SET state='ISOLATED' WHERE state<>'CLEANED'")
        return result
    except BaseException:
        try:
            if store is not None:store.close()
        finally:
            if directory is not None:
                old,directory=directory,None
                os.close(old)
        raise
