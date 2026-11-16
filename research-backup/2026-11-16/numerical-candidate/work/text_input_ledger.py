"""Fresh v12 INTERNAL metadata experiment, deliberately NOT factory-wired.

No tokenizer/Authority lifecycle integration or execution. Trusted caller supplies
ordinary metadata; this layer never labels tokenizer output or weights verified.
All helpers require the SAME connection and existing transaction. Files remain
outside SQLite atomicity. No migration; old v11 meanings remain unchanged.
"""
import hashlib
import controlled_delivery_authority as control
from snapshot_task_ledger import _id
from token_input_snapshot import InputBinding, SnapshotRef, read_input


def _hash(value):
    if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
        raise control.Denied()


class TextInputLedger(control._UnifiedLedger):
    VERSION=12
    SCHEMA=control._UnifiedLedger.SCHEMA+(
        "CREATE TABLE text_input_sources(task_id TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), owner_id TEXT NOT NULL, draft_id TEXT NOT NULL REFERENCES prompt_drafts(draft_id), draft_version INTEGER NOT NULL CHECK(typeof(draft_version)='integer' AND draft_version BETWEEN 1 AND 2147483647), text_sha256 TEXT NOT NULL, render_sha256 TEXT NOT NULL, token_blob_sha256 TEXT NOT NULL, contract_sha256 TEXT NOT NULL, snapshot_digest TEXT NOT NULL, tokenizer_status TEXT NOT NULL CHECK(tokenizer_status='internal_metadata_only'), model_status TEXT NOT NULL CHECK(model_status='unverified_declared'))",
        "CREATE TRIGGER text_source_binding BEFORE INSERT ON text_input_sources WHEN NOT EXISTS(SELECT 1 FROM stored_tasks t JOIN prompt_drafts d ON d.draft_id=NEW.draft_id JOIN task_inputs i ON i.task_id=t.task_id WHERE t.task_id=NEW.task_id AND t.owner_id=NEW.owner_id AND d.owner_id=NEW.owner_id AND d.version=NEW.draft_version AND d.deleted=0 AND i.snapshot_digest=NEW.snapshot_digest) BEGIN SELECT RAISE(ABORT,'binding'); END",
        "CREATE TRIGGER text_source_no_update BEFORE UPDATE ON text_input_sources BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER text_source_no_delete BEFORE DELETE ON text_input_sources BEGIN SELECT RAISE(ABORT,'immutable'); END",
        "CREATE TRIGGER text_source_no_plan BEFORE INSERT ON structural_plans WHEN EXISTS(SELECT 1 FROM text_input_sources WHERE task_id=NEW.task_id) BEGIN SELECT RAISE(ABORT,'text execution disabled'); END",
    )

    def _request(self,connection,owner,key,draft,version,contract):
        self._shared(connection)
        for value in (owner,key,draft):_id(value)
        if type(version) is not int or not 1<=version<=2147483647:raise control.Denied()
        _hash(contract)

    def replay_text(self,connection,owner,key,draft,version,contract):
        """Resolve immutable submission BEFORE reading current/deleted draft."""
        self._request(connection,owner,key,draft,version,contract)
        row=connection.execute('SELECT task_id FROM input_requests WHERE owner_id=? AND idem_key=?',(owner,key)).fetchone()
        if row is None:return None
        source=connection.execute('SELECT owner_id,draft_id,draft_version,contract_sha256 FROM text_input_sources WHERE task_id=?',row).fetchone()
        if source!=(owner,draft,version,contract):raise control.RegistrationConflict()
        task,binding=self._metadata(owner,row[0])
        read_input(self._directory,task.snapshot,binding)
        return task

    def _legacy_input_conflict(self,connection,owner,key):
        self._shared(connection)
        return connection.execute('SELECT 1 FROM input_requests r JOIN text_input_sources s ON s.task_id=r.task_id WHERE r.owner_id=? AND r.idem_key=?',(owner,key)).fetchone() is not None

    def _commit_published_input(self,connection,owner,key,binding,reference):
        """Legacy route cannot replay a text-origin request, even equal bytes."""
        if self._legacy_input_conflict(connection,owner,key):
            raise control.RegistrationConflict()
        return super()._commit_published_input(connection,owner,key,binding,reference)

    def commit_text(self,connection,owner,key,draft,version,raw,contract,render_sha256,binding,reference):
        prior=self.replay_text(connection,owner,key,draft,version,contract)
        if prior is not None:return prior
        if type(raw) is not bytes or not 1<=len(raw)<=8192:raise control.Denied()
        raw.decode('utf-8',errors='strict')
        _hash(render_sha256)
        if type(binding) is not InputBinding or type(reference) is not SnapshotRef:raise control.Denied()
        if binding.owner_id!=owner or binding.preprocessing_sha256!=contract:raise control.Denied()
        actual=connection.execute('SELECT body,version,deleted FROM prompt_drafts WHERE owner_id=? AND draft_id=?',(owner,draft)).fetchone()
        if actual!=(raw,version,0):raise control.DraftVersionConflict()
        stored=read_input(self._directory,reference,binding)
        # Explicit super dispatch only in this new route; no mutable bypass flag.
        task=super()._commit_published_input(connection,owner,key,binding,reference)
        connection.execute('INSERT INTO text_input_sources VALUES(?,?,?,?,?,?,?,?,?,?,?)',
            (task.task_id,owner,draft,version,hashlib.sha256(raw).hexdigest(),render_sha256,
             stored.token_blob_sha256,contract,reference.digest,'internal_metadata_only','unverified_declared'))
        return task

    def bind_plan(self,owner,task_id,raw):
        # All v12 plan binding is intentionally disabled in this isolated slice.
        raise control.Denied()
