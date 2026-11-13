"""Fresh v5 structural binding over v4 storage; no execution authority.

Actual snapshot bytes are verified, then mapped into the plan's separate digest
namespace. Model/layout/node claims are only structurally bound, not attested.
No model assets, network, worker or scheduler imports. No v4 migration.
"""
from dataclasses import dataclass
import json

import pair_plan_contract as contract
import result_artifacts as artifacts
import ledger_database as boundary
from snapshot_task_ledger import LedgerRejected, _need
from token_input_snapshot import read_input


@dataclass(frozen=True, slots=True)
class StructuralBinding:
    snapshot_ref: str
    token_sha256: str
    plan_input_digest: str
    model_record_digest: str
    layout_digest: str
    plan_digest: str
    attempt_digest: str
    bundle_digest: str
    status: str = 'structure_only_assets_and_nodes_unverified'
    execution_allowed: bool = False


_SCHEMA = artifacts._Ledger.SCHEMA + (
    'CREATE TABLE structural_plans(task_id TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), bundle BLOB NOT NULL CHECK(typeof(bundle)=\'blob\' AND length(bundle)<=65536))',
    "CREATE TRIGGER structural_plans_no_update BEFORE UPDATE ON structural_plans BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER structural_plans_no_delete BEFORE DELETE ON structural_plans BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER structural_plan_guard BEFORE INSERT ON structural_plans WHEN NOT EXISTS(SELECT 1 FROM stored_tasks t JOIN inert_attempts a ON a.task_id=t.task_id WHERE t.task_id=NEW.task_id AND t.state='STORED' AND a.state='PENDING') BEGIN SELECT RAISE(ABORT,'binding'); END",
)


class _Ledger(artifacts._Ledger):
    VERSION = 5
    SCHEMA = _SCHEMA

    def _pending(self, *args):
        # Unbound arbitrary plan_claim registration is unavailable in v5.
        del args
        raise LedgerRejected()

    def _validated(self, owner, task_id, raw):
        parsed = contract.parse_plan_bundle(raw)
        bundle = json.loads(parsed.canonical_bundle)
        task, binding = self._metadata(owner,task_id)
        stored = read_input(self._directory,task.snapshot,binding)
        actual = {'kind':'InputSnapshot','version':1,'request_id':binding.request_id,
                  'token_blob_sha256':stored.token_blob_sha256,'token_count':len(stored.tokens),
                  'token_encoding':'int64-le','position_mode':'contiguous-zero',
                  'mask_mode':'causal-unpadded','batch_size':1}
        _need(bundle['input_snapshot']==actual)
        model_digest=contract._digest(bundle['model'])
        layout_digest=contract._digest(bundle['layout'])
        input_digest=contract._digest(actual)
        _need(binding.model_digest==model_digest)
        _need(bundle['layout']['vocab_size']==binding.vocab_size)
        plan=bundle['plan'];attempt=bundle['attempt']
        _need(plan['input_snapshot_digest']==input_digest)
        _need(plan['model_digest']==model_digest and plan['layout_digest']==layout_digest)
        _need(attempt['task_id']==task_id and type(attempt['epoch']) is int and attempt['epoch']==1)
        mapping=[];start=0
        while start<len(stored.tokens):
            count=min(plan['max_tile_tokens'],len(stored.tokens)-start)
            mapping.append((start,count));start+=count
            _need(len(mapping)<=64)
        assignments=tuple((a['agent_id'],a['device_uuid'],a['process_instance_id']) for a in attempt['assignments'])
        _need(assignments[0][0]!=assignments[1][0])
        receipt=StructuralBinding(task.snapshot.digest,stored.token_blob_sha256,input_digest,
                                  model_digest,layout_digest,parsed.plan_digest,parsed.attempt_digest,
                                  artifacts._digest(b'opencompute/structural-bundle/v1',parsed.canonical_bundle))
        return task,bundle,receipt,assignments,tuple(mapping)

    def bind_plan(self, owner, task_id, raw):
        with self._transaction():
            task,bundle,receipt,assignments,mapping=self._validated(owner,task_id,raw)
            _need(task.state=='STORED')
            attempt=bundle['attempt']
            existing=self._db.execute('SELECT bundle FROM structural_plans WHERE task_id=?',(task_id,)).fetchone()
            expected=(attempt['session_id'],1,receipt.plan_digest,receipt.snapshot_ref,'PENDING')
            old=self._db.execute('SELECT session_id,epoch,plan_claim,snapshot_ref,state FROM inert_attempts WHERE task_id=?',(task_id,)).fetchone()
            if existing is not None:
                _need(existing==(raw,) and old==expected)
                saved=self._db.execute('SELECT node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id',(task_id,)).fetchall()
                _need(tuple(saved)==assignments)
            else:
                _need(old is None)
                self._db.execute('INSERT INTO inert_attempts VALUES(?,?,?,?,?,?)',(task_id,*expected))
                for stage,(node,device,instance) in enumerate(assignments):
                    self._db.execute('INSERT INTO inert_holds VALUES(?,?,?,?,?)',(task_id,stage,node,device,instance))
                self._db.execute('INSERT INTO structural_plans VALUES(?,?)',(task_id,raw))
        return receipt

    def _binding(self, owner, task_id, layout, entries, *, historical=False):
        row=self._db.execute('SELECT bundle FROM structural_plans WHERE task_id=?',(task_id,)).fetchone()
        _need(row is not None)
        task,bundle,receipt,assignments,mapping=self._validated(owner,task_id,row[0])
        _need(layout==receipt.layout_digest)
        _need(type(entries) is tuple and len(entries)==len(mapping))
        _need(all(type(e) is tuple and len(e)==3 for e in entries))
        _need(tuple((e[0],e[1]) for e in entries)==mapping)
        a=bundle['attempt']
        saved=self._db.execute('SELECT session_id,epoch,plan_claim,snapshot_ref FROM inert_attempts WHERE task_id=?',(task_id,)).fetchone()
        _need(saved==(a['session_id'],1,receipt.plan_digest,receipt.snapshot_ref))
        saved_holds=self._db.execute('SELECT node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id',(task_id,)).fetchall()
        _need(tuple(saved_holds)==assignments)
        task,record,raw,charge=super()._binding(owner,task_id,layout,entries,historical=historical)
        record['structural_identity']={'bundle_digest':receipt.bundle_digest,
                                      'plan_input_record_digest':receipt.plan_input_digest,
                                      'model_record_digest':receipt.model_record_digest,
                                      'layout_record_digest':receipt.layout_digest,
                                      'attempt_record_digest':receipt.attempt_digest,
                                      'config_sha256_claim':bundle['model']['config_sha256'],
                                      'weights_sha256_claim':bundle['model']['weights_sha256'],
                                      'semantics':bundle['model']['semantics'],
                                      'assets_verified':False,'nodes_verified':False,'execution_allowed':False}
        return task,record,artifacts._encode(record),charge


class BoundResultStore(artifacts.ResultStore):
    def bind_plan(self,*args):
        try:return self._call('bind_plan',*args)
        except Exception:pass
        del args
        raise LedgerRejected()

    def authorize_execution(self,*args):
        # No asset verifier, authenticated node or launch authority is connected.
        del args
        raise LedgerRejected()


def _open(location,input_fd,result_fd,limits,initialize):
    try:
        def factory(connection,snapshot_fd,*,initialize):
            return _Ledger(connection,snapshot_fd,result_fd=result_fd,limits=limits,initialize=initialize)
        return BoundResultStore(boundary._open(location,input_fd,initialize,_factory=factory))
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
