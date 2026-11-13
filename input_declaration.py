"""Private, read-only declaration of a registered v12 input, NOT admission.

Trusted bootstrap configures a service plus declared model/contract pins. They
are not accepted from HTTP callers. File integrity and metadata consistency do
not prove tokenizer correctness, model availability or execution authorization.
No persistent source-authority namespace or globally unique source is asserted.
"""
from dataclasses import dataclass

import noncompute_application_service as application
import pair_plan_contract as pair
from snapshot_task_ledger import _id
from text_input_ledger import TextInputLedger
from token_input_snapshot import read_input


class DeclarationRejected(ValueError):
    def __init__(self):
        super().__init__('input declaration unavailable')


def _need(condition):
    if not condition:
        raise DeclarationRejected()


def _hash(value):
    _need(type(value) is str and len(value) == 64
          and all(c in '0123456789abcdef' for c in value))


@dataclass(frozen=True, slots=True)
class DeclaredModel:
    config_sha256: str
    weights_sha256: str
    semantics: str
    weight_format: str
    compute_dtype: str
    kv_dtype: str
    activation_dtype: str
    logits_dtype: str


@dataclass(frozen=True, slots=True)
class InputDeclaration:
    """Constructible data, never a Task, Session, reservation or capability.

    text/render hashes are registered declarations, not newly recomputed proofs.
    No raw text, token payload, credential, storage path or mutable record escapes.
    """
    task_id: str
    owner_id: str
    request_id: str
    draft_id: str
    draft_version: int
    text_sha256: str
    render_sha256: str
    snapshot_digest: str
    token_blob_sha256: str
    token_count: int
    vocab_size: int
    preprocessing_version: str
    contract_sha256: str
    model_record_digest: str
    model: DeclaredModel
    token_encoding: str = 'int64-le'
    position_mode: str = 'contiguous-zero'
    mask_mode: str = 'causal-unpadded'
    batch_size: int = 1
    tokenizer_status: str = 'internal_metadata_only'
    model_status: str = 'unverified_declared'
    execution_allowed: bool = False
    computation_verified: bool = False


class InputDeclarationReader:
    """Opt-in local configuration; not mounted or imported by any default app.

    Caller must provide a genuine USER verifier on the existing service. Read
    success verifies only at specific checkpoints, not perpetual authorization.
    No database lifecycle, migration, reset, write, deletion or retry is added.
    """
    def __init__(self, *, service=None, model=None,
                 preprocessing_version=None, contract_sha256=None):
        self._service = None
        self._authority = None
        if service is None:
            _need(model is None and preprocessing_version is None and contract_sha256 is None)
            return
        try:
            _need(type(service) is application.ApplicationService)
            # Copy only exact primitive fields. Never retain a mutable caller map.
            fields = ('config_sha256', 'weights_sha256', 'semantics', 'weight_format',
                      'compute_dtype', 'kv_dtype', 'activation_dtype', 'logits_dtype')
            pair._record(model, 'ModelIdentity', ' '.join(fields))
            _need(all(type(model[k]) is str for k in fields))
            _hash(model['config_sha256']); _hash(model['weights_sha256'])
            _need(model['semantics'] == 'stock-qwen2-full-causal-v1')
            _need(model['weight_format'] == 'safetensors-single')
            _need(all(model[k] == 'float32' for k in fields[4:]))
            _id(preprocessing_version); _hash(contract_sha256)
            self._model = DeclaredModel(*(model[k] for k in fields))
            self._model_record_digest = pair._digest(dict(model))
            self._preprocessing_version = preprocessing_version
            self._contract_sha256 = contract_sha256
            self._service = service
            self._authority = service._authority
            return
        except Exception:
            pass
        raise DeclarationRejected()

    async def prepare_input_declaration(self, credential, task_id):
        try:
            return await self._prepare(credential, task_id)
        except Exception:
            pass
        # Fixed public error, without a chained storage/identity exception.
        raise DeclarationRejected()

    async def _prepare(self, credential, task_id):
        service = self._service
        _need(service is not None)
        owner = service._download_owner(credential)
        application._task(task_id)
        authority = self._authority
        _need(service._authority is authority)
        async with authority._lock:
            return self._read_locked(credential, task_id, owner)

    def _read_locked(self, credential, task_id, expected_owner):
        """Private synchronous core; callers own Authority's lock, never await.

        A locked asyncio.Lock is a misuse check, not proof of which task owns it.
        Only the trusted reader/bridge assembly calls this method. No public
        already-locked flag or claim of hostile same-UID isolation is provided.
        """
        service = self._service
        _need(service is not None)
        authority = self._authority
        _need(service._authority is authority)
        _need(authority._lock.locked())
        authority._check()
        application._task(task_id)
        owner = expected_owner
        _need(service._download_owner(credential) == owner)
        ledger = authority._middle_ledger()  # Includes live/path trust gates.
        _need(type(ledger) is TextInputLedger)
        with ledger._transaction():
            row = ledger._db.execute(
                'SELECT state FROM stored_tasks WHERE task_id=? AND owner_id=?',
                (task_id, owner)).fetchone()
            _need(row == ('STORED',))
            source = ledger._db.execute(
                'SELECT owner_id,draft_id,draft_version,text_sha256,render_sha256,'
                'token_blob_sha256,contract_sha256,snapshot_digest,'
                'tokenizer_status,model_status FROM text_input_sources WHERE task_id=?',
                (task_id,)).fetchone()
            _need(source is not None)
            (source_owner, draft, version, text_hash, render_hash, token_hash,
             contract_hash, snapshot_hash, tokenizer_status, model_status) = source
            _need(source_owner == owner)
            _id(draft)
            _need(type(version) is int and 1 <= version <= 2147483647)
            for value in (text_hash, render_hash, token_hash, contract_hash, snapshot_hash):
                _hash(value)
            _need(tokenizer_status == 'internal_metadata_only'
                  and model_status == 'unverified_declared')
            _need(ledger._db.execute('SELECT 1 FROM structural_plans WHERE task_id=?',
                                     (task_id,)).fetchone() is None)
            task, binding = ledger._metadata(owner, task_id)
            _need(task.state == 'STORED' and task.snapshot.digest == snapshot_hash)
            _need(binding.preprocessing_sha256 == contract_hash == self._contract_sha256)
            _need(binding.preprocessing_version == self._preprocessing_version)
            _need(binding.model_digest == self._model_record_digest)
            stored = read_input(ledger._directory, task.snapshot, binding)
            _need(stored.token_blob_sha256 == token_hash)
            value = InputDeclaration(
                task_id, owner, task.request_id, draft, version, text_hash,
                render_hash, snapshot_hash, token_hash, len(stored.tokens),
                binding.vocab_size, binding.preprocessing_version, contract_hash,
                self._model_record_digest, self._model)
        # No await or external sink between final recheck and return. This
        # still does not grant future execution or cross-store uniqueness.
        _need(service._download_owner(credential) == owner)
        _need(service._authority is authority)
        authority._check()
        return value
