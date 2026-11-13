"""Private current-source registration, never execution or production wiring.

No production LocalUserFence implementation is shipped. Trusted local bootstrap
must integrate one with the SAME USER verifier used by ApplicationService. Its
class/shape is an interface contract, NOT evidence of authentic credentials or
revocation safety. Arbitrary same-UID Python is outside this boundary.
"""
from abc import ABC, abstractmethod
from contextlib import contextmanager
import asyncio
import inspect
import re
import secrets
import threading
import types

import noncompute_application_service as application
from input_declaration import InputDeclarationReader, DeclarationRejected
from token_input_snapshot import SnapshotRejected
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.nonce_coordinator import NonceCoordinator, _SourceAlreadyClaimed


class SourceBridgeRejected(ValueError):
    def __init__(self):
        super().__init__('source registration unavailable')


class LocalUserFence(ABC):
    """Trusted integration contract, not a production authenticator.

    verify must check actual USER issuer/audience/expiry/revocation and return
    the service's Principal. hold must serialize local revocation through exit,
    including lock waits. It is synchronous, non-suppressing, and leaf-only:
    never acquire Authority/coordinator/SQLite, call a request sink, or await.
    Time still passes while held; verify is repeated after the write and exit.
    No production adapter, trust switch, self-certified flag or default exists.
    """
    @abstractmethod
    def verify(self, credential):
        raise NotImplementedError

    @abstractmethod
    def hold(self, credential):
        raise NotImplementedError


def _need(condition):
    if not condition:
        raise SourceBridgeRejected()


class _CurrentSourceBridge:
    """No-argument construction is disabled. Use trusted private bootstrap.

    Pins current Authority/provider and one fresh source-only coordinator; no
    restart, recovery, historical source authentication or persistent grant.
    """
    def __init__(self):
        self._enabled = False
        self._closing = False
        self._failed = False
        self._co = None
        self._active = None
        self._cleanup_failed = False
        self._close_attempted = False

    def _check(self):
        _need(self._enabled and not self._closing and not self._failed)
        _need(asyncio.get_running_loop() is self._loop)
        _need(self._service._authority is self._authority)
        _need(self._service._verifier is self._provider)
        self._authority._check()

    def _owner(self, credential):
        self._check()
        owner = self._service._download_owner(credential)
        _need(owner == self._fixed_owner)
        return owner

    @contextmanager
    def _identity(self, credential):
        """Authority already held. Same scope covers co initialize/query/write."""
        self._check()
        _need(self._authority._lock.locked() and self._active is None)
        context = self._provider.hold(credential)
        if inspect.isawaitable(context):
            # Only an unstarted native coroutine can be closed without running
            # its body/finally. Never drive or close suspended/running coroutines
            # or custom awaitables; their lifecycle belongs to the provider.
            if (type(context) is types.CoroutineType
                    and inspect.getcoroutinestate(context) == inspect.CORO_CREATED):
                context.close()
            raise SourceBridgeRejected()
        try:
            with _non_suppressing(context):
                owner = self._owner(credential)
                self._active = (asyncio.current_task(), threading.get_ident(), credential, owner)
                try:
                    yield
                    self._owner(credential)
                finally:
                    self._active = None
            # Context exit can revoke/expire or invalidate its provider. A clean
            # __exit__ alone is not a successful final USER check.
            self._owner(credential)
        finally:
            self._active = None

    @contextmanager
    def _coordinator_owner(self):
        """Immutable co authorizer: no lock acquisition, callbacks or await.

        Only accesses the already-held identity context; never reacquire F or A
        from C. The provider's verify is required to be synchronous and leaf-only.
        """
        self._check()
        active = self._active
        _need(active is not None and active[0] is asyncio.current_task()
              and active[1] == threading.get_ident() and self._authority._lock.locked())
        owner = self._owner(active[2])
        _need(owner == active[3])
        yield owner
        _need(self._active is active)
        self._owner(active[2])

    def _retire(self):
        self._failed = True
        if self._co is not None and not self._close_attempted:
            self._close_attempted = True
            try:
                self._co.close()
            except BaseException:
                # No success and no detached retry; close may have had effects.
                self._cleanup_failed = True

    async def register_current_source(self, credential, task_id):
        """Request input is only credential/task_id, never owner/declaration/nonce."""
        try:
            self._owner(credential)
            application._task(task_id)
            async with self._authority._lock:
                self._check()
                rejected = False
                duplicate = False
                try:
                    with self._identity(credential):
                        # Catch ordinary source rejection INSIDE the fence so
                        # normal exit is still checked; no coordinator mutation.
                        try:
                            declaration = self._reader._read_locked(
                                credential, task_id, self._fixed_owner)
                        except (DeclarationRejected, SnapshotRejected):
                            rejected = True
                        if not rejected:
                            self._owner(credential)
                            nonce = secrets.token_hex(16)
                            try:
                                receipt = self._co.bind_source_declaration(declaration, nonce)
                            except _SourceAlreadyClaimed:
                                duplicate = True
                            self._owner(credential)
                    self._check()
                except BaseException:
                    self._retire()
                    raise
                if not rejected and not duplicate:
                    return receipt
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        raise SourceBridgeRejected()

    async def query_history(self, credential, task_id):
        """Only historical registration; never proof of authentic/current source."""
        try:
            self._owner(credential)
            application._task(task_id)
            async with self._authority._lock:
                try:
                    with self._identity(credential):
                        value = self._co.query_source(task_id)
                    self._check()
                    return value
                except BaseException:
                    self._retire()
                    raise
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        raise SourceBridgeRejected()

    async def close(self):
        if not self._enabled:
            return
        _need(asyncio.get_running_loop() is self._loop)
        self._closing = True
        async with self._authority._lock:
            self._retire()
        if self._cleanup_failed:
            raise SourceBridgeRejected()


async def _open_source_bridge(*, service=None, model=None, preprocessing_version=None,
                              contract_sha256=None, source_namespace=None,
                              coordinator_path=None, identity_fence=None, credential=None):
    """Trusted bootstrap only. Creates a fresh v2 file, never opens or replaces.

    namespace comes from stable local provisioning, not source path/dev/inode,
    request metadata or a claim of cross-restart source authority. Missing real
    local fence integration means callers MUST leave this path disabled.
    """
    bridge = _CurrentSourceBridge()
    try:
        _need(type(service) is application.ApplicationService)
        _need(isinstance(identity_fence, LocalUserFence)
              and identity_fence is service._verifier)
        _need(type(source_namespace) is str
              and re.fullmatch('[0-9a-f]{64}', source_namespace) is not None)
        _need(coordinator_path is not None)
        reader = InputDeclarationReader(service=service, model=model,
                  preprocessing_version=preprocessing_version, contract_sha256=contract_sha256)
        owner = service._download_owner(credential)
        authority = service._authority
        bridge._service = service
        bridge._authority = authority
        bridge._provider = identity_fence
        bridge._fixed_owner = owner
        bridge._loop = asyncio.get_running_loop()
        bridge._reader = reader
        bridge._enabled = True
        async with authority._lock:
            with bridge._identity(credential):
                bridge._co = NonceCoordinator.initialize(coordinator_path,
                    authorize=bridge._coordinator_owner, source_namespace=source_namespace)
            bridge._check()
        return bridge
    except BaseException as exc:
        bridge._retire()
        if isinstance(exc, asyncio.CancelledError):
            raise
    raise SourceBridgeRejected()
