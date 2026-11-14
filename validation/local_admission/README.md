# Private local admission skeleton: default deny, no production integration

Base: `077445d5f77af37357262ee62bf5f671ddb271d4`. Only this document,
`private_local_admission.py` and the explicit test module are added. No existing
code, workflow, route, dependency or default execution behavior changes.

No real IdentityAdapter is supplied. Missing adapter denies all user admission;
subclassing an interface or returning an Observation is not authentication.
SyntheticAdapter belongs only to these tests and must never be production wiring.
Real issuer/audience/signature/expiry/session validation, policy pins, remote
revocation freshness and event-gap policy remain trusted integration prerequisites.
No static shared key, default allow or capability flag substitutes for identity.

The local authority only serializes changes already received locally. It cannot
promise immediate remote IdP revocation. Fresh create writes configuration and
an event journal; reopen is history-only, never restores an active cache or grants
execution. Raw credentials are not persisted. No production service assembles
this provider. Source registration remains execution_allowed=False; v12 text
execution denial and all coordinator fresh-only/history restrictions are unchanged.

## Locks, lifetime and refusal

Bridge order remains Authority A -> local F -> coordinator C. Identity writers
take W -> short F to close admission -> release F -> their own SQLite transaction
and connection close -> F to validate and publish. No F is held while waiting W.
Adapter awaits happen outside these locks and are bound to starting boot/lifecycle,
global revision, policy and subject/session versions. Close invalidates generation,
drains entered writes before releasing the file lock, and cannot resurrect READY.
Cancelled write waiters poison admission and wait for owned work to settle; this
is not forced thread termination or a hard timeout. PID checks precede lock access,
including after the adapter await; forked objects must not be reused.

verify/hold perform synchronous lstat/fstat under F in addition to memory/time
checks. They never take W, open SQLite or call history. Stable file replacement,
missing paths or file-check OSError poison the instance, including cached admission
and actual bridge use. Restoring the original inode does not revive that instance.
This requires a trusted private parent directory: metadata checks do not eliminate
malicious TOCTOU, copied databases, rollback or independent authorities. File checks,
RLocks and SQLite are not preemptible or guaranteed to keep an event loop responsive.

An observed wall-clock expiry or monotonic freshness expiry deletes that cache
entry. Clock rollback cannot revive that observation. Later admission requires
a NEW trusted adapter observation and complete write/publication checks. This is
not general wall-clock rollback protection, a persistent credential denylist,
remote time verification or a guarantee about system suspend. No additional
credential-expiry-to-monotonic conversion is implemented.

Unknown commit, connection exit, publication or cancellation failures deny and
poison rather than retry. close consumes its fd ownership before the uncertain
OS boundary; CLOSE_FAILED refuses retry of a possibly reused descriptor number.
History can remain despite refusal. No compensating delete, nonce replacement,
automatic recovery, cross-database atomicity or global exactly-once is claimed.

BUSY during an existing bridge's post-preflight fence entry can permanently retire
that bridge/coordinator even if this provider later becomes READY. It is not merely
a temporary pause. Source commit followed by identity refusal can leave inert
source history; refusal does not imply no persistent effects.

## Verification and limits

Frozen R2 module SHA256:
`5c4afb2b07898dd27cbc1b0dc8f7b3ab74536de0920b80802e0dc57259b27196`.
Frozen test SHA256:
`f6ff7b8c1da2c1b68f2db53c12fac97ed1174df8c0e3af49e4ea07b2fe61a6a3`.

Implementation evidence: R1 missing-module RED; separate PID-before-lock,
file-binding and close-ownership RED/GREEN repairs. Independent review later
reproduced cached admission after file replacement and observed-expiry revival.
The three exact regressions failed on R1 (32 cases, 3 failures), then passed on
R2; four further lifecycle cases give 36 new-module tests. Implementation's
explicit suites passed 36+20+20+12+36=124. Independent review reported matching
R1/R2 hashes and patch, RED/GREEN, 36 new-module cases, 20 compatibility cases,
8 extra cases and two cross-process checks; it did not obtain the full evidence
archive or independently rerun all 124. These are ordinary-data results, not real
identity, model, GPU, network/node or product acceptance. Skipped CI is not passing CI.

Faults are wrappers before/after real SQLite commit, after connection/OS close,
synthetic clocks/PID, generation changes and thread/event barriers. They are not
power-loss or SQLite-engine/OS failure experiments. No real IdP, remote revocation,
hostile path race or device shutdown was tested. The synthetic source fixture
uses explicit test-only ledger assembly, never production setup.

Run only under an external guard installed BEFORE project imports. From repository
root with an existing Python 3.12 + Pydantic environment, this command runs exactly
the 36 ordinary tests, with no discovery or setup:

```bash
python -B -W error::RuntimeWarning -W error::ResourceWarning - <<'PY'
import sys, runpy, importlib.abc, builtins, resource
resource.setrlimit(resource.RLIMIT_CPU, (40,40))
resource.setrlimit(resource.RLIMIT_AS, (1536*1024**2,1536*1024**2))
resource.setrlimit(resource.RLIMIT_FSIZE, (32*1024**2,32*1024**2))
resource.setrlimit(resource.RLIMIT_NOFILE, (128,128))
def forbidden(name):
    return name.split('.')[0] in {'torch','transformers','safetensors','tensorflow','public_pool'} or any('worker' in p.lower() for p in name.split('.'))
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self,name,*args):
        if forbidden(name): raise RuntimeError('blocked import '+name)
original_import=builtins.__import__
def guarded_import(name,globals=None,locals=None,fromlist=(),level=0):
    if forbidden(name) or any('worker' in str(p).lower() for p in (fromlist or ())): raise RuntimeError('blocked framework/worker')
    return original_import(name,globals,locals,fromlist,level)
def audit(event,args):
    if event in {'socket.bind','socket.connect','socket.connect_ex','socket.getaddrinfo','socket.gethostbyname','socket.gethostbyaddr','socket.getnameinfo','subprocess.Popen','os.system','os.fork','os.forkpty','os.posix_spawn','os.exec','os.spawn'}: raise RuntimeError('blocked operation '+event)
sys.meta_path.insert(0,Block());builtins.__import__=guarded_import;sys.addaudithook(audit)
target='validation/local_admission/test_authority.py'
sys.argv=[target]
runpy.run_path(target,run_name='__main__')
PY
```

These are CPU/address-space/per-file/fd limits, not a hard RSS bound, total output
quota or wall-clock timeout. Guarding trusted Python is not a hostile-code sandbox.
