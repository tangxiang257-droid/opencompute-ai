# Actual one-token operations with separated local role ownership

Fixed base 208debb01d320704dd2292b7aa40e846a74b80b7. Private and opt-in only.
No listeners, processes, models, credentials, real resource or device providers
are started or installed. Missing execution dependencies remain default-deny.

StageAOperation owns its coordinated sender/StageOwner and local load/completion;
StageBOperation owns its committed receipt, Driver/journal and source spool.
The old private_one_token composition now calls these SAME operations rather than
retaining duplicate stage implementations. Its legacy composite checks remain.
RegisteredRole binds operation completion to its local control journal. One scope
is used by role begin, backend pending materialization, Driver and finish; Driver's
optional explicit scope rejects reuse and retains old default generation behavior.
Load has its separate existing hold_load contract. Completion return values still
require real trusted implementation; they are not physical completion evidence.

Split hidden endpoints use existing PreparedReceiptConnection/Channel; the reverse
ACK includes complete fixed provenance plus the actual committed frame/payload
hash. It denies execution. A's hidden Channel must already have been started by
its local bootstrap; the prepared receiving Channel has already advertised credit.
Result endpoints use separate metadata and data Channels, each locally owned.
Canonical metadata binds fixed provenance, actual stage1 input, result binding,
size, digest and local TLS generation. Ingress independently reserves persistent
storage before creating ObjectReceiver/advertising data capacity. B independently
derives frame context from its local TLS binding, never reads the remote receiver
object. Ingress sends publication ACK only after its durable commit; ACK is not a
quota release, permission or proof of model numerics. Result channels start in the
endpoint operation and must not be pre-started by the legacy combined bridge.

B's two Spool Books remain LOCAL source accounting and destination-promise mirror.
The mirror is not the ingress database and is not proof of remote reservation.
Ingress separately charges its actual artifact ledger. Stable real remote resource
commitments are a required external provider obligation before computation. No
cross-node atomic reservation, measured RSS/GPU peak or refund mechanism is added.
Transport logical capacity must include hidden endpoints, result metadata/data
endpoints and control endpoints, plus all encoder/copy/OS/workspace overhead.

B opens export only after its local control journal reports COMMITTED, nonretired,
and the exact metadata digest; per-frame check_export rechecks local source,
control and completion. A export compares actual bytes to committed metadata too.
Failures/unknown keep local charges and permanently retire qualification. Caller
owns each role service/task lifetime and explicit endpoint close. Coordinator
cannot join or physically stop remote role work.

The end-to-end ordinary-byte test runs actual extracted operations, RegisteredRole,
ControlClient/Service, independent hidden/result endpoints and persistent ingress.
It reads back 607744 synthetic bytes; providers/backend are explicit doubles.
Role task contexts reject A accesses to B DB and vice versa during service calls
and transport, including completion hooks; this is test instrumentation, not OS
sandboxing or protection against malicious Python callbacks.

Bootstrap still uses the existing in-process NonceCoordinator issuance registry.
The expected stage1 digest is fixed from authenticated hidden ACK metadata by the
trusted test supervisor; distributing that expectation to a real ingress needs
real authenticated bootstrap/negotiation. This is role-local runtime isolation,
NOT proof of a cross-process/multi-host launcher or actual TLS authentication.
No true model forward, GPU, node, network or physical-stop experiment is run.
Global anti-replay, hostile same-UID mutation, crash/power-loss and real resource
or device completion services remain outside these synthetic results.
