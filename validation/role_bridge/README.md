# Coordinated async role bridge — private, default deny

Base: 78568aa50c9ddf64b54e3ff766d53401ad03ebb6. This increment connects existing
objects; it adds no public invocation, listener, credentials or execution grant.

The private one-token entry now requires the exact sender and receiver issued
by the same live NonceCoordinator reservation. The coordinator retains original
object/fence/path/connection bindings and checks their permanent session/stage
slots. A plain LocalAttemptJournal, foreign issued object, reopened registry,
changed slot or replaced callback cannot substitute. Provenance checking is
local trusted assembly qualification, not owner authentication or global replay
protection against copied/rolled-back databases. No schema migration is added.

AsyncRoleBridge is an explicit alternative to the prior synchronous validation
conduit. It owns already-established streams/Channels on one event loop:

1. A sends its actual committed hidden Envelope through Channel/send_handoff.
   B uses the fixed BoundReceiptConnection and coordinated receipt journal.
   Its real DeliveryAttempt records the frame and releases its credit once.
   The private entry does not record this receipt a second time through raw bytes.
2. Stage 1's existing bounded producer seals its source spool and completes
   registration/authorization exit before transmission. Synchronous projection
   is not executed on a background thread merely to adapt network I/O.
3. C reserves the actual artifact ticket before starting ObjectReceiver/credit.
   B sends fixed frames from the sealed spool using the receiver's actual context.
   C validates and writes bounded blocks directly into that reserved artifact
   directory, then publishes its existing manifest and final ledger transaction.
   No additional whole-object intermediate file is introduced.

The hidden ingress must already be ready through the gated receipt preparation
path, preserving its complete 64 KiB frame admission. Hidden endpoints are
limited to <=65536 bytes/one tile; result sender to <=32768+48 bytes/one tile.
These limits and connection snapshots are rechecked, not merely assumed from
fixtures. The async host demand explicitly includes all four endpoint payload
capacities (196704 bytes at these maxima) instead of the sync-only 64 KiB
coalescing buffer. The former 262144-byte test host policy is refused before
load/reservation; full async tests explicitly declare 524288 bytes. This does
not raise output/Book limits or claim to cover every Envelope/asyncio/SSL/OS
copy and allocator peak; real resource admission still must account for them.
Source/destination Books remain charged; artifact storage applies its
own independent limits. This is not atomic reservation across devices.

The composite Book connection binding includes independent ingress/egress
identities. It is not confused with ObjectReceiver's TLS channel-binding-derived
object context/tag. Certificate and channel metadata in tests are invented;
real Channel validation runs, but no actual TLS handshake is proven. Prepared
receipt readiness and H/credit frames never grant execution: its fixed
_deny_execution remains unchanged. The separate private compute authority,
resource service and device completion providers are still absent by default.

All send/receive/start tasks must be owned and settled on cancellation/failure.
Credit follows bounded write; the final result frame's slot is retired rather
than returned for another frame. Partial files and charges survive failure.
Uncertain close is not retried on a potentially reused descriptor. Publication
uses a synchronous final authority fence and source/connection recheck around
the existing ledger commit; no network await occurs inside that transaction.
External authority providers must not hold thread/SQLite locks across the
outer asynchronous pipeline scope. Separate databases are not one transaction.

The current bridge owns both ends in one local bootstrap and passes receiver
context locally. It does not implement authenticated remote context negotiation,
a distributed job scheduler or a three-host launcher. No arbitrary ledger
subclass acceptance or controlled_delivery_authority execution enablement is
added. A concrete remote bootstrap must eventually replace local endpoint/context
ownership while preserving these exact guards and durable ambiguous-outcome
queries; reconnect must never regrant old computation.

Tests use actual coordinator, PreparedReceiptConnection, Channel,
BoundReceiptConnection, ObjectReceiver, SQLite and artifact files with memory
StreamReaders and SSL metadata doubles. Computation/auth/resource/device
providers remain synthetic. The full private entry test stores/fetches one
151936-value/607744-byte synthetic output. Event barriers cover failure timing;
no sleep establishes a race outcome. Explicit audit/import guards prohibit
framework imports, network connect/bind/DNS and subprocess/system calls.

These tests establish local role composition and bounded logical ownership,
not numerical model correctness, peak RSS/GPU capacity, real cross-device
identity, process-crash durability, forced cancellation or physical device stop.
