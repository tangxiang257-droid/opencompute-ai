# Ordinary fixture cleanup ownership — no runtime/provider change

Base: 3c7cee2 plus frozen, unpublished control bootstrap 38f0a92f and installation
session 15d8b2ed. This increment changes only validation files. Models, runtime
execution gates, identities, quotas, authorization, device providers and PR state
remain untouched. It is not a physical-stop or production-host claim.

The prior Chain fixture registered shutdown with unittest before registering
RoleInstallStore, RoleResources, Book and ReceivingStore close callbacks.
Unittest runs these in LIFO order: the borrowed services closed first while two
control-loop jobs were still active. Cancellation then tried to quarantine using
closed databases. The original trace and readonly old-ledger evidence establish
this cause. Original initial-disconnection attribution remains uncertain.

The fixture now defers these dependency closers to one retained cleanup task:
1. Cancel and await still-active owned jobs, examining original task results.
2. Close/join every owned endpoint and tracked Channel reader.
3. Check for errors and unsettled jobs/readers. Unknown cleanup raises and retains
   this layer's deferred dependencies; it cannot be silently called successful.
4. Only after successful settlement close the dependencies in reverse order.

Caller cancellation cannot abandon the retained task. A shutdown failure before
join does not close a live owner's dependency. CancelledError with a non-cancel
cause/context is retained; gather(return_exceptions=True) is not used to inspect
cancelled tasks because it can discard that chain. Normal cancellation remains a
recorded/observable event, not automatically a cleanup failure.

Tests deliberately injecting one exact post-close exception can explicitly name
that object as expected_cleanup_error; no other exception is accepted. This is a
fixture-only assertion mechanism, not runtime permission or a general ignore flag.
Three existing fault tests now await their expected background role failure,
record its outcome and require completed launcher cleanup before teardown; they
no longer race a second teardown cancellation against that already failing work.
Their original failed regression logs remain in evidence.

The new tests assert that automatic cleanup enters with dependencies open, every
tracked job/reader/session monitor exits, launcher cleanup tasks have no error,
and retained A/B promises are UNKNOWN with charge 2097152 each. Source/destination
Book records retain charge 33168 each and become ISOLATED/retired=1. Original LIFO
readonly evidence instead had HELD/READING/RESERVED, not released charges. These
are synthetic fixed-fixture values, not device-memory allocation or refunds.

The error path retains this fixture layer's deferred handles; unittest may still
run older parent cleanup callbacks and TemporaryDirectory removal afterwards.
Do not interpret this as a durable process-wide ownership supervisor or permanent
file preservation. Tests explicitly settle artificial blocked jobs in their own
finally blocks. Real resource responsibility and crash handling remain external.

Only ordinary bytes, real local SQLite and in-memory Channels are exercised.
External account/TLS/authorization/resource/device/backend providers remain LAB
substitutes. No real model, GPU, node, network connect/listener, merge or deployment.
Historical and injected failures are preserved; successful results alone never
establish cleanup correctness or physical device completion.
