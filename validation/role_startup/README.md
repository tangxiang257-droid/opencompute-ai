# Private role startup wiring

Fixed baseline73dd99e09a174d0d477de9ff351e69b529e89194. Supplied memory streams
and synthetic dependencies only. No process launcher, listener, actual TLS dial,
model/GPU/worker invocation, execution gate enablement or resource service added.

RoleStartup.create validates required local installation/runtime dependencies,
assignment and completion method contracts before constructing its issuance Channel.
It owns the supplied writer even on rejection. Method presence is not proof of real
identity, permission, physical resources or device completion. Actual deployments
must supply genuine providers; missing providers deny. No default synthetic provider.

_accept_unstarted validates supplied TLS stream metadata and creates a reader task,
but sends no H. _connect_unstarted provides the equivalent private dial seam; tests
replace the dial/context with ordinary doubles and perform no actual connection.
Legacy accept/connect still advertise H; their default ordinary repeated-start
behavior remains unchanged. Private seams and issuance use single-start mode.
Issuance rejects already-started channels, preventing connect+install double H.
Attempted private H is consumed before await; failed/cancelled start never retries.

RoleStartup installs through the actual IssuanceClient and original Channel, then
actual sender or PreparedReceiptConnection.prepare_online. B keeps _deny_execution
and emits no H until durable installation/journal/plan preparation. Runtime remains
the existing operations/control/hidden/result chain, not an alternate executor.
The source/assignment/resource bindings and all byte/frame/aggregate limits remain.

A stage1 valid prepared object transfers to startup at install. Successful close
or failure closes its transport and fences its journal, including while a prepare
barrier is pending. Journal.close is a logical fence; SQLite store connections are
per transaction, not a persistent handle. Already admitted computation, if any, is
not physically stopped by closing metadata/transports. Caller must first own and
quiesce work under the existing device/operation contract.

Caller owns tasks invoking install and must cancel/join them; startup.close drains
its own cleanup task/readers and fences journals but does not auto-join an external
install coroutine. Tests explicitly release/cancel/join event-blocked tasks. Cleanup
is shielded against repeated cancellation; reader tasks are drained even when a
writer.close raises. Unknown records and charges are never refunded or recovered.

Tests exercise actual ordinary-byte online A→B→persistent-result wiring, inherited
integration assertions rerun through this new bootstrap, missing/malformed providers,
zero-H rejection, original connection retention, prestarted refusal, H ordering,
B barrier cancellation and closing pending preparation. This proves no real TLS,
identity authority, model numerics, device completion, physical memory capacity,
cross-host process behavior, or distributed atomic reservation.
