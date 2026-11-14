# Private receipt-ready assembly (offline prototype)

This is an opt-in local Python assembly, not a default production feature, remote
permit, model operation or execution authorization. No socket or TLS handshake is
created by this helper. Supply an already authenticated stream; fake SSL metadata
in the tests is not evidence of TLS authentication.

```python
prepared = await PreparedReceiptConnection.create(reader, writer, config, grants)
journal = await prepared.prepare(coordinator, reservation, fresh_path,
                                 prepare_fence=optional_async_barrier)
receipt = await prepared.bound.receive_once(journal)
```

The helper constructs an exact `Channel`, then an exact `BoundReceiptConnection`.
The async factory retires and awaits stream cleanup if TLS metadata validation or
binding fails. `prepare` accepts an exact `NonceCoordinator` and uses its real
receiver factory. Its slot claim, fresh ledger and complete V2 plan registration
are completed before an optional asynchronous local preparation barrier. The
barrier's return value has no authority. No async context manager is accepted as
an authorization mechanism. After the barrier, the helper rechecks journal owner,
peer, complete stored context and plan in journal → peer → SQLite lock order.
Execution authorization is fixed to deny; a receipt is never execution permission.

Private states are monotonic: unprepared → preparing → ready → closed, with failure
able to go directly to closed. The opt-in `start` reserves its one advertisement
before any await, sends a single H capacity message and never retries an unknown
advertisement. It does not wait for the peer H or first data. During unprepared or
preparing, observing a D header rejects it before any read of its body, decode,
queue insertion or receipt. This does not identify when the peer sent buffered
bytes: data whose header is first processed after readiness cannot be classified
as earlier traffic.

A post-claim failure/cancellation burns the existing coordinator slot permanently;
there is no rebinding or recovery. Failures before a successful claim may leave no
slot, as with invalid peer preflight, but this helper/stream still cannot retry.
Construction/preparation failure closes its owned stream, and a created journal is
closed on preparation failure. After success, the caller owns normal journal and
Channel closure. No change is made to generic `accept`, `connect` or repeated
`Channel.start` behavior when the private gate is off.

The private helper defaults to 64 KiB payload capacity and one queued tile, rejects
larger overrides, and the gated reader additionally caps the *complete* D frame at
64 KiB before reading its body. Existing receipt limits remain in force. These are
logical wire/queue bounds, **not** process RSS or total allocator bounds.

## Time-of-check boundary

The owner/plan/ledger recheck occurs before waiting for the Channel write lock and
before H drain. A trusted external actor could change owner/plan state while that
await is pending without revoking the Channel. H is therefore a point-in-time
receipt-readiness statement, not a continuous owner authorization, an atomic
cross-device promise, or proof that state remains valid at the exact H write time.
Every actual receipt commit still checks the existing owner/peer/context fences.
No threading RLock or SQLite transaction is held across an await.

## Offline verification

Run only the new selected ordinary test module, not the real channel tests:

```
PYTHONPATH=.:tests python -W error::ResourceWarning -B tests/test_p1_prepared_receipt.py
```

The new test module installs audit rejection for socket bind/connect/DNS and
subprocess launch. It uses real Channel/coordinator/SQLite with in-memory streams
and fake TLS metadata, never keys, certificates, network listeners, Workers or
models. Initial missing-interface RED and separate behavioral RED logs are kept
outside the copied source tree. A deliberate in-memory late-check mutant reads
510 payload bytes after the D header while unprepared; the event-barrier test
rejects it. A separate pre-fix binding test demonstrated an unclosed writer after
binding failed; the async factory fixes that boundary.
