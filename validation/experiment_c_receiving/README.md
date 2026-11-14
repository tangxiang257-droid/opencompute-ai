# Fresh experiment to existing C receiving path

Baseline 3082ac3f52cf0dbc13ed6ba1bd3fb4e24bd61e8d. Private, unmounted and
co-located only. No real user issuer, node, capacity, listener or execution is
configured. The published import increment is not altered in its quotas/bans.

The actual path is confirmation -> original online registration -> C input import
-> CReceivingPolicy.create_experiment -> existing ReceivingController/ReceivingStore
and ReceivingRole -> existing ResultIngress -> existing result file/manifest commit
-> existing result-published ACK. The new policy is an explicit branch of the exact
existing policy class, not duck typing or a conversion of old TaskConsent authority.

A private map retains only the original issuer live registration after every
registration/USER scope has exited successfully. Reopen, copied SQL, a dataclass
copy, a history receipt or an import receipt cannot recreate it. Creating a C view
requires original REGISTERED plus IMPORTED, current source owner/revision/snapshot,
independent C identity/capacity and explicit receiving/maintenance parent providers.
The descriptor's protocol version is explicitly mapped to existing C wire version1;
its consent digest still binds the complete immutable version2 experiment, copy
terms, receiver resource_id and original placements. Source v12/v13 execution bans
remain unchanged. ExperimentAdmission.hold still refuses claim/install/execute.

Runtime fences use synchronous actual ledger/snapshot checks and USER/task fences;
they do not acquire an async lock over network waits. Resource checks validate the
same import commitment, not new reserves. Providers must validate on entry and exit,
raise on revoked/unknown, and come from trusted configuration. Interface shape or a
LAB context manager proves no issuer authority or physical allocation. Maintenance
is separately limited to revoke/stop-ack after USER permission loss.

Three acknowledgements must not be conflated:
- PENDING is only registration. ACTIVE requires existing CommittedBInput from the
  actual B receipt; source token/input digests cannot substitute for hidden output.
- Existing CDispatchGate ACK is a B-start prerequisite and remains unchanged. This
  increment does not produce a fresh A/B execution grant or bypass that gate.
- ResultIngress result-published ACK follows actual durable result commit. Losing
  it can leave RESULT_STORED plus C UNKNOWN. No retry, reexecution or quota refund.

Tests run actual confirmation, import, C protocol, ResultIngress, ordinary files,
SQLite and metadata/data frame exchange using in-memory streams with invented SSL.
To reach ACTIVE without enabling real execution, the B CommittedBInput.read_committed
boundary is explicitly replaced with a LAB proof. That is NOT real installed-B proof,
real TLS, remote identity/revocation, cross-host execution or numerical validation.
The existing generic-B rejection remains in legacy regression tests. No model import,
GPU, node, listener, network socket, production or real private key export occurs.

Next actual blockers: fresh experiment A/B task authority and online claim/install
integration; real user issuer/confirmation and node mTLS configuration; remote current
C permission and durable resource provider; actual installed-B input-ready evidence;
platform/device allocation and completion. These cannot be supplied by copying this
co-located Python policy or SQLite to another host. UNKNOWN retains files/charge;
there is no distributed atomic reserve or global clone/rollback defense.

Independent counterexamples reproduced and corrected wrapper-provider revival,
cancelled-target receiving eligibility, and provider callback drift before entering
the protected body. Wrapper checks now run before/after entry, body and scope exit;
observed failure permanently seals the experiment view. Target task/attempt states
are checked, with a stored artifact ticket required for the final publication ACK.
The source reader now shares its entire synchronous validation core; its original
locked entry retains the async-lock misuse check. It does not use a shortened source
metadata check. Already-written data or committed records cannot be revoked back.
