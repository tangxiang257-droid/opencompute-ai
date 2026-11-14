# Original bound local retirement after installation revocation

This private change separates a terminal local cleanup check from online execution
eligibility. It does not issue permission, recover a role, relax a source ledger,
release quota, or certify physical device termination.

On successful existing bind_control, the operation retains its canonical complete
control binding, spec digest and original local control/descriptor/provider/ticket/
journal references and quarantine methods. Existing begin retains the exact scope
before local claim/device-open, including partially failed local opens. Only the
completion.quarantine path may use these local pins. Wrong scope, binding or
observed reference/method drift is refused. Legacy operations without this binding
retain their original check. ready, begin, load, call, finish, read and export keep
the original online authorization behavior and reject revoked installations.

The original quarantine procedure remains permanent retirement, local SQL marks,
device/bridge quarantine, resource UNKNOWN, loader quarantine and backend retire.
It snapshots original callback methods and parameters before invoking callbacks,
so a callback cannot redirect the later resource retirement to a new object. All
actions continue after errors; errors and final binding drift are aggregated. The
old reservation remains charged. A failure is UNKNOWN, not safe release or proof
that computation has stopped. This is trusted same-process state, not a portable
permit, restart qualification, or defense against arbitrary memory modification.

A deterministic actual A control task is cancelled; its completion quarantine
entry revokes the original installation Channel before running the old callback.
The old callback is blocked by online identity validation; the fixed callback
retires the original scope. This does not prove other owner cleanup paths never
retired the operation: an initial too-weak test of op.retired alone passed on old
code because another cleanup path already retired it. The precise RED instead
checks that this completion callback returned after actual cleanup.

Ordinary tests cover A cancellation/revocation, B completed-scope revocation,
wrong binding/scope, provider substitution, device cleanup error/retained charge,
and callback redirection. Actual launchers/control/journals are used with LAB
identity/resource/compute providers and memory Channels. No model, GPU, socket
listener/connect, production credential or real node is used. The original
historical teardown's first channel-close cause remains undetermined. This is a
bounded repair to a reproduced ordering, not a declaration that all shutdowns or
C-to-user download are solved. The download mapping blocker is unchanged.

Additional callback boundary: CompletionBridge now retains its original device's
quarantine bound method at construction. An earlier local-retire callback that
changes bridge.device cannot redirect the subsequent bridge notification. The
operation's final pin check still reports that drift; the field is not restored
or treated as authorized. The seventh test proves zero foreign notifications and
one notification to the original provider with the original scope. This pins the
call target only: arbitrary mutable internals of a trusted provider and physical
completion cannot be inferred from invoking its method.
