# Configured central action assembly

Base: 229ede57070c5174662d9ed46bdbb832cc00c67f.
`private_experiment_assembly.assemble_actions` is reusable private runtime code,
not a test provider, production launcher, permission issuer or device runner.
It constructs the existing ActionServer through create_action_server and owns
only its dedicated action channel and explicitly invoked service calls.

## Trust and inputs

The default is refusal (`enabled=False`). Explicit `True` requests construction;
it neither authenticates a provider nor grants execution. Trusted LOCAL bootstrap
must provide the independent configuration digest and actual original admission,
credential, CoordinatorControl, authenticated purpose channel, identity/clock
providers and the SAME central resource service used by original installation.
There are no module strings, dynamic imports, test-mode providers or defaults.

The bounded public JSON fixes:

- experiment/context digest and stage;
- role node/device/instance/resource ID and charge;
- full control-scope digest;
- actual control and action peer identities, including their distinct generations;
- original local issuer node/certificate configuration.

The issuer is not the remote role peer. Generations can be pinned only after the
actual connections exist. The expected digest must not come from a remote peer
or be calculated from untrusted input as a substitute for operator configuration.
The input contains no credential, private key, token payload or executable hook.

The adapter rechecks original bindings, provider objects/methods and current
identity/resource/permission fences. These checks cannot distinguish a lying
trusted provider from a genuine one; the host's provider selection remains a
trust boundary. Tests supplying LAB providers do not configure a real deployment.

## Ownership and failure

Malformed config, disabled entry, wrong bindings, missing providers and aliases
are rejected before channel take; unrelated/borrowed services are not closed or
retired. Accepted channels/writers are permanently occupied before the first
await. Existing control-bound/START/COMMIT records remain the only durable action
state. B construction still awaits actual authenticated committed-input proof.
No local identity/resource/SQLite fence spans a network wait.

After take, failure closes the original owned channel, preserves UNKNOWN and
charges, and never rebinds/retries. Provider exits are followed by a final binding
check. Calls and cleanup use pinned originals, so changed attributes cannot
redirect invocation or close a foreign authority/channel. No background service
loop or dispatch starts during construction. `serve_once` is explicit and single
owner; `close` seals before cancellation, joins owned work even if channel close
fails, retains repeated-cancellation cleanup, and aggregates cleanup errors.
Borrowed admission/control/resources are not closed or refunded. Remote peers may
independently retire their linked roles when this owned channel closes.

There is no hard guarantee for blocking synchronous callbacks. Close/timeout,
lease expiry and UNKNOWN are not physical device-stop evidence. The unchanged
5/4/1-second policy is experimental, and one SQLite authority path does not prevent
clone/rollback or a separately created authority root.

## Integration and tests

The configured entry replaces the direct action factory call in the existing
ordinary-byte A→B→C fixture. It constructs actual ActionAuthority/ActionServer,
uses actual installed/control records and B input-ready proof, and reaches C's
independent ACK gate and persistent result. The happy path asserts stored bytes;
negative tests assert no body call, retained rows, closed owned channels or joined
owned tasks. Events decide races; timeouts are safety bounds, not race evidence.

Identity issuer/certificate streams, external task/capacity policy, clock and
model/device/backend remain explicit LAB substitutes. No model/Torch/weights/GPU,
node or socket connect/listen was run. No production credentials or private keys
were created/exported; fixtures use temporary LAB credentials.
The guarded runner in the evidence bundle rejects those actions/imports.

## What this does not assemble

A single static configuration cannot currently produce the whole real experiment:

1. Real issuer/user resolver, task/node permission and platform capacity services
   are not configured; their real policy decisions cannot be invented here.
2. Role-local A/B launchers and C importer/receiver must be independently started
   with authenticated purpose streams and their own persistent resources.
3. B action qualification depends on A output actually reaching B; it cannot be
   constructed eagerly from the original token hash or copied central database.
4. Actual device/loader/completion providers require platform-specific evidence
   and separately authorized hardware validation.

`thin_host.run_once` remains unconditionally refused. This module is a concrete
central composition increment, not completion of production host assembly or
Phase 1's two-public-network GPU correctness/performance acceptance.
