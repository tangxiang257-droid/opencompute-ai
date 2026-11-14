# Private CUDA completion provider — disabled without trusted dependencies

Base c78221f2e7131890e930529b6794086ad79fa179. No model/GPU execution in this validation.

`DeviceCompletion` fills the existing runtime `device` slot for one role, one
request/token and one explicit `cuda:N`. It is not RoleInstallStore's device
permission service or an execution grant. Existing identity, task authority,
resources, installation device authorization and all startup gates still apply.
No default provider is installed. No eager Torch import; `hold_load` imports
Torch only after authority AND capacity `hold_device(spec, device, stage, action)`
fences admit it. These must be trusted, current services, not methods returning
None on an invented object. The capacity service must own a persistent EXCLUSIVE
allocation for the request and keep it across calls and failures; a context exit
must not release the allocation. The provider itself neither allocates memory nor
implements identity. `_runtime` is a private test injection, not a production
configuration, capability or proof of authority.

PinnedStageLoader is checked before entering load fences and again immediately
before calling load, to reject another device after callback mutation. The loader
must select the same explicit device; no CPU fallback. Its roots, hashes, layer
range and software/runtime version must be trusted bootstrap configuration. The
exact `torch.__version__` string (including any build suffix) must equal the
configured version; CUDA must be available and the index valid. This does not
validate driver compatibility or establish a tested Torch release. The test
version strings are synthetic, not a deployment recommendation.

One existing role scope covers load and backend work. The load fence performs
whole-device synchronization before/after loader and factory calls. A body error
poisons first, tries to fence partial loading, and preserves both errors if that
also fails. A runs its backend in the existing StageOwner thread; B runs on its
caller thread. `require_open` captures the first backend thread/current stream,
subsequent projection slices recheck it before linear. D2H and finish must use
that thread/stream. No CUDA context is held across await. Main-thread role
`require_finished` reads completion state without pretending it is the worker.

The conservative first implementation uses `torch.cuda.synchronize(device)`, not
an event on an unproven stream. Production and D2H copies are both fenced before
CPU bytes are read. This waits all streams of the current process on that device;
it is NOT isolation or stopping of other processes. Trusted loaded kernels must
not enqueue untracked work on other devices or after scope completion. No custom
background producer, CUDA graph or arbitrary backend is admitted by this design.
The external exclusive lease remains a prerequisite.

Pending tensors must be ordinary Torch Tensor, float32, explicit device,
(1,1,count), contiguous, offset zero and exact-size storage: no view retaining a
large logits backing. Stage B copies <=32768 bytes, A hidden <=65536. Copy is
blocking CPU `to(..., non_blocking=False, copy=True)` followed by an explicit
device fence. Encoding packs one CPU scalar at a time into a bounded bytearray
then bytes in LE FP32; it avoids whole-block Python float lists. This bounds
logical copy buffers, NOT full Python RSS, PyTorch allocator, KV or CUDA workspace.
Numerical finite checks and output semantics remain enforced by existing adapters
and driver; this provider does not attest that bytes are correct model logits.

Every failure permanently poisons the provider. Quarantine sets the stop event
without waiting for a worker synchronize; it never asserts physical stopping.
Pending/host/runtime references survive copy/sync/fence-exit failure. Successful
materialization clears them only after both gates exit successfully. Finish
becomes observable only after synchronization, and a concurrent poison always
reads UNKNOWN. There is no resource release, retry, reload or qualification
restoration API. The owning operation retains loader/backend on failure. If a
worker hangs, this provider does not supply a physical kill mechanism.

Tests exercise production provider logic with fake runtime; actual A/B local
operation wiring uses fake loaders/backends explicitly and ordinary SQLite/bytes.
AST-loaded projection tests use symbolic tensor methods, not a model forward.
Runtime tests call the actual `_TorchRuntime` methods with a fake Torch API bound
to an uninitialized runtime instance; real Torch is never imported. Barriers are
threading Events; timeout only prevents a test hang, never establishes ordering.

Official references read during design (2026-11-14):
- https://docs.pytorch.org/docs/main/notes/cuda.html (asynchrony, stream ownership, D2H completion)
- https://docs.pytorch.org/docs/stable/generated/torch.cuda.synchronize.html
- https://docs.pytorch.org/docs/2.15/generated/torch.Tensor.cpu.html

The stable docs redirected to 2.15 during review; this is a documentation
observation, not a tested runtime. Future explicitly authorized hardware tests
must pin PyTorch/CUDA/driver versions, test actual asynchronous errors/copies,
measure resource peaks and compare numerics before enabling execution. v12
execution prohibition and lack of real grant/capacity providers remain blockers.
