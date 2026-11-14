# Retain owned launcher cancellation failures

Base: ae8b7593d13f14061cf906575db1ea1c18103c06. Candidate is not published.

Actual supervision callers already use StageLauncher/ReceivingLauncher.close.
Their shared _Owner.close used gather(return_exceptions=True) then discarded all
CancelledError outcomes. gather can replace cancellation with a fresh exception,
losing a cleanup failure stored in its cause/context. A real launcher with an
owned ordinary Event task demonstrated close returning success despite that task
raising cancellation from OSError. This is a shutdown evidence defect, not proof
of an actual GPU failure or measured resource leak.

The existing close now waits for its original pending task snapshot and reads
each original task.result once. It traverses exception groups, cause and context
with cycle detection; only a graph containing cancellation alone is ignored.
Non-cancellation evidence is retained in the original _close_task error. Other
owned endpoints are still closed; no new state machine, permission, restart,
quota release or timeout policy is added. Already-completed business tasks are
outside this pending-task snapshot, as before.

The test creates actual launchers through existing LAB installation/identity and
resource fixtures. Owned work is a plain asyncio Event task. RED records two
false clean shutdowns; GREEN checks the original exception object survives the
final error graph, including a nested group. Existing complete-chain callers
continue to call the unchanged close API. Real physical allocation, permissions,
CUDA completion and process death are not exercised. The same _Owner method is
used by receiving launchers; this candidate is not a C cross-host deployment test.

Remaining deployment path:
- Control attach/bootstrap and action authority exist; independent role listening
  hosts, purpose-specific endpoint configuration and process orchestration remain.
- Hidden/result streams and ResultIngress/ReceivingLauncher exist; real peer trust,
  routing and source/destination capacity must be configured and verified.
- A process supervisor must retain UNKNOWN ownership after stuck work or failed
  close; this fix makes its existing close result honest, but does not implement
  watchdog isolation, process kill, GPU stop, safe recovery or quota refund.
- thin_host.run_once and v12/v13 execution restrictions are unchanged. Missing
  real identity, permission, resources or device completion remain refusing.

Only guarded synthetic tests, no network listener/connect, nodes/models/devices.
