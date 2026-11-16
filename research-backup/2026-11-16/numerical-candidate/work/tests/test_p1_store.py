import unittest
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from cluster_p1.contracts import Task, Session, Stage, Tile, State
from cluster_p1.store import P1Store, Conflict

H = "a" * 64


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "db"
        self.s = P1Store(self.path)
        self.s.create_task(
            Task(
                task_id="t", owner_id="u", model_hash=H, total_layers=4, payload_ref="x"
            ),
            event_id="task",
        )

    def plan(self, sid="s", **kw):
        args = dict(
            session_id=sid,
            task_id="t",
            owner_id="u",
            epoch=1,
            model_hash=H,
            layout_hash=H,
            selected_nodes=("n0", "n1"),
            expected_tokens=2,
        )
        args.update(kw)
        stages = [
            Stage(
                session_id=sid,
                stage_id=i,
                layer_start=i * 2,
                layer_end=i * 2 + 2,
                model_hash=H,
                layout_hash=H,
                assigned_node=f"n{i}",
            )
            for i in range(2)
        ]
        self.s.bind_session(Session(**args), stages, event_id="plan" + sid)

    def run_session(self):
        self.plan()
        for stage in range(2):
            self.s.stage_transition("s", 1, stage, "ready", event_id=f"ready{stage}")
        self.s.transition("s", 1, State.RUNNING, event_id="run")

    def tile(self, stage=0, tid=0, start=0, epoch=1, sid="s"):
        return Tile(
            model_hash=H,
            layout_hash=H,
            session_id=sid,
            epoch=epoch,
            stage_id=stage,
            tile_id=tid,
            token_start=start,
            token_count=2,
            shape=(1, 2, 4),
            dtype="float16",
        )

    def commit(self, tile, *, event_id, store=None):
        store = store or self.s
        for state in ("received", "computed", "verified"):
            store.tile_transition(tile, state, event_id=event_id + "-" + state)
        store.commit_tile(tile, event_id=event_id)

    def test_replay_ownership_and_transitions(self):
        self.run_session()
        self.s.transition("s", 1, State.RUNNING, event_id="run")
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.FAILED, event_id="run")
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.PREPARING, event_id="back")
        with self.assertRaises(PermissionError):
            self.s.get_task("t", owner_id="other")
        self.assertEqual(self.s.get_task("t", owner_id="u").task_id, "t")

    def test_plan_binding_and_fingerprints(self):
        with self.assertRaises(Conflict):
            self.plan(model_hash="b" * 64)
        with self.assertRaises(Conflict):
            self.plan(owner_id="other")
        self.plan()
        with self.assertRaises(Conflict):
            self.plan("other")

    def test_contiguous_stage_coverage(self):
        session = Session(
            session_id="s",
            task_id="t",
            owner_id="u",
            epoch=1,
            model_hash=H,
            layout_hash=H,
            selected_nodes=("n",),
            expected_tokens=2,
        )
        for start, end in [(1, 4), (0, 3)]:
            stage = Stage(
                session_id="s",
                stage_id=0,
                layer_start=start,
                layer_end=end,
                model_hash=H,
                layout_hash=H,
                assigned_node="n",
            )
            with self.assertRaises(Conflict):
                self.s.bind_session(session, [stage], event_id="bad")

    def test_tile_order_dependency_and_prefix(self):
        self.run_session()
        for t in [self.tile(1), self.tile(tid=1, start=2), self.tile(sid="other")]:
            with self.assertRaises(Conflict):
                self.commit(t, event_id="bad")
        self.commit(self.tile(), event_id="a")
        self.commit(self.tile(), event_id="a")
        with self.assertRaises(Conflict):
            self.commit(self.tile(), event_id="duplicate")
        self.commit(self.tile(1), event_id="b")
        self.assertEqual(self.s.committed_prefix("s", 1), 2)

    def test_terminal_states_fail_closed(self):
        self.run_session()
        self.s.transition("s", 1, State.CANCELLED, event_id="cancel")
        with self.assertRaises(Conflict):
            self.commit(self.tile(), event_id="late")
        with self.assertRaises(Conflict):
            self.s.advance_epoch("s", 1, event_id="revive")
        self.assertEqual(self.s.get_task("t", owner_id="u").state, State.CANCELLED)

    def test_restart_and_epoch_fencing(self):
        self.run_session()
        self.commit(self.tile(), event_id="old")
        self.s = P1Store(self.path)
        self.assertEqual(self.s.identify_interrupted(event_id="restart"), ["s"])
        self.assertEqual(self.s.get_session("s").state, State.INTERRUPTED)
        self.assertEqual(self.s.advance_epoch("s", 1, event_id="epoch"), 2)
        for stage in range(2):
            self.s.stage_transition("s", 2, stage, "ready", event_id=f"newready{stage}")
        self.s.transition("s", 2, State.RUNNING, event_id="resume")
        with self.assertRaises(Conflict):
            self.commit(self.tile(), event_id="stale")
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.FAILED, event_id="stale-state")
        self.commit(self.tile(epoch=2), event_id="new")
        self.assertEqual(self.s.committed_prefix("s", 2), 0)

    def test_concurrent_writers_and_old_tables(self):
        from cluster_v03.store import ClusterStore

        old = ClusterStore(self.path)
        job = old.submit("sum", {"numbers": [1]})
        self.run_session()

        def commit(i):
            try:
                self.commit(
                    self.tile(), event_id=f"writer{i}", store=P1Store(self.path)
                )
                return True
            except Conflict:
                return False

        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(commit, range(4))), 1)
        self.assertEqual(old.task_detail(job["id"])["status"], "queued")

    def test_tile_lifecycle_and_session_completion_gate(self):
        self.run_session()
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.COMPLETED, event_id="premature")
        t = self.tile()
        with self.assertRaises(Conflict):
            self.s.tile_transition(t, "verified", event_id="skip")
        self.s.tile_transition(t, "received", event_id="receive")
        self.s.tile_transition(t, "computed", event_id="compute")
        with self.assertRaises(Conflict):
            self.s.tile_transition(t, "committed", event_id="unverified")
        self.s.tile_transition(t, "verified", event_id="verify")
        self.s.commit_tile(t, event_id="commit")
        self.assertEqual(self.s.stage_state("s", 1, 0), "running")
        for state in ["received", "computed", "verified", "committed"]:
            self.s.tile_transition(self.tile(1), state, event_id="down" + state)
        self.s.transition("s", 1, State.VERIFYING, event_id="verify-session")
        self.s.transition("s", 1, State.COMPLETED, event_id="complete-session")
        self.assertEqual(self.s.stage_state("s", 1, 0), "completed")

    def test_tile_fingerprint_mismatch_and_partial_completion(self):
        self.run_session()
        bad = self.tile().model_copy(update={"model_hash": "b" * 64})
        with self.assertRaises(Conflict):
            self.commit(bad, event_id="badmodel")
        self.commit(self.tile(), event_id="one-stage")
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.VERIFYING, event_id="partial")
        self.s.transition("s", 1, State.FAILED, event_id="failure")
        with self.assertRaises(Conflict):
            self.commit(self.tile(1), event_id="after-failure")

    def test_stage_readiness_is_individual_and_epoch_fenced(self):
        self.plan()
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.RUNNING, event_id="early-run")
        self.s.stage_transition("s", 1, 0, "ready", event_id="stage0")
        self.assertEqual(self.s.stage_state("s", 1, 0), "ready")
        self.assertEqual(self.s.stage_state("s", 1, 1), "preparing")
        with self.assertRaises(Conflict):
            self.s.transition("s", 1, State.RUNNING, event_id="one-ready")
        self.s.identify_interrupted(event_id="restart-stage")
        self.s.advance_epoch("s", 1, event_id="new-stage-epoch")
        with self.assertRaises(Conflict):
            self.s.stage_transition("s", 1, 1, "ready", event_id="old-ready")
        self.assertEqual(self.s.stage_state("s", 2, 0), "preparing")

    def test_queued_task_cancellation_is_owned_and_terminal(self):
        with self.assertRaises(PermissionError):
            self.s.cancel_task("t", owner_id="wrong", event_id="cancel")
        self.s.cancel_task("t", owner_id="u", event_id="cancel")
        self.s.cancel_task("t", owner_id="u", event_id="cancel")
        with self.assertRaises(Conflict):
            self.plan()
        self.assertEqual(self.s.get_task("t", owner_id="u").state, State.CANCELLED)
