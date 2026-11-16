"""Contract and concurrency tests for the v0.4.9 fair request queue."""
import asyncio
import unittest

from compute.request_queue_v049 import (
    DuplicateRequestError, FairRequestQueue, QueueFullError,
)


class FairRequestQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_robin_between_clients_and_fifo_within_client(self):
        queue = FairRequestQueue(max_pending=8, max_pending_per_client=4)
        a1 = await queue.put("a1", "client-a", "A1", timeout=5)
        a2 = await queue.put("a2", "client-a", "A2", timeout=5)
        b1 = await queue.put("b1", "client-b", "B1", timeout=5)

        first = await queue.get()
        second = await queue.get()
        third = await queue.get()
        self.assertEqual(
            [(first.client_id, first.payload),
             (second.client_id, second.payload),
             (third.client_id, third.payload)],
            [("client-a", "A1"), ("client-b", "B1"), ("client-a", "A2")])
        await queue.complete(first, "done-a1")
        await queue.complete(second, "done-b1")
        await queue.complete(third, "done-a2")
        self.assertEqual(await a1.result(), "done-a1")
        self.assertEqual(await a2.result(), "done-a2")
        self.assertEqual(await b1.result(), "done-b1")

    async def test_global_and_per_client_admission_limits(self):
        queue = FairRequestQueue(max_pending=2, max_pending_per_client=1)
        await queue.put("one", "A", 1, timeout=5)
        with self.assertRaises(QueueFullError):
            await queue.put("two", "A", 2, timeout=5)
        await queue.put("three", "B", 3, timeout=5)
        with self.assertRaises(QueueFullError):
            await queue.put("four", "C", 4, timeout=5)
        self.assertEqual(queue.pending_count, 2)

    async def test_duplicate_id_rejected_until_request_settled(self):
        queue = FairRequestQueue()
        await queue.put("same", "A", 1, timeout=5)
        with self.assertRaises(DuplicateRequestError):
            await queue.put("same", "B", 2, timeout=5)
        request = await queue.get()
        with self.assertRaises(DuplicateRequestError):
            await queue.put("same", "B", 2, timeout=5)
        await queue.complete(request, "ok")
        await queue.put("same", "B", 2, timeout=5)

    async def test_expired_request_is_not_dispatched(self):
        queue = FairRequestQueue()
        ticket = await queue.put("expired", "A", 1, timeout=0.02)
        await asyncio.sleep(0.04)
        with self.assertRaisesRegex(TimeoutError, "expired in queue"):
            await ticket.result()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(queue.get(), timeout=0.02)
        # Retrieve the queue's terminal Future exception as well; the first
        # waiter observed its deadline directly before the consumer pruned it.
        with self.assertRaisesRegex(TimeoutError, "expired in queue"):
            await ticket.result()
        self.assertEqual(queue.pending_count, 0)

    async def test_result_deadline_does_not_abort_dispatched_work(self):
        queue = FairRequestQueue()
        ticket = await queue.put("inflight", "A", "payload", timeout=0.02)
        request = await queue.get()
        await asyncio.sleep(0.04)
        await queue.complete(request, "finished")
        self.assertEqual(await ticket.result(), "finished")

    async def test_expired_client_does_not_block_other_clients(self):
        queue = FairRequestQueue()
        expired = await queue.put("old", "A", "old", timeout=0.02)
        live = await queue.put("live", "B", "live", timeout=2)
        await asyncio.sleep(0.04)
        request = await queue.get()
        self.assertEqual(request.payload, "live")
        with self.assertRaises(TimeoutError):
            await expired.result()
        await queue.complete(request, "ok")
        self.assertEqual(await live.result(), "ok")

    async def test_failure_is_delivered_to_waiter(self):
        queue = FairRequestQueue()
        ticket = await queue.put("failure", "A", None, timeout=5)
        request = await queue.get()
        await queue.fail(request, RuntimeError("worker failed"))
        with self.assertRaisesRegex(RuntimeError, "worker failed"):
            await ticket.result()

    async def test_close_fails_queued_and_rejects_new_work(self):
        queue = FairRequestQueue()
        ticket = await queue.put("queued", "A", None, timeout=5)
        await queue.close()
        with self.assertRaisesRegex(RuntimeError, "closed before dispatch"):
            await ticket.result()
        with self.assertRaisesRegex(RuntimeError, "queue is closed"):
            await queue.put("new", "B", None, timeout=5)
        with self.assertRaisesRegex(RuntimeError, "queue is closed"):
            await queue.get()
        self.assertEqual(queue.pending_count, 0)

    async def test_invalid_configuration_and_inputs(self):
        with self.assertRaises(ValueError):
            FairRequestQueue(max_pending=0)
        with self.assertRaises(ValueError):
            FairRequestQueue(max_pending=2, max_pending_per_client=3)
        queue = FairRequestQueue()
        for request_id, client_id, timeout in [
            ("", "A", 1), ("x", "", 1), ("x", "A", 0), ("x", "A", True)
        ]:
            with self.subTest(request_id=request_id, client_id=client_id):
                with self.assertRaises(ValueError):
                    await queue.put(request_id, client_id, None, timeout=timeout)


if __name__ == "__main__":
    unittest.main()
