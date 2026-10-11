import unittest

from src.lamport import (IDLE, IN_CS, RELEASE, REPLY, REQUEST, REQUESTING, WAITING, Message, Process)


def make(n=3):
    return [Process(i, list(range(n))) for i in range(n)]


class TestProcess(unittest.TestCase):
    def test_clock_update_rule(self):
        p = make()[0]
        p.clock = 5
        p.update_clock(2)
        self.assertEqual(p.clock, 6)       # max(5, 2) + 1
        p.update_clock(10)
        self.assertEqual(p.clock, 11)      # max(6, 10) + 1

    def test_request_broadcast_and_state(self):
        p = make()[1]
        outs = p.make_request()
        self.assertEqual(p.clock, 1)
        self.assertEqual(p.status, REQUESTING)
        self.assertEqual(sorted(o[1] for o in outs), [0, 2])
        self.assertTrue(all(o[0] == REQUEST and o[2] == 1 for o in outs))
        self.assertEqual(p.queue, [(1, 1)])

    def test_duplicate_request_rejected(self):
        p = make()[0]
        p.make_request()
        with self.assertRaises(ValueError):
            p.make_request()

    def test_queue_order_and_tie_break(self):
        p = make()[0]
        p.queue_insert((3, 2))
        p.queue_insert((1, 2))
        p.queue_insert((1, 1))
        self.assertEqual(p.queue, [(1, 1), (1, 2), (3, 2)])  # tie on ts=1 broken by pid
        self.assertFalse(p.queue_insert((1, 1)))              # no duplicates

    def test_receive_request_sends_reply(self):
        a, b, _ = make()
        outs = a.make_request()
        msg = Message(1, REQUEST, 0, 1, outs[0][2], outs[0][3])
        reply, _ = b.receive(msg)
        self.assertEqual(b.clock, 3)  # max(0,1)+1 = 2, then +1 for the send event
        self.assertEqual(b.queue, [(1, 0)])
        self.assertEqual(reply[0][0], REPLY)
        self.assertEqual(reply[0][1], 0)

    def test_reply_tracking_and_entry_condition(self):
        a = make()[0]
        a.make_request()
        self.assertFalse(a.can_enter())
        a.receive(Message(1, REPLY, 1, 0, 3, a.request_ts))
        self.assertEqual(a.status, WAITING)
        self.assertFalse(a.can_enter())          # still missing P2's reply
        a.receive(Message(2, REPLY, 2, 0, 3, a.request_ts))
        self.assertTrue(a.can_enter())
        a.enter()
        self.assertEqual(a.status, IN_CS)

    def test_cannot_enter_if_not_head_of_queue(self):
        a = make()[1]
        a.make_request()                          # (1, 1)
        a.receive(Message(1, REQUEST, 0, 1, 1, 1))  # (1, 0) is ahead
        a.receive(Message(2, REPLY, 0, 1, 3, 1))
        a.receive(Message(3, REPLY, 2, 1, 3, 1))
        self.assertEqual(len(a.replies), 2)
        self.assertFalse(a.can_enter())
        with self.assertRaises(ValueError):
            a.enter()

    def test_stale_reply_ignored(self):
        a = make()[0]
        a.receive(Message(1, REPLY, 1, 0, 2, 1))
        self.assertEqual(a.status, IDLE)
        self.assertEqual(a.replies, set())

    def test_release_handling(self):
        a, b, c = make()
        a.make_request()
        b.receive(Message(1, REQUEST, 0, 1, 1, 1))
        for pid in (1, 2):
            a.receive(Message(pid, REPLY, pid, 0, 2, 1))
        a.enter()
        outs, old = a.release()
        self.assertEqual(old, 1)
        self.assertEqual(a.status, IDLE)
        self.assertEqual(a.queue, [])
        self.assertTrue(all(o[0] == RELEASE for o in outs))
        b.receive(Message(2, RELEASE, 0, 1, outs[0][2], 1))
        self.assertEqual(b.queue, [])

    def test_release_without_cs_rejected(self):
        with self.assertRaises(ValueError):
            make()[0].release()

    def test_reset(self):
        p = make()[0]
        p.make_request()
        p.reset()
        self.assertEqual((p.clock, p.status, p.queue, p.replies), (0, IDLE, [], set()))


if __name__ == "__main__":
    unittest.main()
