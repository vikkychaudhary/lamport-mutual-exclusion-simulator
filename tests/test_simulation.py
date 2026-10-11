import csv
import io
import json
import random
import unittest

from src.exporters import events_to_csv, report_json, report_txt
from src.lamport import IDLE, IN_CS
from src.simulation import Simulation, worked_example


def in_cs(sim):
    return [p.pid for p in sim.processes if p.status == IN_CS]


class TestSimulation(unittest.TestCase):
    def test_empty_state(self):
        sim = Simulation(3)
        self.assertEqual(sim.history, [])
        self.assertIsNone(sim.step())
        self.assertEqual(sim.run_until_completion(), (0, True))
        m = sim.metrics()
        self.assertEqual(m["total_messages"], 0)
        self.assertIsNone(m["avg_messages_per_entry"])
        self.assertTrue(m["safety_ok"])

    def test_two_process_cycle(self):
        sim = Simulation(2)
        sim.request_cs(0)
        sim.run_until_completion()
        m = sim.metrics()
        self.assertEqual((m["total_request"], m["total_reply"], m["total_release"]), (1, 1, 1))
        self.assertEqual(m["cs_entries"], 1)
        self.assertEqual(m["releases"], 1)

    def test_messages_not_delivered_silently(self):
        sim = Simulation(3)
        sim.request_cs(0)
        self.assertEqual(len(sim.pending), 2)
        self.assertEqual(sim.processes[1].queue, [])   # not delivered yet
        sim.step()
        self.assertEqual(sim.processes[1].queue, [(1, 0)])
        self.assertEqual(len(sim.processes[2].queue), 0)

    def test_no_entry_before_all_replies(self):
        sim = Simulation(4)
        sim.request_cs(0)
        while sim.step(auto_release=False):
            if in_cs(sim):
                self.assertEqual(len(sim.processes[0].replies), 3)
        self.assertEqual(in_cs(sim), [0])

    def test_equal_timestamps_ordered_by_pid(self):
        sim = Simulation(3)
        sim.generate_concurrent()
        self.assertEqual([p.request_ts for p in sim.processes], [1, 1, 1])
        sim.run_until_completion()
        order = [e["process"] for e in sim.history if e["type"] == "ENTER_CS"]
        self.assertEqual(order, [0, 1, 2])

    def test_worked_example(self):
        sim = worked_example()
        self.assertEqual(sim.metrics()["cs_entries"], 3)
        self.assertTrue(sim.is_stable())

    def test_request_while_cs_occupied(self):
        sim = Simulation(3)
        sim.request_cs(0)
        while not in_cs(sim):
            sim.step(auto_release=False)
        sim.request_cs(1)
        for _ in range(20):
            sim.step(auto_release=False)
            self.assertEqual(in_cs(sim), [0])        # P1 must wait
        self.assertEqual(sim.processes[1].status, "Waiting")
        sim.release_cs(0)
        sim.run_until_completion()
        self.assertEqual(sim.metrics()["cs_entries"], 2)

    def test_repeated_requests_and_cycles(self):
        sim = Simulation(3)
        sim.request_cs(1)
        with self.assertRaises(ValueError):
            sim.request_cs(1)                          # duplicate outstanding request
        for _ in range(4):
            sim.run_until_completion()
            sim.request_cs(1)
        sim.run_until_completion()
        self.assertEqual(sim.metrics()["cs_entries"], 5)

    def test_invalid_operations(self):
        sim = Simulation(3)
        for bad in (-1, 3, "a", None, 1.5, True):
            with self.assertRaises(ValueError):
                sim.request_cs(bad)
        with self.assertRaises(ValueError):
            sim.release_cs(0)
        for bad in (1, 11, "4", None):
            with self.assertRaises(ValueError):
                Simulation(bad)
        with self.assertRaises(ValueError):
            sim.run_until_completion(0)

    def test_reset_during_pending_requests(self):
        sim = Simulation(4)
        sim.generate_concurrent()
        sim.step()
        sim.reset()
        self.assertEqual((sim.pending, sim.history, sim.sent), ([], [], []))
        self.assertTrue(all(p.clock == 0 and p.status == IDLE and not p.queue for p in sim.processes))
        sim.reset(6)
        self.assertEqual(len(sim.processes), 6)

    def test_safety_limit(self):
        sim = Simulation(5)
        sim.generate_concurrent()
        count, stable = sim.run_until_completion(max_events=3)
        self.assertEqual(count, 3)
        self.assertFalse(stable)
        count, stable = sim.run_until_completion()
        self.assertTrue(stable)

    def test_clear_history_keeps_metrics(self):
        sim = Simulation(3)
        sim.request_cs(0)
        sim.run_until_completion()
        sim.clear_history()
        self.assertEqual(sim.visible_history(), [])
        self.assertEqual(sim.metrics()["cs_entries"], 1)

    def test_ten_process_progress(self):
        sim = Simulation(10)
        sim.generate_concurrent()
        count, stable = sim.run_until_completion(max_events=5000)
        self.assertTrue(stable)
        m = sim.metrics()
        self.assertEqual(m["cs_entries"], 10)
        self.assertEqual(m["total_messages"], 10 * 27)   # 3(N-1) per entry
        self.assertTrue(m["safety_ok"])

    def test_stress_random_requests_mutual_exclusion(self):
        for seed in range(30):
            rng = random.Random(seed)
            sim = Simulation(rng.randint(2, 10))
            requested = 0
            for _ in range(300):
                action = rng.random()
                idle = [p.pid for p in sim.processes if p.status == IDLE]
                if action < 0.3 and idle:
                    sim.request_cs(rng.choice(idle))
                    requested += 1
                elif action < 0.4 and sim.cs_holder() is not None:
                    sim.release_cs(sim.cs_holder())
                else:
                    sim.step(auto_release=rng.random() < 0.3)
                self.assertLessEqual(len(in_cs(sim)), 1)
            sim.run_until_completion(max_events=20000)
            self.assertTrue(sim.is_stable())
            self.assertEqual(sim.metrics()["cs_entries"], requested)   # progress: every request served
            self.assertEqual(sim.violations, [])
            self.assertTrue(all(not p.queue for p in sim.processes))

    def test_exports(self):
        sim = worked_example()
        rows = list(csv.DictReader(io.StringIO(events_to_csv(sim.visible_history()))))
        self.assertEqual(len(rows), len(sim.history))
        data = json.loads(report_json(sim))
        self.assertEqual(data["mutual_exclusion"], "OK")
        self.assertEqual(len(data["events"]), len(sim.history))
        self.assertIn("Mutual exclusion: OK", report_txt(sim))
        empty = Simulation(2)
        self.assertEqual(events_to_csv(empty.visible_history()).strip().split(",")[0], "seq")
        self.assertIn("EVENTS (0 shown", report_txt(empty))


if __name__ == "__main__":
    unittest.main()
