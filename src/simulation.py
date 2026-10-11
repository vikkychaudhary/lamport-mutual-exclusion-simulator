"""Deterministic, event-driven simulation of Lamport mutual exclusion.

Messages travel through a single FIFO list (so every channel is FIFO, which the
algorithm requires). Nothing is delivered until step() processes it.
"""
from typing import Dict, List, Optional, Tuple

from .lamport import (IDLE, IN_CS, RELEASE, REPLY, REQUEST, Message, Outgoing, Process)

MIN_PROCESSES = 2
MAX_PROCESSES = 10


def label(pid: int) -> str:
    return "P%d" % pid


class Simulation:
    def __init__(self, n: int = 4):
        self.reset(n)

    # ---- setup ---------------------------------------------------------
    def reset(self, n: Optional[int] = None) -> None:
        if n is None:
            n = getattr(self, "n", None)   # keep the current size on a plain reset
        if isinstance(n, bool) or not isinstance(n, int) or not (MIN_PROCESSES <= n <= MAX_PROCESSES):
            raise ValueError("Number of processes must be an integer from %d to %d." % (MIN_PROCESSES, MAX_PROCESSES))
        self.n = n
        self.processes = [Process(i, list(range(self.n))) for i in range(self.n)]
        self.pending: List[Message] = []     # messages in transit (FIFO)
        self.sent: List[Message] = []        # every message ever sent
        self.history: List[Dict] = []        # every event ever logged
        self.history_start = 0               # events before this index are hidden by "clear history"
        self.violations: List[str] = []
        self.last_message: Optional[Message] = None
        self.active_pid: Optional[int] = None
        self._next_msg_id = 1
        self._seq = 0

    def _check_pid(self, pid) -> int:
        if isinstance(pid, bool) or not isinstance(pid, int) or not (0 <= pid < self.n):
            raise ValueError("Invalid process ID %r: choose an integer from 0 to %d." % (pid, self.n - 1))
        return pid

    # ---- internal helpers ----------------------------------------------
    def _send(self, sender: int, outs: List[Outgoing]) -> None:
        for kind, receiver, ts, req_ts in outs:
            msg = Message(self._next_msg_id, kind, sender, receiver, ts, req_ts)
            self._next_msg_id += 1
            self.pending.append(msg)
            self.sent.append(msg)

    def _log(self, etype: str, pid: int, sender: str, receiver: str, msg_ts, queue_change: str, text: str) -> Dict:
        self._seq += 1
        event = {
            "seq": self._seq, "type": etype, "process": pid, "sender": sender, "receiver": receiver,
            "msg_ts": msg_ts,
            "clocks": {label(p.pid): p.clock for p in self.processes},
            "queues": {label(p.pid): list(p.queue) for p in self.processes},
            "queue_change": queue_change, "explanation": text,
        }
        self.history.append(event)
        self.active_pid = pid
        return event

    def _check_safety(self, p: Process) -> None:
        holders = [q.pid for q in self.processes if q.status == IN_CS]
        problems = []
        if len(holders) > 1:
            problems.append("SAFETY VIOLATION: processes %s are in the critical section together." % holders)
        if len(p.replies) != len(p.peers):
            problems.append("SAFETY VIOLATION: P%d entered without all REPLY messages." % p.pid)
        for text in problems:
            self.violations.append(text)
            self._log("SAFETY_VIOLATION", p.pid, label(p.pid), "-", None, "", text)

    # ---- user / algorithm actions --------------------------------------
    def request_cs(self, pid: int) -> Dict:
        self._check_pid(pid)
        p = self.processes[pid]
        outs = p.make_request()
        self._send(pid, outs)
        return self._log("REQUEST", pid, label(pid), "ALL", p.request_ts,
                         "added (%d,P%d) to own queue" % (p.request_ts, pid),
                         "P%d ticks its clock to %d and broadcasts REQUEST(t=%d) to %d other process(es)."
                         % (pid, p.clock, p.request_ts, len(outs)))

    def release_cs(self, pid: int) -> Dict:
        self._check_pid(pid)
        p = self.processes[pid]
        outs, old_ts = p.release()
        self._send(pid, outs)
        return self._log("RELEASE", pid, label(pid), "ALL", p.clock,
                         "removed (%d,P%d) from own queue" % (old_ts, pid),
                         "P%d leaves the critical section and broadcasts RELEASE(t=%d)." % (pid, p.clock))

    def generate_concurrent(self, count: Optional[int] = None) -> List[Dict]:
        """Issue requests from idle processes (lowest IDs first) before any message is delivered."""
        idle = [p.pid for p in self.processes if p.status == IDLE]
        if not idle:
            raise ValueError("No idle process is available to make a request.")
        if count is None:
            count = len(idle)
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError("Request count must be a positive integer.")
        return [self.request_cs(pid) for pid in idle[:count]]

    def cs_holder(self) -> Optional[int]:
        for p in self.processes:
            if p.status == IN_CS:
                return p.pid
        return None

    # ---- the event loop --------------------------------------------------
    def step(self, auto_release: bool = True) -> Optional[Dict]:
        """Process exactly one event. Priority: enter CS, deliver message, auto-release."""
        for p in self.processes:
            if p.can_enter():
                p.enter()
                event = self._log("ENTER_CS", p.pid, label(p.pid), "-", p.request_ts, "",
                                  "P%d enters the critical section: its request (%d,P%d) is first in its queue and it holds REPLYs from all %d other process(es)."
                                  % (p.pid, p.request_ts, p.pid, len(p.peers)))
                self._check_safety(p)
                return event
        if self.pending:
            return self._deliver(self.pending.pop(0))
        if auto_release:
            holder = self.cs_holder()
            if holder is not None:
                return self.release_cs(holder)
        return None

    def _deliver(self, msg: Message) -> Dict:
        receiver = self.processes[msg.receiver]
        outs, note = receiver.receive(msg)
        self._send(receiver.pid, outs)
        self.last_message = msg
        extra = " and sends REPLY back to P%d" % msg.sender if outs else ""
        text = "P%d receives %s(t=%d) from P%d, sets its clock to %d, %s%s." % (
            receiver.pid, msg.kind, msg.timestamp, msg.sender, receiver.clock, note, extra)
        return self._log("RECV_" + msg.kind, receiver.pid, label(msg.sender), label(receiver.pid),
                         msg.timestamp, note, text)

    def is_stable(self) -> bool:
        return (not self.pending and self.cs_holder() is None
                and not any(p.can_enter() for p in self.processes))

    def run_until_completion(self, max_events: int = 1000) -> Tuple[int, bool]:
        """Run until stable or max_events reached. Returns (events processed, reached_stable)."""
        if isinstance(max_events, bool) or not isinstance(max_events, int) or max_events < 1:
            raise ValueError("Safety limit must be a positive integer.")
        count = 0
        while count < max_events:
            if self.step(auto_release=True) is None:
                return count, True
            count += 1
        return count, self.is_stable()

    # ---- history views ---------------------------------------------------
    def clear_history(self) -> None:
        """Hide existing events from the log view (metrics still use the full history)."""
        self.history_start = len(self.history)

    def visible_history(self) -> List[Dict]:
        return self.history[self.history_start:]

    # ---- metrics ---------------------------------------------------------
    def metrics(self) -> Dict:
        counts = {REQUEST: 0, REPLY: 0, RELEASE: 0}
        for m in self.sent:
            counts[m.kind] += 1
        entries = sum(1 for e in self.history if e["type"] == "ENTER_CS")
        releases = sum(1 for e in self.history if e["type"] == "RELEASE")
        total = len(self.sent)
        max_clock = max([p.clock for p in self.processes] + [c for e in self.history for c in e["clocks"].values()])
        holder = self.cs_holder()
        return {
            "total_request": counts[REQUEST], "total_reply": counts[REPLY], "total_release": counts[RELEASE],
            "total_messages": total, "cs_entries": entries, "releases": releases,
            "max_clock": max_clock,
            "avg_messages_per_entry": (total / entries) if entries else None,
            "queue_length": max(len(p.queue) for p in self.processes),
            "cs_owner": label(holder) if holder is not None else None,
            "pending_messages": len(self.pending),
            "safety_ok": not self.violations,
            "violations": list(self.violations),
        }


def worked_example() -> Simulation:
    """Three processes request at the same logical time (clock 1); P0 wins the tie, then P1, then P2."""
    sim = Simulation(3)
    for pid in (0, 1, 2):
        sim.request_cs(pid)
    sim.run_until_completion()
    return sim
