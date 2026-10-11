"""Lamport's distributed mutual exclusion: per-process state and message rules.

Each Process only knows its own state. It never reads another process's state;
all coordination happens through Message objects (REQUEST, REPLY, RELEASE).
"""
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

REQUEST = "REQUEST"
REPLY = "REPLY"
RELEASE = "RELEASE"

IDLE = "Idle"
REQUESTING = "Requesting"  # request broadcast, no REPLY received yet
WAITING = "Waiting"        # some REPLYs received, entry condition not met yet
IN_CS = "In Critical Section"

# (kind, receiver_pid, timestamp, request_timestamp)
Outgoing = Tuple[str, int, int, int]


@dataclass(frozen=True)
class Message:
    msg_id: int
    kind: str
    sender: int
    receiver: int
    timestamp: int      # Lamport clock value carried by the message
    request_ts: int     # timestamp of the request this message refers to


class Process:
    def __init__(self, pid: int, all_pids: List[int]):
        self.pid = pid
        self.peers = [p for p in all_pids if p != pid]
        self.reset()

    def reset(self) -> None:
        self.clock = 0
        self.request_ts: Optional[int] = None
        self.status = IDLE
        self.replies: Set[int] = set()
        self.queue: List[Tuple[int, int]] = []   # sorted (timestamp, pid)
        self.history: List[str] = []

    # ---- logical clock -------------------------------------------------
    def tick(self) -> int:
        """Local or send event: increment the clock."""
        self.clock += 1
        return self.clock

    def update_clock(self, received_ts: int) -> int:
        """Receive event: local_clock = max(local_clock, received_ts) + 1."""
        self.clock = max(self.clock, received_ts) + 1
        return self.clock

    # ---- request queue -------------------------------------------------
    def queue_insert(self, entry: Tuple[int, int]) -> bool:
        if entry in self.queue:
            return False
        self.queue.append(entry)
        self.queue.sort()  # order by (timestamp, process id)
        return True

    def queue_remove(self, pid: int, request_ts: int) -> bool:
        entry = (request_ts, pid)
        if entry in self.queue:
            self.queue.remove(entry)
            return True
        return False

    # ---- algorithm steps -----------------------------------------------
    def make_request(self) -> List[Outgoing]:
        if self.status != IDLE:
            raise ValueError("P%d already has an outstanding request or is in the critical section." % self.pid)
        ts = self.tick()
        self.request_ts = ts
        self.status = REQUESTING
        self.replies = set()
        self.queue_insert((ts, self.pid))
        self.history.append("requested at t=%d" % ts)
        return [(REQUEST, peer, ts, ts) for peer in self.peers]

    def receive(self, msg: Message) -> Tuple[List[Outgoing], str]:
        """Handle a delivered message. Returns (outgoing messages, queue note)."""
        self.update_clock(msg.timestamp)
        out: List[Outgoing] = []
        if msg.kind == REQUEST:
            self.queue_insert((msg.request_ts, msg.sender))
            reply_ts = self.tick()
            out.append((REPLY, msg.sender, reply_ts, msg.request_ts))
            note = "inserted (%d,P%d) into P%d queue" % (msg.request_ts, msg.sender, self.pid)
        elif msg.kind == REPLY:
            if self.status in (REQUESTING, WAITING) and msg.request_ts == self.request_ts:
                self.replies.add(msg.sender)
                self.status = WAITING
                note = "recorded REPLY from P%d (%d/%d)" % (msg.sender, len(self.replies), len(self.peers))
            else:
                note = "ignored stale REPLY from P%d" % msg.sender
        elif msg.kind == RELEASE:
            removed = self.queue_remove(msg.sender, msg.request_ts)
            note = ("removed (%d,P%d) from P%d queue" % (msg.request_ts, msg.sender, self.pid)
                    if removed else "RELEASE matched no queue entry")
        else:
            raise ValueError("Unknown message kind: %s" % msg.kind)
        self.history.append("t=%d %s from P%d" % (self.clock, msg.kind, msg.sender))
        return out, note

    def can_enter(self) -> bool:
        """Entry condition: own request first in the ordered queue AND a REPLY from every other process."""
        return (self.status in (REQUESTING, WAITING)
                and self.request_ts is not None
                and len(self.replies) == len(self.peers)
                and bool(self.queue)
                and self.queue[0] == (self.request_ts, self.pid))

    def enter(self) -> None:
        if not self.can_enter():
            raise ValueError("P%d does not satisfy the entry condition." % self.pid)
        self.status = IN_CS
        self.history.append("entered CS")

    def release(self) -> Tuple[List[Outgoing], int]:
        if self.status != IN_CS:
            raise ValueError("P%d is not in the critical section." % self.pid)
        old_ts = self.request_ts
        ts = self.tick()
        self.queue_remove(self.pid, old_ts)
        self.status = IDLE
        self.request_ts = None
        self.replies = set()
        self.history.append("released at t=%d" % ts)
        return [(RELEASE, peer, ts, old_ts) for peer in self.peers], old_ts
