"""Export helpers: CSV event history, JSON report, TXT report (standard library only)."""
import csv
import io
import json
from typing import Dict, List

FIELDS = ["seq", "type", "process", "sender", "receiver", "msg_ts", "clocks", "queue_change", "explanation"]


def _flat(event: Dict) -> Dict:
    row = {k: event.get(k, "") for k in FIELDS}
    row["process"] = "P%d" % event["process"]
    row["msg_ts"] = "" if event["msg_ts"] is None else event["msg_ts"]
    row["clocks"] = ";".join("%s=%s" % kv for kv in event["clocks"].items())
    return row


def events_to_csv(events: List[Dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS)
    writer.writeheader()
    for e in events:
        writer.writerow(_flat(e))
    return buf.getvalue()


def report_dict(sim) -> Dict:
    events = sim.visible_history()
    return {
        "processes": sim.n,
        "metrics": sim.metrics(),
        "mutual_exclusion": "OK" if not sim.violations else "VIOLATED",
        "final_state": [{"process": "P%d" % p.pid, "clock": p.clock, "status": p.status,
                         "queue": [list(q) for q in p.queue]} for p in sim.processes],
        "events_hidden_by_clear": sim.history_start,
        "events": [_flat(e) for e in events],
    }


def report_json(sim) -> str:
    return json.dumps(report_dict(sim), indent=2)


def report_txt(sim) -> str:
    r = report_dict(sim)
    m = r["metrics"]
    avg = "n/a" if m["avg_messages_per_entry"] is None else "%.2f" % m["avg_messages_per_entry"]
    lines = [
        "LAMPORT MUTUAL EXCLUSION SIMULATION REPORT",
        "=" * 44,
        "Processes: %d" % r["processes"],
        "Mutual exclusion: %s" % r["mutual_exclusion"],
        "REQUEST / REPLY / RELEASE: %d / %d / %d (total %d)" % (
            m["total_request"], m["total_reply"], m["total_release"], m["total_messages"]),
        "Completed CS entries: %d, releases: %d" % (m["cs_entries"], m["releases"]),
        "Max logical clock: %d" % m["max_clock"],
        "Average messages per CS entry: %s" % avg,
        "", "FINAL STATE"]
    for p in r["final_state"]:
        lines.append("  %s clock=%d status=%s queue=%s" % (p["process"], p["clock"], p["status"], p["queue"]))
    lines += ["", "EVENTS (%d shown, %d hidden by 'clear history')" % (len(r["events"]), r["events_hidden_by_clear"])]
    for e in r["events"]:
        lines.append("#%s %-12s %s -> %s ts=%s | %s" % (e["seq"], e["type"], e["sender"], e["receiver"],
                                                      e["msg_ts"], e["explanation"]))
    for v in m["violations"]:
        lines.append("ERROR: " + v)
    return "\n".join(lines) + "\n"
