"""Lamport Mutual Exclusion Simulator - Streamlit UI (run: python -m streamlit run app.py)."""
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from src.exporters import events_to_csv, report_json, report_txt
from src.simulation import MAX_PROCESSES, MIN_PROCESSES, Simulation, worked_example
from src.visualization import STATUS_COLORS, network_figure, timeline_figure

st.set_page_config(page_title="Lamport Mutual Exclusion Simulator", page_icon="\U0001F537", layout="wide")
st.markdown("<style>%s</style>" % (Path(__file__).parent / "assets" / "style.css").read_text(encoding="utf-8"),
            unsafe_allow_html=True)

S = st.session_state
if "sim" not in S:
    S.sim = Simulation(4)
    S.running = False
    S.flash = None
    S.n_proc = 4
    S.sel_pid = 0


# ---------------------------------------------------------------- callbacks
def notify(level, text):
    S.flash = (level, text)


def attempt(action, *args):
    try:
        action(*args)
        S.flash = None
    except ValueError as exc:
        notify("error", str(exc))


def cb_start():
    sim = S.sim
    if not sim.pending and sim.cs_holder() is None and not any(p.queue for p in sim.processes):
        attempt(sim.generate_concurrent, min(3, sim.n))
    S.running = True


def cb_pause():
    S.running = False


def cb_resume():
    S.running = True


def cb_step():
    S.running = False
    if S.sim.step(auto_release=S.auto_release) is None:
        notify("info", "Nothing to process. Request the critical section, or release the current holder.")
    else:
        S.flash = None


def cb_run():
    S.running = False
    count, stable = S.sim.run_until_completion(int(S.max_events))
    if stable:
        notify("success", "Reached a stable state after %d event(s)." % count)
    else:
        notify("warning", "Stopped at the safety limit (%d events) before reaching a stable state." % count)


def cb_reset():
    S.running = False
    S.sim.reset()
    S.flash = None


def cb_reload():
    S.running = False
    S.sim = Simulation(int(S.n_proc))
    clamp_conc(int(S.n_proc))
    S.sel_pid = 0
    S.flash = None


def cb_request():
    attempt(S.sim.request_cs, S.sel_pid)


def cb_release():
    attempt(S.sim.release_cs, S.sel_pid)


def cb_concurrent():
    attempt(S.sim.generate_concurrent, int(S.conc_count))


def cb_clear():
    S.sim.clear_history()


def clamp_conc(n):
    if "conc_count" in S:
        S.conc_count = max(1, min(int(S.conc_count), n))


def cb_apply_n():
    S.running = False
    clamp_conc(int(S.n_proc))
    attempt(S.sim.reset, int(S.n_proc))
    S.sel_pid = 0


# ---------------------------------------------------------------- sidebar
sim = S.sim
with st.sidebar:
    st.markdown("## \U0001F537 Lamport ME")
    page = st.radio("Navigation", ["Dashboard", "Event Log & Timeline", "Analytics", "Learn"], key="page")
    st.markdown("### Simulation configuration")
    st.slider("Number of processes", MIN_PROCESSES, MAX_PROCESSES, key="n_proc", on_change=cb_apply_n,
              help="Changing this resets the simulation.")
    st.slider("Simulation speed (events/second)", 1, 10, 3, key="speed", help="Used by Start / Resume auto-run.")
    st.number_input("Safety limit for Run Until Completion", 10, 100000, 1000, key="max_events",
                    help="Maximum events processed in one run, to prevent endless loops.")
    st.checkbox("Auto-release critical section on Step", True, key="auto_release",
                help="When on, Step Forward releases the holder once no messages are in transit.")
    with st.expander("Algorithm information"):
        st.markdown("**Entry rule:** own request first in the ordered queue **and** a REPLY from every other process.\n\n"
                    "**Clock rule:** `clock = max(clock, received) + 1`.\n\n"
                    "**Cost:** 3(N-1) messages per critical-section entry.")
    st.button("Reset simulation", on_click=cb_reset, use_container_width=True)
    with st.expander("Help"):
        st.markdown("1. Press **Start** (auto-runs) or request the critical section for a process.\n"
                    "2. **Step Forward** processes exactly one event.\n"
                    "3. **Run Until Completion** processes events until stable or the safety limit.\n"
                    "4. **Clear event history** only hides old log rows; metrics keep counting.\n"
                    "5. Runs locally; nothing is sent to any server.")

m = sim.metrics()
if S.running:
    status, status_cls = "Running", "ok"
elif not sim.history:
    status, status_cls = "Ready", ""
elif sim.is_stable():
    status, status_cls = "Stable / complete", "ok"
else:
    status, status_cls = "Paused", "warn"


def kpi(col, label, value, cls=""):
    col.markdown('<div class="glass"><div class="kpi-label">%s</div><div class="kpi-value %s">%s</div></div>'
                 % (label, cls, value), unsafe_allow_html=True)


def event_rows(events):
    return [{"Seq": e["seq"], "Type": e["type"], "Sender": e["sender"], "Receiver": e["receiver"],
             "Msg ts": e["msg_ts"], "Clocks": " ".join("%s=%d" % kv for kv in e["clocks"].items()),
             "Queue change": e["queue_change"], "Explanation": e["explanation"]} for e in events]


st.markdown('<div class="hero"><h1>Lamport Mutual Exclusion Simulator</h1>'
            '<p>Distributed processes share one critical section using only logical clocks and '
            'REQUEST / REPLY / RELEASE messages - no central lock.</p></div>', unsafe_allow_html=True)
if S.flash:
    getattr(st, S.flash[0])(S.flash[1])
for v in m["violations"]:
    st.error(v)

# ---------------------------------------------------------------- dashboard
if page == "Dashboard":
    cols = st.columns(6)
    kpi(cols[0], "Status", status, status_cls)
    kpi(cols[1], "Processes", sim.n)
    kpi(cols[2], "Max logical clock", m["max_clock"])
    kpi(cols[3], "Longest request queue", m["queue_length"])
    kpi(cols[4], "Critical section", ("held by " + m["cs_owner"]) if m["cs_owner"] else "free",
        "bad" if m["cs_owner"] else "ok")
    kpi(cols[5], "Mutual exclusion", "OK" if m["safety_ok"] else "VIOLATED", "ok" if m["safety_ok"] else "bad")

    st.markdown("#### Simulation controls")
    c = st.columns(6)
    c[0].button("\u25B6 Start", on_click=cb_start, use_container_width=True, disabled=S.running,
                help="Auto-runs events. If nothing is pending it first generates concurrent requests.")
    c[1].button("\u23F8 Pause", on_click=cb_pause, use_container_width=True, disabled=not S.running)
    c[2].button("\u23EF Resume", on_click=cb_resume, use_container_width=True, disabled=S.running)
    c[3].button("\u23ED Step Forward", on_click=cb_step, use_container_width=True, help="Process exactly one event.")
    c[4].button("\u23E9 Run Until Completion", on_click=cb_run, use_container_width=True)
    c[5].button("\u21BA Reload clean state", on_click=cb_reload, use_container_width=True,
                help="Creates a brand new simulation with the configured process count.")
    d = st.columns([2, 1, 1, 2, 1, 1])
    d[0].selectbox("Process", list(range(sim.n)), key="sel_pid", format_func=lambda i: "P%d" % i)
    d[1].button("Request CS", on_click=cb_request, use_container_width=True,
                help="Selected process broadcasts a REQUEST (must be idle).")
    d[2].button("Release CS", on_click=cb_release, use_container_width=True,
                help="Selected process leaves the critical section and broadcasts RELEASE.")
    d[3].number_input("Concurrent requests", 1, sim.n, min(3, sim.n), key="conc_count",
                      help="How many idle processes request at the same instant.")
    d[4].button("Generate Concurrent Requests", on_click=cb_concurrent, use_container_width=True)
    d[5].button("Clear event history", on_click=cb_clear, use_container_width=True)

    left, right = st.columns([3, 2])
    with left:
        st.markdown("#### Process network")
        st.plotly_chart(network_figure(sim), use_container_width=True)
        st.caption("Arrows are messages currently in transit. The white arrow is the latest delivered message; "
                   "the white-outlined node is the process that handled the latest event.")
    with right:
        st.markdown("#### Messages in transit (%d)" % len(sim.pending))
        pend = [{"Id": x.msg_id, "Type": x.kind, "From": "P%d" % x.sender, "To": "P%d" % x.receiver, "ts": x.timestamp}
                for x in sim.pending[:50]]
        if pend:
            st.dataframe(pend, hide_index=True, use_container_width=True, height=260)
        else:
            st.info("No messages in transit.")
        st.markdown("#### Latest events")
        last = sim.visible_history()[-6:][::-1]
        if last:
            st.dataframe([{"#": e["seq"], "Type": e["type"], "What happened": e["explanation"]} for e in last],
                         hide_index=True, use_container_width=True, height=260)
        else:
            st.info("Event history is empty.")

    st.markdown("#### Processes and their local request queues")
    per_row = 5
    for start in range(0, sim.n, per_row):
        row = st.columns(per_row)
        for col, p in zip(row, sim.processes[start:start + per_row]):
            queue = " ".join("(%d,P%d)" % e for e in p.queue) or "empty"
            req = "-" if p.request_ts is None else p.request_ts
            col.markdown(
                '<div class="glass pcard%s"><b>P%d</b> <span class="pill" style="background:%s">%s</span>'
                '<div class="mono">clock: %d<br>request ts: %s<br>replies: %d/%d<br>queue: %s</div></div>'
                % (" active" if p.pid == sim.active_pid else "", p.pid, STATUS_COLORS[p.status], p.status,
                   p.clock, req, len(p.replies), len(p.peers), queue), unsafe_allow_html=True)

# ---------------------------------------------------------------- event log
elif page == "Event Log & Timeline":
    events = sim.visible_history()
    st.markdown("### Event log")
    f1, f2 = st.columns(2)
    types = f1.multiselect("Filter by event type", sorted({e["type"] for e in events}), key="f_types")
    procs = f2.multiselect("Filter by process", ["P%d" % i for i in range(sim.n)], key="f_procs")
    shown = [e for e in events
             if (not types or e["type"] in types)
             and (not procs or "P%d" % e["process"] in procs or e["sender"] in procs or e["receiver"] in procs)]
    if shown:
        st.dataframe(event_rows(shown), hide_index=True, use_container_width=True, height=420)
    else:
        st.info("No events to show.")
    st.markdown("### Timeline")
    st.plotly_chart(timeline_figure(shown, sim.n), use_container_width=True)
    st.markdown("### Export")
    x = st.columns(3)
    x[0].download_button("Download events (CSV)", events_to_csv(events), "lamport_events.csv", "text/csv",
                         use_container_width=True)
    x[1].download_button("Download report (JSON)", report_json(sim), "lamport_report.json", "application/json",
                         use_container_width=True)
    x[2].download_button("Download report (TXT)", report_txt(sim), "lamport_report.txt", "text/plain",
                         use_container_width=True)
    st.caption("Exports contain all visible (not cleared) events, ignoring the filters above.")

# ---------------------------------------------------------------- analytics
elif page == "Analytics":
    st.markdown("### Metrics (derived from the event history)")
    a = st.columns(4)
    kpi(a[0], "REQUEST messages", m["total_request"])
    kpi(a[1], "REPLY messages", m["total_reply"])
    kpi(a[2], "RELEASE messages", m["total_release"])
    kpi(a[3], "Total messages", m["total_messages"])
    b = st.columns(4)
    kpi(b[0], "Completed CS entries", m["cs_entries"])
    kpi(b[1], "Completed releases", m["releases"])
    kpi(b[2], "Avg messages / entry", "n/a" if m["avg_messages_per_entry"] is None else "%.2f" % m["avg_messages_per_entry"])
    kpi(b[3], "CS owner", m["cs_owner"] or "none")
    st.markdown("")
    if m["safety_ok"]:
        st.success("Mutual exclusion: OK - no safety violation has been detected.")
    else:
        st.error("Mutual exclusion VIOLATED - see the event log.")
    g1, g2 = st.columns(2)
    g1.markdown("#### Messages by type")
    g1.bar_chart(pd.DataFrame({"count": [m["total_request"], m["total_reply"], m["total_release"]]},
                              index=["REQUEST", "REPLY", "RELEASE"]))
    g2.markdown("#### Logical clock per process")
    g2.bar_chart(pd.DataFrame({"clock": [p.clock for p in sim.processes]}, index=["P%d" % p.pid for p in sim.processes]))
    st.caption("Expected cost: 3(N-1) messages per completed critical-section entry (N = %d, so %d)." % (sim.n, 3 * (sim.n - 1)))

# ---------------------------------------------------------------- learn
else:
    st.markdown("### Learn: Lamport's distributed mutual exclusion")
    st.info("**Entry condition:** a process may enter its critical section only when its request is first in its ordered "
            "local queue and it has received REPLY messages from every other participating process.")
    st.markdown("""
**Distributed mutual exclusion.** Several independent processes, each with only its own memory, share a resource that must be
used by one process at a time (the *critical section*). There is no shared lock, so they coordinate by sending messages.

**Why coordinate?** If two processes update the same resource together, results get corrupted. Mutual exclusion prevents that.

**Lamport logical clocks.** Real clocks on different machines disagree, so each process keeps a counter. It increases the counter
for its own events and, on receiving a message, sets `clock = max(clock, received) + 1`. This gives a consistent ordering of events.

**Messages.**
- **REQUEST(t):** "I want the critical section; my request has timestamp t." Sent to everyone.
- **REPLY:** "I received your request." Every receiver always answers.
- **RELEASE:** "I am done." Sent to everyone on exit, so they delete the request from their queues.

**Ordering.** Each process keeps a local queue sorted by `(timestamp, process id)`. Equal timestamps are broken by the lower process ID,
so every process sorts the same requests identically.

**Why all REPLYs?** With a REPLY from everyone, the requester knows every other process has seen its request and queued it, so nobody
can later sneak in ahead of it. Together with the queue-head rule this protects the critical section.

**Simultaneous requests.** If P0 and P1 both request at clock 1, the keys are (1,0) and (1,1). Every queue puts P0 first, so P0 enters, then P1.

**Starvation.** Requests are served in timestamp order and each exit sends RELEASE, so with reliable messages every request is eventually served.

**Assumptions and limits.** Reliable, FIFO message delivery; no crashed processes; a fixed, known set of processes.
**Cost:** 3(N-1) messages per entry (N-1 REQUEST, N-1 REPLY, N-1 RELEASE). A crashed process blocks everyone, because its REPLY never arrives.

**This is not a centralized lock.** No process or shared variable decides who enters; each process decides locally from its own queue and the replies it received.
""")
    st.markdown("#### Pseudocode")
    st.code("""request():   clock += 1; ts = clock; queue.insert((ts, me)); send REQUEST(ts) to all others
on REQUEST(ts) from j:  clock = max(clock, ts) + 1; queue.insert((ts, j)); clock += 1; send REPLY to j
on REPLY from j:        clock = max(clock, ts) + 1; replies.add(j)
enter CS when:          queue.head == (my_ts, me)  AND  replies == all other processes
release():  clock += 1; queue.remove((my_ts, me)); send RELEASE(my_ts) to all others
on RELEASE(my_ts) from j: clock = max(clock, ts) + 1; queue.remove((my_ts, j))""", language="text")
    st.markdown("#### Worked example (3 processes, generated live by the simulation engine)")
    ws = worked_example()
    st.markdown("P0, P1 and P2 all request at the same time. Each ticks its clock from 0 to 1, so all three requests have timestamp 1 "
                "and the tie is broken by process ID: the order is (1,P0), (1,P1), (1,P2). The table shows the real events "
                "(%d in total) and the resulting order of entry: %s."
                % (len(ws.history), ", ".join("P%d" % e["process"] for e in ws.history if e["type"] == "ENTER_CS")))
    st.dataframe(event_rows(ws.history), hide_index=True, use_container_width=True, height=420)
    st.markdown("#### Glossary")
    st.table(pd.DataFrame({"Term": ["Critical section", "Logical clock", "Timestamp", "Request queue", "REQUEST", "REPLY",
                                    "RELEASE", "Safety", "Progress"],
                           "Meaning": ["Code that only one process may run at a time", "Per-process counter used to order events",
                                       "Clock value attached to a request or message", "Local list of requests sorted by (timestamp, ID)",
                                       "Ask everyone for permission", "Acknowledge a request", "Announce leaving the critical section",
                                       "Never two processes in the critical section", "Waiting requests are eventually served"]}))

# ---------------------------------------------------------------- auto-run loop
if S.running:
    if sim.step(auto_release=True) is None:
        S.running = False
        notify("success", "Simulation complete: no more events to process.")
    else:
        time.sleep(1.0 / S.speed)
    st.rerun()
