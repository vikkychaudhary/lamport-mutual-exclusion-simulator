# Lamport Mutual Exclusion Simulator

A beginner-friendly, **local** web app (Streamlit) that simulates Lamport's distributed mutual exclusion algorithm with logical clocks and REQUEST / REPLY / RELEASE messages. It is not deployed online; it runs on your own computer.

## 1. Problem statement
Several independent processes share a resource that only one may use at a time (the *critical section*). They have no shared memory and no central lock, so they must agree on an order using messages only.

## 2. Objectives
Implement the real algorithm, make every message visible, prove mutual exclusion with automated tests, and teach the idea interactively.

## 3. Features
- Deterministic event-driven simulation (no threads), 2 to 10 processes.
- Controls: Start, Pause, Resume, Step Forward, Run Until Completion (with safety limit), Reset, Reload clean state, Request/Release CS per process, Generate Concurrent Requests, Clear event history, speed slider.
- Plotly process network (messages in transit, latest message, CS owner), per-process queue cards, event log with filters, timeline chart.
- Metrics derived from the event history; explicit mutual-exclusion status (violations are recorded, never hidden).
- Exports: CSV (events), JSON and TXT (report).
- Learn page with explanation, pseudocode, glossary and a worked 3-process example generated live by the engine.

## 4. Technology stack
Python 3.9+ (developed for 3.9.6), Streamlit, Plotly (pandas is installed with Streamlit). Algorithm, tests and exports use only the standard library. No databases, APIs, Node.js or Docker.

## 5. Architecture
`src/lamport.py` (per-process state and rules) -> `src/simulation.py` (message network, event loop, metrics) -> `src/exporters.py` and `src/visualization.py` -> `app.py` (UI only). The algorithm never imports Streamlit.

## 6. Directory structure
```
lamport-mutual-exclusion-simulator/
    app.py  README.md  requirements.txt  .gitignore  LICENSE
    src/        __init__.py lamport.py simulation.py visualization.py exporters.py
    tests/      __init__.py test_lamport.py test_simulation.py
    assets/     style.css
    exports/    .gitkeep
```

## 7. The algorithm
**Clock rules:** on a local or send event `clock += 1`; on receive `clock = max(clock, received_timestamp) + 1`.

**Workflow**
1. Process *i* increments its clock, puts `(ts, i)` in its own queue and sends `REQUEST(ts)` to all others.
2. Each receiver updates its clock, inserts `(ts, i)` into its queue (sorted by timestamp, then process ID) and sends `REPLY`.
3. *i* records REPLYs.
4. **Entry condition:** *i* enters only when its request is first in its ordered queue **and** it has REPLYs from every other process.
5. On exit *i* removes its request and broadcasts `RELEASE`; receivers delete that request. The next process at the head may enter.

Cost: 3(N-1) messages per entry. Equal timestamps are ordered by process ID.

## 8. How the simulator works
Each message goes into a single FIFO list (so channels are FIFO). `step()` handles exactly one event with this priority: (1) a process meeting the entry condition enters; (2) the oldest in-transit message is delivered; (3) if auto-release is on, the CS holder releases. Nothing is delivered silently. After every entry the engine checks that only one process is in the CS and that all REPLYs were received.

## 9. Installation
Python 3.9 or newer and `pip`. Run all commands from the project root (the folder containing `app.py`).

### Windows PowerShell
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m streamlit run app.py
python -m unittest discover -s tests -v
```
If activation is blocked ("running scripts is disabled"), run once for this window only:
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`
or skip activation and call the venv Python directly:
```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m streamlit run app.py
```

### macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m streamlit run app.py
python -m unittest discover -s tests -v
```
Streamlit prints a local address (usually http://localhost:8501); open it in a browser.

## 10. Exporting data
Open **Event Log & Timeline** and use the download buttons (CSV events, JSON report, TXT report). Exports include visible events only (not ones hidden by "Clear event history") and ignore the table filters.

## 11. Example walkthrough
Press **Generate Concurrent Requests** (3), then **Step Forward** repeatedly. P0, P1, P2 all request at clock 1, so the queue order is (1,P0), (1,P1), (1,P2). Each REQUEST is delivered and answered by a REPLY; P0 enters after receiving both REPLYs, releases, and then P1 and P2 follow. The Learn page shows the same run as a table.

## 12. Assumptions and limitations
Reliable FIFO delivery, no crashes or message loss, fixed process set, one outstanding request per process (extra requests are rejected). All processes run in one Python program; "network" delay is modelled by the ordered message list. Metrics' "queue length" is the longest local queue.

**Test status:** the 26 unit tests use only the standard library and were run and passed (Python 3.12). The Streamlit UI could not be launched in the build environment (Streamlit/Plotly were not installable offline), so `app.py` was only syntax-checked and exercised against stand-in modules. Please run the app once on your machine to confirm the UI.

## 13. Troubleshooting
- `No module named streamlit`: activate the venv and rerun the install command.
- `py` not found (Windows): use `python` instead.
- Port in use: `python -m streamlit run app.py --server.port 8502`.
- `No module named src`: run from the project root.
- Install fails on a very new Python: use Python 3.9 to 3.12.

## 14. Future ideas
Message delay and loss models, Ricart-Agrawala comparison, process crash simulation, animated message flight.

## 15. License
MIT (see `LICENSE`).
