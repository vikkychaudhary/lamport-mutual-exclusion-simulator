"""Plotly figures built only from real simulation state (no fabricated messages)."""
import math
from typing import Dict, List

import plotly.graph_objects as go

from .lamport import IDLE, IN_CS, RELEASE, REPLY, REQUEST, REQUESTING, WAITING

KIND_COLORS = {REQUEST: "#22d3ee", REPLY: "#34d399", RELEASE: "#f59e0b"}
KIND_SYMBOLS = {REQUEST: "circle", REPLY: "diamond", RELEASE: "square"}
STATUS_COLORS = {IDLE: "#64748b", REQUESTING: "#3b82f6", WAITING: "#8b5cf6", IN_CS: "#f43f5e"}
TEXT = "#e2e8f0"


def _positions(n: int) -> List[tuple]:
    return [(math.cos(2 * math.pi * i / n - math.pi / 2), math.sin(2 * math.pi * i / n - math.pi / 2)) for i in range(n)]


def _arrow(fig, a, b, color, width=1.6, dash=None, offset=0.05):
    dx, dy = b[0] - a[0], b[1] - a[1]
    length = math.hypot(dx, dy) or 1.0
    ux, uy = dx / length, dy / length
    px, py = -uy * offset, ux * offset          # shift so A->B and B->A do not overlap
    start = (a[0] + ux * 0.2 + px, a[1] + uy * 0.2 + py)
    end = (b[0] - ux * 0.2 + px, b[1] - uy * 0.2 + py)
    fig.add_annotation(x=end[0], y=end[1], ax=start[0], ay=start[1], xref="x", yref="y", axref="x", ayref="y",
                       showarrow=True, arrowhead=3, arrowsize=1.1, arrowwidth=width, arrowcolor=color, text="", opacity=0.9)
    return ((start[0] + end[0]) / 2, (start[1] + end[1]) / 2)


def network_figure(sim) -> go.Figure:
    pos = _positions(sim.n)
    fig = go.Figure()
    # legend entries
    for kind in (REQUEST, REPLY, RELEASE):
        fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers", name=kind,
                                 marker=dict(color=KIND_COLORS[kind], symbol=KIND_SYMBOLS[kind], size=10)))
    # messages in transit
    mids: Dict[str, Dict[str, list]] = {k: {"x": [], "y": [], "t": []} for k in KIND_COLORS}
    for m in sim.pending[:80]:
        mid = _arrow(fig, pos[m.sender], pos[m.receiver], KIND_COLORS[m.kind])
        mids[m.kind]["x"].append(mid[0])
        mids[m.kind]["y"].append(mid[1])
        mids[m.kind]["t"].append("%s P%d \u2192 P%d (t=%d)" % (m.kind, m.sender, m.receiver, m.timestamp))
    for kind, d in mids.items():
        if d["x"]:
            fig.add_trace(go.Scatter(x=d["x"], y=d["y"], mode="markers", text=d["t"], hoverinfo="text", showlegend=False,
                                     marker=dict(color=KIND_COLORS[kind], symbol=KIND_SYMBOLS[kind], size=9,
                                                 line=dict(color="#0b1020", width=1))))
    # latest delivered message (white dashed arrow)
    lm = sim.last_message
    if lm is not None:
        _arrow(fig, pos[lm.sender], pos[lm.receiver], "#ffffff", width=3, offset=-0.07)
    # process nodes
    fig.add_trace(go.Scatter(
        x=[p[0] for p in pos], y=[p[1] for p in pos], mode="markers+text", showlegend=False,
        text=["P%d<br>t=%d" % (q.pid, q.clock) for q in sim.processes], textfont=dict(color="white", size=13),
        hovertext=["P%d | %s | clock %d | queue %s | replies %d/%d" % (q.pid, q.status, q.clock, q.queue, len(q.replies), len(q.peers))
                   for q in sim.processes], hoverinfo="text",
        marker=dict(size=58, color=[STATUS_COLORS[q.status] for q in sim.processes],
                    line=dict(color=["#ffffff" if q.pid == sim.active_pid else "rgba(255,255,255,0.25)" for q in sim.processes],
                              width=[4 if q.pid == sim.active_pid else 1.5 for q in sim.processes]))))
    owner = sim.cs_holder()
    fig.add_annotation(x=0, y=0, xref="x", yref="y", showarrow=False, align="center",
                       text="<b>Critical Section</b><br>%s" % ("held by P%d" % owner if owner is not None else "free"),
                       font=dict(color=TEXT, size=13), bgcolor="rgba(244,63,94,0.25)" if owner is not None else "rgba(100,116,139,0.2)",
                       bordercolor="rgba(255,255,255,0.3)", borderpad=8)
    fig.update_layout(height=470, margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      xaxis=dict(visible=False, range=[-1.45, 1.45]), yaxis=dict(visible=False, range=[-1.4, 1.4], scaleanchor="x"),
                      legend=dict(orientation="h", y=-0.02, font=dict(color=TEXT)), font=dict(color=TEXT))
    return fig


def timeline_figure(events: List[Dict], n: int) -> go.Figure:
    fig = go.Figure()
    types = ["REQUEST", "RECV_REQUEST", "RECV_REPLY", "RECV_RELEASE", "ENTER_CS", "RELEASE", "SAFETY_VIOLATION"]
    colors = {"REQUEST": "#22d3ee", "RECV_REQUEST": "#38bdf8", "RECV_REPLY": "#34d399", "RECV_RELEASE": "#fbbf24",
              "ENTER_CS": "#f43f5e", "RELEASE": "#f59e0b", "SAFETY_VIOLATION": "#ffffff"}
    for t in types:
        sel = [e for e in events if e["type"] == t]
        if sel:
            fig.add_trace(go.Scatter(x=[e["seq"] for e in sel], y=["P%d" % e["process"] for e in sel], mode="markers", name=t,
                                     marker=dict(size=11, color=colors[t], symbol="diamond" if t == "ENTER_CS" else "circle"),
                                     text=["#%d %s<br>%s" % (e["seq"], e["type"], e["explanation"]) for e in sel], hoverinfo="text"))
    if not events:
        fig.add_annotation(text="No events yet", showarrow=False, font=dict(color=TEXT, size=16), xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(height=max(260, 48 * n + 120), margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(255,255,255,0.03)", font=dict(color=TEXT), legend=dict(orientation="h", y=-0.25),
                      xaxis=dict(title="Event sequence number", gridcolor="rgba(255,255,255,0.08)"),
                      yaxis=dict(categoryorder="array", categoryarray=["P%d" % i for i in reversed(range(n))],
                                 gridcolor="rgba(255,255,255,0.08)"))
    return fig
