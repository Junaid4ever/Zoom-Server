# ============================================
# ZOOM BOT CENTRAL – HACKER STYLE DASHBOARD
# ============================================
import os
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import socketio

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False
)

app = FastAPI(title="Zoom Bot Central")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

workers = {}
running_tasks = {}

class StartBotRequest(BaseModel):
    meeting_code: str
    passcode: str = ""
    bot_count: int = 10
    duration_minutes: int = 120
    name_type: str = "indian"
    custom_names: Optional[List[str]] = None
    join_mode: str = "individual"

class TerminateRequest(BaseModel):
    meeting_code: Optional[str] = None
    task_id: Optional[str] = None

@sio.event
async def connect(sid, environ):
    print(f"[SIO] Connected: {sid}")

@sio.event
async def disconnect(sid):
    # Remove worker and all its tasks immediately
    wid_to_remove = None
    for wid, info in workers.items():
        if info.get("sid") == sid:
            wid_to_remove = wid
            break
    if wid_to_remove:
        # Remove all tasks of this worker
        for tid in list(running_tasks.keys()):
            if running_tasks[tid].get("worker_id") == wid_to_remove:
                # Restore capacity
                wid = running_tasks[tid].get("worker_id")
                if wid and wid in workers:
                    workers[wid]["free_capacity"] = min(
                        workers[wid]["max_capacity"],
                        workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                    )
                del running_tasks[tid]
        del workers[wid_to_remove]
        print(f"[SIO] Worker {wid_to_remove} disconnected and all tasks removed.")
    else:
        print(f"[SIO] Disconnect from unknown sid: {sid}")

@sio.event
async def register_worker(sid, data):
    wid = data.get("worker_id", f"worker-{sid[:6]}")
    max_cap = int(data.get("max_capacity", 10))
    workers[wid] = {
        "sid": sid,
        "max_capacity": max_cap,
        "free_capacity": max_cap,
        "last_seen": datetime.now().isoformat()
    }
    await sio.emit("registered", {"worker_id": wid, "max_capacity": max_cap}, to=sid)
    print(f"[SIO] Registered {wid} | capacity={max_cap}")

@sio.event
async def update_capacity(sid, data):
    wid = data.get("worker_id")
    if wid in workers:
        workers[wid]["free_capacity"] = max(0, int(data.get("free_capacity", 0)))
        workers[wid]["last_seen"] = datetime.now().isoformat()

@sio.event
async def task_completed(sid, data):
    tid = data.get("task_id")
    if tid and tid in running_tasks:
        wid = running_tasks[tid].get("worker_id")
        if wid and wid in workers:
            workers[wid]["free_capacity"] = min(
                workers[wid]["max_capacity"],
                workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
            )
        del running_tasks[tid]
        print(f"[SIO] Task completed: {tid}")

@app.get("/health")
async def health():
    return {"ok": True, "workers": len(workers)}

@app.get("/status")
@app.get("/api/status")
async def status():
    total_free = sum(w.get("free_capacity", 0) for w in workers.values())
    now = datetime.now()
    for tid, task in running_tasks.items():
        if "started_at" in task:
            started = datetime.fromisoformat(task["started_at"])
            elapsed = (now - started).total_seconds() / 60
            task["elapsed_minutes"] = round(elapsed, 1)
            task["remaining_minutes"] = max(0, round(task.get("duration_minutes", 120) - elapsed, 1))
            if task["remaining_minutes"] <= 0:
                task["remaining_minutes"] = 0
    return {
        "workers": workers,
        "running_tasks": running_tasks,
        "total_free_capacity": total_free,
        "timestamp": now.isoformat()
    }

@app.post("/api/start-bots")
async def start_bots(req: StartBotRequest):
    if req.bot_count < 1:
        raise HTTPException(400, "bot_count must be >= 1")
    meeting = req.meeting_code.strip().replace(" ", "")
    if not meeting:
        raise HTTPException(400, "meeting_code required")

    remaining = req.bot_count
    assigned = []
    sorted_workers = sorted(
        workers.items(),
        key=lambda x: x[1].get("free_capacity", 0),
        reverse=True
    )

    for wid, info in sorted_workers:
        if remaining <= 0:
            break
        free = int(info.get("free_capacity", 0))
        if free <= 0 or not info.get("sid"):
            continue
        give = min(free, remaining)
        task_id = str(uuid.uuid4())[:8]
        payload = {
            "task_id": task_id,
            "meeting_code": meeting,
            "passcode": req.passcode or "",
            "bot_count": give,
            "duration_minutes": req.duration_minutes,
            "name_type": req.name_type or "indian",
            "custom_names": req.custom_names,
            "join_mode": req.join_mode or "individual"
        }
        await sio.emit("new_task", payload, to=info["sid"])
        running_tasks[task_id] = {
            "task_id": task_id,
            "meeting_code": meeting,
            "bot_count": give,
            "worker_id": wid,
            "name_type": payload["name_type"],
            "duration_minutes": req.duration_minutes,
            "started_at": datetime.now().isoformat(),
            "remaining_minutes": req.duration_minutes,
            "join_mode": req.join_mode or "individual"
        }
        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give, "task_id": task_id})
        remaining -= give
        print(f"[API] Task {task_id} → {wid} ({give} bots) mode={req.join_mode}")

    if not assigned:
        raise HTTPException(503, "No free capacity or no connected workers. Start worker first.")

    return {
        "success": True,
        "message": f"Started {req.bot_count - remaining} bots",
        "assigned": assigned,
        "remaining_unassigned": remaining
    }

@app.post("/api/terminate")
@app.post("/api/kill-meeting")
async def terminate(req: Optional[TerminateRequest] = None):
    if req and req.task_id:
        task_id = req.task_id
        if task_id not in running_tasks:
            raise HTTPException(404, "Task not found")
        meeting = running_tasks[task_id].get("meeting_code")
        wid = running_tasks[task_id].get("worker_id")
        if wid in workers and workers[wid].get("sid"):
            await sio.emit("terminate", {"task_id": task_id, "meeting_code": meeting}, to=workers[wid]["sid"])
        # Restore capacity
        if wid and wid in workers:
            workers[wid]["free_capacity"] = min(
                workers[wid]["max_capacity"],
                workers[wid].get("free_capacity", 0) + running_tasks[task_id].get("bot_count", 0)
            )
        del running_tasks[task_id]
        print(f"[API] Terminate task {task_id}")
        return {"success": True, "message": f"Task {task_id} terminated"}

    elif req and req.meeting_code:
        meeting = req.meeting_code
        to_kill = [tid for tid, t in running_tasks.items() if t.get("meeting_code") == meeting]
        for tid in to_kill:
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid, "meeting_code": meeting}, to=workers[wid]["sid"])
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
            del running_tasks[tid]
        print(f"[API] Terminate meeting {meeting}")
        return {"success": True, "message": f"Meeting {meeting} terminated"}

    else:
        # Kill ALL
        for tid in list(running_tasks.keys()):
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid, "meeting_code": None}, to=workers[wid]["sid"])
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
        running_tasks.clear()
        print(f"[API] Terminate ALL")
        return {"success": True, "message": "All tasks terminated"}

# ============================================
# HACKER-STYLE DASHBOARD HTML
# ============================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes"/>
<title>🔒 Junaid Members Panel</title>
<style>
    /* Matrix-style dark theme */
    * { margin:0; padding:0; box-sizing:border-box; }
    body {
        background: #0a0f0a;
        font-family: 'Courier New', monospace;
        color: #00ff41;
        padding: 12px;
        min-height: 100vh;
        background-image: radial-gradient(circle at 20% 50%, rgba(0,255,65,0.03) 0%, transparent 50%);
    }
    .container { max-width:1400px; margin:0 auto; }
    .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 8px 16px;
        border: 1px solid #00ff41;
        border-radius: 6px;
        background: rgba(0,255,65,0.05);
        margin-bottom: 14px;
        flex-wrap: wrap;
        gap: 6px;
        box-shadow: 0 0 20px rgba(0,255,65,0.1);
    }
    .header h1 {
        font-size: 20px;
        font-weight: 700;
        text-shadow: 0 0 10px #00ff41;
        letter-spacing: 2px;
    }
    .header h1 span { color: #00cc33; text-shadow: 0 0 5px #00cc33; }
    .header-actions {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
    }
    .status-badge {
        display: flex;
        align-items: center;
        gap: 4px;
        border: 1px solid #00ff41;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 12px;
        background: rgba(0,255,65,0.05);
    }
    .status-badge .dot {
        width: 6px; height: 6px;
        border-radius: 50%;
        background: #00ff41;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.2} }
    .theme-toggle, .mode-switch {
        border: 1px solid #00ff41;
        border-radius: 30px;
        padding: 2px 10px;
        cursor: pointer;
        background: rgba(0,255,65,0.05);
        color: #00ff41;
        font-size: 18px;
        height: 30px;
        display: flex;
        align-items: center;
        gap: 4px;
        transition: all 0.3s;
    }
    .theme-toggle:hover, .mode-switch:hover { background: rgba(0,255,65,0.15); border-color: #00ff41; }
    .mode-switch label {
        display: flex;
        align-items: center;
        gap: 4px;
        cursor: pointer;
        font-size: 11px;
        font-weight: 500;
    }
    .mode-switch input[type="checkbox"] {
        appearance: none;
        width: 28px; height: 16px;
        background: #1a2a1a;
        border-radius: 20px;
        position: relative;
        cursor: pointer;
        transition: background 0.3s;
        flex-shrink: 0;
        border: 1px solid #00ff41;
    }
    .mode-switch input[type="checkbox"]::after {
        content: '';
        position: absolute;
        top: 2px; left: 2px;
        width: 10px; height: 10px;
        background: #00ff41;
        border-radius: 50%;
        transition: transform 0.3s;
    }
    .mode-switch input[type="checkbox"]:checked { background: #00ff41; }
    .mode-switch input[type="checkbox"]:checked::after { transform: translateX(12px); background: #0a0f0a; }
    .mode-label { white-space: nowrap; font-size: 11px; }
    .mode-label.active { color: #00ff41; text-shadow: 0 0 8px #00ff41; }

    .stats-row {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(70px, 1fr));
        gap: 6px;
        margin-bottom: 12px;
    }
    .stat-item {
        border: 1px solid #00ff41;
        border-radius: 6px;
        padding: 6px 4px;
        text-align: center;
        background: rgba(0,255,65,0.03);
        box-shadow: 0 0 10px rgba(0,255,65,0.05);
    }
    .stat-item .num {
        font-size: 20px;
        font-weight: 700;
        color: #00ff41;
        line-height: 1.2;
        text-shadow: 0 0 15px #00ff41;
    }
    .stat-item .num.green { color: #00ff41; }
    .stat-item .num.red { color: #ff3333; }
    .stat-item .num.yellow { color: #ffaa00; }
    .stat-item .label {
        font-size: 9px;
        color: #66cc88;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-top: 2px;
    }
    .stat-item.highlight { border-color: #00ff41; background: rgba(0,255,65,0.08); }
    .stat-item.highlight .num { font-size: 24px; }

    .main-grid {
        display: grid;
        grid-template-columns: 1fr 240px;
        gap: 12px;
    }
    @media (max-width: 820px) { .main-grid { grid-template-columns: 1fr; } }

    .card {
        border: 1px solid #00ff41;
        border-radius: 6px;
        padding: 12px;
        margin-bottom: 12px;
        background: rgba(0,255,65,0.02);
        box-shadow: 0 0 15px rgba(0,255,65,0.05);
    }
    .card-title {
        font-size: 12px;
        color: #66cc88;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 8px;
        border-bottom: 1px dashed #00ff41;
        padding-bottom: 4px;
    }

    .form-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 6px;
    }
    @media (max-width: 500px) { .form-grid { grid-template-columns: 1fr; } }
    .form-group { display: flex; flex-direction: column; gap: 2px; }
    .form-group label { font-size: 10px; color: #66cc88; }
    .form-group input, .form-group select, .form-group textarea {
        padding: 4px 6px;
        background: #0a0f0a;
        border: 1px solid #00ff41;
        border-radius: 4px;
        color: #00ff41;
        font-size: 13px;
        font-family: 'Courier New', monospace;
        outline: none;
    }
    .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
        border-color: #00ff41;
        box-shadow: 0 0 10px rgba(0,255,65,0.2);
    }
    .form-group textarea { resize: vertical; font-size: 12px; }

    #customBox {
        display: none;
        margin-top: 6px;
        padding: 8px;
        border: 1px solid #00ff41;
        border-radius: 4px;
        background: rgba(0,255,65,0.03);
    }
    #customBox .name-status { font-size: 10px; color: #66cc88; margin-top: 4px; }
    #customBox .name-status .ok { color: #00ff41; }
    #customBox .name-status .err { color: #ff3333; }

    .actions {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-top: 10px;
    }
    .btn {
        padding: 4px 12px;
        border: 1px solid #00ff41;
        border-radius: 4px;
        background: transparent;
        color: #00ff41;
        font-family: 'Courier New', monospace;
        font-weight: 600;
        font-size: 13px;
        cursor: pointer;
        transition: all 0.3s;
    }
    .btn-primary { background: #00ff41; color: #0a0f0a; text-shadow: none; }
    .btn-primary:hover { background: #00cc33; box-shadow: 0 0 20px #00ff41; }
    .btn-danger { border-color: #ff3333; color: #ff3333; }
    .btn-danger:hover { background: #ff3333; color: #0a0f0a; box-shadow: 0 0 20px #ff3333; }
    .btn-outline { color: #66cc88; border-color: #66cc88; }
    .btn-outline:hover { background: rgba(0,255,65,0.1); }
    .btn-sm { padding: 1px 8px; font-size: 11px; }

    .log {
        margin-top: 8px;
        padding: 4px 8px;
        border: 1px solid #00ff41;
        border-radius: 4px;
        background: rgba(0,255,65,0.02);
        font-family: 'Courier New', monospace;
        font-size: 12px;
        min-height: 24px;
        color: #66cc88;
    }
    .log .ok { color: #00ff41; }
    .log .err { color: #ff3333; }
    .log .info { color: #66cc88; }

    .workers-panel {
        border: 1px solid #00ff41;
        border-radius: 6px;
        padding: 10px;
        background: rgba(0,255,65,0.02);
        position: sticky;
        top: 10px;
    }
    .workers-panel .panel-title {
        font-size: 11px;
        color: #66cc88;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 6px;
        display: flex;
        justify-content: space-between;
    }
    .workers-panel .panel-title span { color: #00ff41; }
    .workers-scroll {
        max-height: 300px;
        overflow-y: auto;
        padding-right: 2px;
    }
    .workers-scroll::-webkit-scrollbar { width: 3px; }
    .workers-scroll::-webkit-scrollbar-track { background: #0a0f0a; }
    .workers-scroll::-webkit-scrollbar-thumb { background: #00ff41; border-radius: 4px; }
    .worker-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 4px 6px;
        border: 1px solid #00ff41;
        border-radius: 4px;
        margin-bottom: 3px;
        font-size: 11px;
        font-family: 'Courier New', monospace;
        background: rgba(0,255,65,0.02);
    }
    .worker-item .name { color: #00ff41; }
    .worker-item .cap { color: #66cc88; }
    .worker-item .cap .free { color: #00ff41; }

    .table-wrap { overflow-x: auto; }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
    }
    th, td {
        padding: 4px 6px;
        text-align: left;
        border-bottom: 1px solid #00ff41;
        vertical-align: middle;
    }
    th {
        color: #66cc88;
        font-weight: 500;
        font-size: 9px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    tr:hover td { background: rgba(0,255,65,0.05); }
    .meeting-code {
        font-weight: 600;
        color: #00ff41;
        font-family: monospace;
        font-size: 13px;
        cursor: pointer;
        text-shadow: 0 0 8px #00ff41;
    }
    .meeting-code:hover { text-decoration: underline; }
    .badge {
        display: inline-block;
        padding: 0 6px;
        border-radius: 12px;
        font-size: 9px;
        font-weight: 500;
        line-height: 16px;
        border: 1px solid;
    }
    .badge-indian { border-color: #00ff41; color: #00ff41; }
    .badge-english { border-color: #66cc88; color: #66cc88; }
    .badge-custom { border-color: #ffaa00; color: #ffaa00; }

    .timer-bar {
        display: flex;
        align-items: center;
        gap: 4px;
    }
    .timer-bar .progress {
        flex: 1;
        height: 3px;
        background: #1a2a1a;
        border-radius: 4px;
        overflow: hidden;
        border: 1px solid #00ff41;
    }
    .timer-bar .progress .fill {
        height: 100%;
        border-radius: 4px;
        transition: width 1s linear;
        background: #00ff41;
    }
    .timer-bar .time-text {
        font-family: monospace;
        font-size: 10px;
        min-width: 28px;
        text-align: right;
        color: #66cc88;
    }
    .timer-bar .time-text.warning { color: #ffaa00; }
    .timer-bar .time-text.danger { color: #ff3333; }

    .empty { text-align: center; color: #66cc88; padding: 12px 0; font-size: 11px; }
    .footer-meta {
        margin-top: 6px;
        padding-top: 6px;
        border-top: 1px solid #00ff41;
        font-size: 10px;
        color: #66cc88;
    }

    @media (max-width: 600px) {
        body { padding: 6px; }
        .header h1 { font-size: 16px; }
        .status-badge { font-size: 9px; padding: 0 6px; }
        .theme-toggle, .mode-switch { font-size: 14px; padding: 0 6px; height: 24px; }
        .stats-row { grid-template-columns: repeat(3, 1fr); gap: 4px; }
        .stat-item .num { font-size: 16px; }
        .stat-item.highlight .num { font-size: 18px; }
        .stat-item { padding: 3px 2px; }
        .stat-item .label { font-size: 7px; }
        .card { padding: 8px; }
        .form-group input, .form-group select, .form-group textarea { font-size: 16px; padding: 6px; }
        .btn { font-size: 13px; padding: 6px 12px; }
        .workers-panel { position: static; }
        table { font-size: 10px; }
        th, td { padding: 2px 4px; }
        .meeting-code { font-size: 10px; }
        .timer-bar .time-text { font-size: 9px; }
    }
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🔒 Junaid <span>Members Panel</span></h1>
        <div class="header-actions">
            <div class="status-badge"><span class="dot"></span><span id="statusText">Connected</span><span style="margin-left:4px;color:#66cc88;">|</span><span id="liveTime" style="font-size:11px;"></span></div>
            <div class="mode-switch">
                <label>
                    <span class="mode-label" id="modeLabel">Individual</span>
                    <input type="checkbox" id="modeToggle" />
                    <span class="mode-label" id="modeLabel2">Together</span>
                </label>
            </div>
            <button class="theme-toggle" id="themeToggle" aria-label="Toggle theme" style="font-size:16px;">🌙</button>
        </div>
    </div>

    <div class="stats-row">
        <div class="stat-item highlight"><div class="num" id="totalCap">0</div><div class="label">Total Capacity</div></div>
        <div class="stat-item"><div class="num green" id="freeCap">0</div><div class="label">Free Capacity</div></div>
        <div class="stat-item"><div class="num" id="workersN">0</div><div class="label">Workers</div></div>
        <div class="stat-item"><div class="num" id="tasksN">0</div><div class="label">Active Tasks</div></div>
        <div class="stat-item"><div class="num" id="botsN">0</div><div class="label">Running Bots</div></div>
    </div>

    <div class="main-grid">
        <div>
            <div class="card">
                <div class="card-title">▶ Start New Meeting</div>
                <div class="form-grid">
                    <div class="form-group"><label>Meeting ID</label><input id="meetingId" placeholder="5415403058"/></div>
                    <div class="form-group"><label>Passcode</label><input id="passcode" placeholder="optional"/></div>
                    <div class="form-group"><label>Bots</label><input type="number" id="botCount" value="10" min="1" max="500" oninput="updCount()"/></div>
                    <div class="form-group"><label>Duration (min)</label><input type="number" id="duration" value="120" min="1"/></div>
                    <div class="form-group" style="grid-column:1/-1">
                        <label>Name Type</label>
                        <select id="nameType" onchange="toggleCustom()">
                            <option value="indian">🇮🇳 Indian</option>
                            <option value="english">🇺🇸 English</option>
                            <option value="custom">✏️ Custom</option>
                        </select>
                    </div>
                </div>
                <div id="customBox">
                    <label style="font-size:10px;color:#66cc88;">Custom names (one per line)</label>
                    <textarea id="customNames" rows="3" placeholder="Rahul Sharma&#10;Arjun Singh"></textarea>
                    <div class="name-status">Names: <strong id="nameCount">0</strong> &nbsp;|&nbsp; Need: <strong id="needCount">10</strong><span id="nameStatus"></span></div>
                </div>
                <div class="actions">
                    <button class="btn btn-primary" onclick="startBots()">🚀 Start</button>
                    <button class="btn btn-outline" onclick="refresh()">⟳ Refresh</button>
                </div>
                <div id="msg" class="log">[SYSTEM] Ready</div>
            </div>

            <div class="card">
                <div class="card-title" style="display:flex;justify-content:space-between">
                    <span>📋 Active Meetings</span>
                    <span id="taskCount" style="color:#66cc88;text-transform:none;">0 running</span>
                </div>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>Task</th><th>Meeting</th><th>Bots</th><th>Type</th><th>Mode</th><th>Time Left</th><th style="text-align:center">Action</th></tr></thead>
                        <tbody id="tbody"><tr><td colspan="7" class="empty">No active meetings</td></tr></tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="workers-panel">
            <div class="panel-title"><span>🖥️ Workers</span><span id="workerCount">0</span></div>
            <div class="workers-scroll" id="wlist"><div class="empty">No workers connected</div></div>
            <div class="footer-meta">Total: <strong id="totalCapFooter">0</strong> &nbsp;|&nbsp; Free: <strong id="freeCapFooter">0</strong></div>
        </div>
    </div>
</div>

<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<script>
const API = location.origin;
const sio = io(API);
const $ = id => document.getElementById(id);
const meetingId = $('meetingId'), passcode = $('passcode'), botCount = $('botCount');
const duration = $('duration'), nameType = $('nameType'), customNames = $('customNames');
const customBox = $('customBox'), msg = $('msg'), tbody = $('tbody'), wlist = $('wlist');
const totalCap = $('totalCap'), freeCap = $('freeCap'), workersN = $('workersN');
const tasksN = $('tasksN'), botsN = $('botsN'), totalCapFooter = $('totalCapFooter');
const freeCapFooter = $('freeCapFooter'), workerCount = $('workerCount'), taskCount = $('taskCount');
const statusText = $('statusText'), liveTime = $('liveTime');
const themeToggle = $('themeToggle'), modeToggle = $('modeToggle'), modeLabel = $('modeLabel'), modeLabel2 = $('modeLabel2');

// Theme
function getTheme(){ return localStorage.getItem('junaid_theme') || 'dark'; }
function setTheme(theme){ 
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('junaid_theme', theme);
    themeToggle.textContent = theme === 'dark' ? '🌙' : '☀️';
}
setTheme(getTheme());
themeToggle.addEventListener('click', ()=>{
    const current = getTheme();
    setTheme(current === 'dark' ? 'light' : 'dark');
});

// Mode
function getMode(){ return localStorage.getItem('junaid_mode') || 'individual'; }
function setMode(mode){
    localStorage.setItem('junaid_mode', mode);
    const checked = mode === 'together';
    modeToggle.checked = checked;
    modeLabel.style.color = checked ? '' : '#00ff41';
    modeLabel2.style.color = checked ? '#00ff41' : '';
}
setMode(getMode());
modeToggle.addEventListener('change', ()=>{
    const mode = modeToggle.checked ? 'together' : 'individual';
    setMode(mode);
});

function show(m, type='info'){
    const cls = type==='ok'?'ok':type==='err'?'err':'info';
    msg.innerHTML = `<span class="${cls}">[${new Date().toLocaleTimeString()}] ${m}</span>`;
}
function toggleCustom(){
    customBox.style.display = nameType.value === 'custom' ? 'block' : 'none';
    updCount();
}
function updCount(){
    const bots = parseInt(botCount.value) || 0;
    const names = customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);
    $('nameCount').textContent = names.length;
    $('needCount').textContent = bots;
    const st = $('nameStatus');
    if(nameType.value !== 'custom'){ st.innerHTML = ''; return; }
    st.innerHTML = names.length >= bots ? ' <span class="ok">✅</span>' : ` <span class="err">❌ Need ${bots - names.length} more</span>`;
}
customNames.addEventListener('input', updCount);

function updateClock(){
    liveTime.textContent = new Date().toLocaleTimeString();
}
setInterval(updateClock, 1000);
updateClock();

async function refresh(){
    try{
        const r = await fetch(API+'/status');
        const d = await r.json();
        const workers = d.workers || {};
        const tasks = d.running_tasks || {};

        let total=0, free=0;
        Object.values(workers).forEach(w=>{
            total += w.max_capacity || 0;
            free += w.free_capacity || 0;
        });
        const wc = Object.keys(workers).length;
        const tc = Object.keys(tasks).length;
        let rb = 0;
        Object.values(tasks).forEach(t=>{ rb += t.bot_count || 0; });

        totalCap.textContent = total;
        freeCap.textContent = free;
        workersN.textContent = wc;
        tasksN.textContent = tc;
        botsN.textContent = rb;
        totalCapFooter.textContent = total;
        freeCapFooter.textContent = free;
        workerCount.textContent = wc;
        taskCount.textContent = tc + ' running';

        if(!wc){
            wlist.innerHTML = '<div class="empty">No workers connected</div>';
        } else {
            wlist.innerHTML = Object.entries(workers).map(([id,w])=>{
                const f = w.free_capacity ?? 0;
                const m = w.max_capacity ?? 0;
                const pct = m>0 ? Math.round((f/m)*100) : 0;
                return `<div class="worker-item"><span class="name">🟢 ${id}</span><span class="cap">${f}/${m} <span class="free">(${pct}%)</span></span></div>`;
            }).join('');
        }

        if(!Object.keys(tasks).length){
            tbody.innerHTML = '<tr><td colspan="7" class="empty">No active meetings</td></tr>';
        } else {
            tbody.innerHTML = Object.entries(tasks).map(([tid,t])=>{
                const meeting = t.meeting_code || 'N/A';
                const bots = t.bot_count || 0;
                const type = t.name_type || 'indian';
                const mode = t.join_mode || 'individual';
                const remaining = t.remaining_minutes ?? t.duration_minutes || 120;
                const totalDur = t.duration_minutes || 120;
                const pct = totalDur>0 ? ((totalDur - Math.max(0,remaining)) / totalDur * 100) : 0;
                const clamped = Math.min(100, Math.max(0, pct));
                const warn = remaining < 5 ? 'danger' : remaining < 15 ? 'warning' : '';
                const typeBadge = type === 'indian' ? 'indian' : type === 'english' ? 'english' : 'custom';
                const modeIcon = mode === 'together' ? '👥' : '🚶';
                return `<tr>
                    <td style="font-size:10px;color:#66cc88;">${tid}</td>
                    <td class="meeting-code" onclick="alert('Screenshot not supported')">${meeting}</td>
                    <td>${bots}</td>
                    <td><span class="badge badge-${typeBadge}">${type}</span></td>
                    <td>${modeIcon}</td>
                    <td>
                        <div class="timer-bar">
                            <div class="progress"><div class="fill" style="width:${clamped}%;background:${remaining<5?'#ff3333':remaining<15?'#ffaa00':'#00ff41'}"></div></div>
                            <span class="time-text ${warn}">${remaining>0?Math.ceil(remaining)+'m':'0m'}</span>
                        </div>
                    </td>
                    <td style="text-align:center"><button class="btn btn-danger btn-sm" onclick="killTask('${tid}')">✕</button></td>
                </tr>`;
            }).join('');
        }
        statusText.textContent = 'Connected';
        show('Status refreshed', 'ok');
    } catch(e){
        statusText.textContent = 'Offline';
        show(e.message || 'Refresh failed', 'err');
    }
}

async function startBots(){
    const meeting = meetingId.value.trim().replace(/\s/g,'');
    const pass = passcode.value.trim();
    const bots = parseInt(botCount.value) || 10;
    const dur = parseInt(duration.value) || 120;
    const type = nameType.value;
    const mode = getMode();
    let custom = null;
    if(type === 'custom'){
        custom = customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);
        if(custom.length < bots) return show('Need '+(bots - custom.length)+' more names', 'err');
    }
    if(!meeting) return show('Meeting ID required', 'err');
    try{
        show('Starting...', 'info');
        const r = await fetch(API+'/api/start-bots', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({
                meeting_code: meeting, passcode: pass, bot_count: bots,
                duration_minutes: dur, name_type: type, custom_names: custom,
                join_mode: mode
            })
        });
        const d = await r.json();
        if(r.ok){
            show(d.message || 'Started!', 'ok');
            setTimeout(refresh, 1000);
        } else {
            show(d.detail || 'Failed', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

async function killTask(taskId){
    if(!confirm(`Kill task ${taskId}?`)) return;
    try{
        const r = await fetch(API+'/api/terminate', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({task_id: taskId})
        });
        const d = await r.json();
        if(r.ok){
            show(`✅ Kill sent`, 'ok');
            setTimeout(refresh, 1000);
        } else {
            show(d.detail || 'Kill failed', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

setInterval(refresh, 5000);
refresh();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(asgi_app, host="0.0.0.0", port=port)
