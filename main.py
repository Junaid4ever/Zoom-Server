# ============================================
# ZOOM BOT CENTRAL – FULL (LOGS + SCREENSHOT + FIX)
# ============================================
import os
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List
import base64

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import socketio

# ----- SOCKET.IO SERVER -----
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

# ----- IN-MEMORY STORAGE -----
workers = {}             # worker_id -> {sid, max_capacity, free_capacity, last_seen}
running_tasks = {}       # task_id -> {meeting_code, bot_count, worker_id, ...}
bot_logs = {}            # task_id -> list of log entries

# ----- PYDANTIC MODELS -----
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

# ----- SOCKET.IO EVENTS -----
@sio.event
async def connect(sid, environ):
    print(f"[SIO] Connected: {sid}")

@sio.event
async def disconnect(sid):
    # Remove worker only if it has no running tasks
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            has_tasks = any(t.get("worker_id") == wid for t in running_tasks.values())
            if not has_tasks:
                del workers[wid]
                print(f"[SIO] Worker removed: {wid} (no tasks)")
            else:
                # Keep worker but mark as offline (sid = None)
                workers[wid]["sid"] = None
                workers[wid]["last_seen"] = datetime.now().isoformat()
                print(f"[SIO] Worker {wid} offline (tasks remain)")
            break

@sio.event
async def register_worker(sid, data):
    wid = data.get("worker_id", f"worker-{sid[:6]}")
    max_cap = int(data.get("max_capacity", 10))
    now = datetime.now().isoformat()

    if wid in workers:
        # Reconnect – keep free_capacity, update sid
        workers[wid]["sid"] = sid
        workers[wid]["max_capacity"] = max_cap
        workers[wid]["last_seen"] = now
        print(f"[SIO] Worker {wid} reconnected, free_capacity unchanged: {workers[wid]['free_capacity']}")
    else:
        workers[wid] = {
            "sid": sid,
            "max_capacity": max_cap,
            "free_capacity": max_cap,
            "last_seen": now
        }
        print(f"[SIO] New worker {wid} | capacity={max_cap}")

    await sio.emit("registered", {"worker_id": wid, "max_capacity": max_cap}, to=sid)

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
        # Also clean logs
        if tid in bot_logs:
            del bot_logs[tid]
        print(f"[SIO] Task completed: {tid}")

@sio.event
async def bot_log(sid, data):
    """Receive log from worker and broadcast to all clients"""
    task_id = data.get("task_id")
    if task_id not in bot_logs:
        bot_logs[task_id] = []
    bot_logs[task_id].append(data)
    # Broadcast to all connected dashboard clients
    await sio.emit("new_log", data)

@sio.event
async def request_screenshot(sid, data):
    """Client requests screenshot for a task; forward to worker"""
    task_id = data.get("task_id")
    if task_id not in running_tasks:
        await sio.emit("screenshot_response", {"task_id": task_id, "error": "Task not found"}, to=sid)
        return
    wid = running_tasks[task_id].get("worker_id")
    if wid not in workers or not workers[wid].get("sid"):
        await sio.emit("screenshot_response", {"task_id": task_id, "error": "Worker offline"}, to=sid)
        return
    worker_sid = workers[wid]["sid"]
    await sio.emit("request_screenshot", {"task_id": task_id}, to=worker_sid)

@sio.event
async def screenshot_response(sid, data):
    """Worker sends screenshot; broadcast to all clients"""
    # Broadcast so anyone viewing dashboard can see it
    await sio.emit("screenshot_response", data)

# ----- API ENDPOINTS -----
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
        if free <= 0:
            continue
        # Only assign if worker is connected (sid not None)
        if not info.get("sid"):
            print(f"⚠️ Worker {wid} is offline, skipping")
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
        raise HTTPException(503, "No free capacity or no connected workers. Start Colab worker first.")

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
        # Send terminate to worker if connected
        if wid in workers and workers[wid].get("sid"):
            await sio.emit("terminate", {"task_id": task_id, "meeting_code": meeting}, to=workers[wid]["sid"])
        # Restore capacity
        if wid and wid in workers:
            workers[wid]["free_capacity"] = min(
                workers[wid]["max_capacity"],
                workers[wid].get("free_capacity", 0) + running_tasks[task_id].get("bot_count", 0)
            )
        del running_tasks[task_id]
        if task_id in bot_logs:
            del bot_logs[task_id]
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
            if tid in bot_logs:
                del bot_logs[tid]
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
        bot_logs.clear()
        print(f"[API] Terminate ALL")
        return {"success": True, "message": "All tasks terminated"}

# ============================================
# REDESIGNED DASHBOARD WITH LOGS & SCREENSHOT
# ============================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
    <meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes"/>
    <title>Junaid Members Panel (Zoom)</title>
    <style>
        :root {
            --bg-body: #0a0e17;
            --bg-card: #0d1117;
            --bg-input: #0d1117;
            --border-color: #21262d;
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --text-muted: #484f58;
            --accent-blue: #58a6ff;
            --accent-green: #3fb950;
            --accent-red: #f85149;
            --accent-yellow: #d29922;
            --shadow: 0 4px 12px rgba(0,0,0,0.4);
            --header-bg: linear-gradient(135deg, #0d1b2a, #1b2d45);
            --header-border: #1e3a5f;
            --badge-indian: #1a3a2a;
            --badge-english: #1a2a4a;
            --badge-custom: #3a2a1a;
            --scrollbar-thumb: #30363d;
            --hover-bg: #161b22;
            --stat-bg: rgba(88,166,255,0.08);
            --log-info: #8b949e;
            --log-success: #3fb950;
            --log-error: #f85149;
            --log-warning: #d29922;
        }
        [data-theme="light"] {
            --bg-body: #f0f6fc;
            --bg-card: #ffffff;
            --bg-input: #ffffff;
            --border-color: #d0d7de;
            --text-primary: #1f2328;
            --text-secondary: #656d76;
            --text-muted: #8b949e;
            --accent-blue: #0969da;
            --accent-green: #1a7f37;
            --accent-red: #cf222e;
            --accent-yellow: #9a6700;
            --shadow: 0 4px 12px rgba(0,0,0,0.08);
            --header-bg: linear-gradient(135deg, #e1ecf4, #d0dbe8);
            --header-border: #b0c4de;
            --badge-indian: #daf0d5;
            --badge-english: #d5e4f0;
            --badge-custom: #f0e4d5;
            --scrollbar-thumb: #c0c8d0;
            --hover-bg: #f6f8fa;
            --stat-bg: rgba(9,105,218,0.08);
            --log-info: #656d76;
            --log-success: #1a7f37;
            --log-error: #cf222e;
            --log-warning: #9a6700;
        }
        * { margin:0; padding:0; box-sizing:border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: var(--bg-body);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 12px;
            transition: background 0.3s, color 0.3s;
        }
        .container { max-width:1400px; margin:0 auto; }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 16px;
            background: var(--header-bg);
            border-radius: 14px;
            border: 1px solid var(--header-border);
            margin-bottom: 14px;
            flex-wrap: wrap;
            gap: 8px;
            transition: background 0.3s, border-color 0.3s;
        }
        .header h1 {
            font-size: 20px;
            font-weight: 700;
            background: linear-gradient(90deg, var(--accent-blue), #79c0ff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: 0.2px;
        }
        .header h1 span { font-weight:300; color:var(--text-secondary); -webkit-text-fill-color:var(--text-secondary); }
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
            background: var(--bg-card);
            padding: 2px 10px;
            border-radius: 20px;
            border: 1px solid var(--border-color);
            font-size: 12px;
            white-space: nowrap;
        }
        .status-badge .dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: var(--accent-green);
            animation: pulse 2s infinite;
        }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
        .theme-toggle, .mode-switch {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 30px;
            padding: 2px 10px;
            cursor: pointer;
            font-size: 18px;
            line-height: 1;
            transition: all 0.2s;
            color: var(--text-primary);
            height: 30px;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .theme-toggle:hover, .mode-switch:hover { transform: scale(1.05); border-color: var(--accent-blue); }
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
            width: 28px;
            height: 16px;
            background: var(--border-color);
            border-radius: 20px;
            position: relative;
            cursor: pointer;
            transition: background 0.3s;
            flex-shrink: 0;
        }
        .mode-switch input[type="checkbox"]::after {
            content: '';
            position: absolute;
            top: 2px;
            left: 2px;
            width: 12px;
            height: 12px;
            background: var(--bg-card);
            border-radius: 50%;
            transition: transform 0.3s;
        }
        .mode-switch input[type="checkbox"]:checked { background: var(--accent-blue); }
        .mode-switch input[type="checkbox"]:checked::after { transform: translateX(12px); }
        .mode-label { white-space: nowrap; }
        .mode-label.active { color: var(--accent-blue); }

        .stats-row {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(80px, 1fr));
            gap: 8px;
            margin-bottom: 14px;
        }
        .stat-item {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            padding: 8px 6px;
            text-align: center;
            transition: background 0.3s, border-color 0.3s;
        }
        .stat-item .num {
            font-size: 22px;
            font-weight: 700;
            color: var(--accent-blue);
            line-height: 1.2;
        }
        .stat-item .num.green { color: var(--accent-green); }
        .stat-item .num.red { color: var(--accent-red); }
        .stat-item .num.yellow { color: var(--accent-yellow); }
        .stat-item .label {
            font-size: 10px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.3px;
            margin-top: 2px;
        }
        .stat-item.highlight {
            border-color: var(--accent-blue);
            background: var(--stat-bg);
        }
        .stat-item.highlight .num {
            color: var(--accent-blue);
            font-size: 26px;
        }

        .main-grid {
            display: grid;
            grid-template-columns: 1fr 280px;
            gap: 14px;
        }
        @media (max-width: 820px) {
            .main-grid { grid-template-columns: 1fr; }
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 14px;
            margin-bottom: 14px;
            transition: background 0.3s, border-color 0.3s;
        }
        .card-title {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.4px;
            margin-bottom: 10px;
        }

        .form-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
        }
        @media (max-width: 500px) {
            .form-grid { grid-template-columns: 1fr; }
        }
        .form-group {
            display: flex;
            flex-direction: column;
            gap: 2px;
        }
        .form-group label {
            font-size: 11px;
            color: var(--text-secondary);
            font-weight: 500;
        }
        .form-group input, .form-group select, .form-group textarea {
            padding: 6px 8px;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 13px;
            transition: border-color 0.2s, background 0.3s, color 0.3s;
        }
        .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
            outline: none;
            border-color: var(--accent-blue);
            box-shadow: 0 0 0 2px rgba(88,166,255,0.2);
        }
        .form-group textarea { resize: vertical; font-family: monospace; font-size: 12px; }

        #customBox {
            display: none;
            margin-top: 8px;
            padding: 10px;
            background: var(--bg-body);
            border: 1px solid var(--border-color);
            border-radius: 8px;
        }
        #customBox .name-status { font-size: 11px; color: var(--text-secondary); margin-top: 4px; }
        #customBox .name-status .ok { color: var(--accent-green); }
        #customBox .name-status .err { color: var(--accent-red); }

        .actions {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-top: 12px;
        }
        .btn {
            padding: 6px 14px;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            font-size: 13px;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        .btn-primary { background: var(--accent-green); color: #fff; }
        .btn-primary:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-danger { background: var(--accent-red); color: #fff; }
        .btn-danger:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-outline { background: transparent; color: var(--text-secondary); border: 1px solid var(--border-color); }
        .btn-outline:hover { background: var(--hover-bg); color: var(--text-primary); }
        .btn-sm { padding: 2px 8px; font-size: 11px; }

        .log {
            margin-top: 10px;
            padding: 6px 10px;
            background: var(--bg-body);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            font-family: monospace;
            font-size: 12px;
            min-height: 30px;
            color: var(--text-secondary);
        }
        .log .ok { color: var(--accent-green); }
        .log .err { color: var(--accent-red); }
        .log .info { color: var(--accent-blue); }

        .workers-panel {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 14px;
            height: fit-content;
            position: sticky;
            top: 12px;
        }
        .workers-panel .panel-title {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.4px;
            margin-bottom: 8px;
            display: flex;
            justify-content: space-between;
        }
        .workers-panel .panel-title span { color: var(--accent-blue); }
        .workers-scroll {
            max-height: 350px;
            overflow-y: auto;
            padding-right: 2px;
        }
        .workers-scroll::-webkit-scrollbar { width: 3px; }
        .workers-scroll::-webkit-scrollbar-track { background: var(--bg-body); }
        .workers-scroll::-webkit-scrollbar-thumb { background: var(--scrollbar-thumb); border-radius: 4px; }
        .worker-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 6px 8px;
            background: var(--bg-body);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            margin-bottom: 4px;
            font-size: 12px;
            font-family: monospace;
        }
        .worker-item .name { color: var(--accent-blue); }
        .worker-item .cap { color: var(--text-secondary); }
        .worker-item .cap .free { color: var(--accent-green); }

        .table-wrap { overflow-x: auto; }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        th, td {
            padding: 6px 8px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
            vertical-align: middle;
        }
        th {
            color: var(--text-secondary);
            font-weight: 500;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }
        tr:hover td { background: var(--hover-bg); }
        .meeting-code {
            font-weight: 600;
            color: var(--accent-blue);
            font-family: monospace;
            font-size: 13px;
            cursor: pointer;
        }
        .meeting-code:hover { text-decoration: underline; }
        .badge {
            display: inline-block;
            padding: 0 8px;
            border-radius: 12px;
            font-size: 10px;
            font-weight: 500;
            line-height: 18px;
        }
        .badge-indian { background: var(--badge-indian); color: var(--accent-green); }
        .badge-english { background: var(--badge-english); color: var(--accent-blue); }
        .badge-custom { background: var(--badge-custom); color: var(--accent-yellow); }

        .timer-bar {
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .timer-bar .progress {
            flex: 1;
            height: 3px;
            background: var(--border-color);
            border-radius: 4px;
            overflow: hidden;
        }
        .timer-bar .progress .fill {
            height: 100%;
            border-radius: 4px;
            transition: width 1s linear;
        }
        .timer-bar .time-text {
            font-family: monospace;
            font-size: 11px;
            min-width: 32px;
            text-align: right;
            color: var(--text-secondary);
        }
        .timer-bar .time-text.warning { color: var(--accent-yellow); }
        .timer-bar .time-text.danger { color: var(--accent-red); }

        .empty { text-align: center; color: var(--text-secondary); padding: 16px 0; font-size: 12px; }
        .footer-meta {
            margin-top: 8px;
            padding-top: 8px;
            border-top: 1px solid var(--border-color);
            font-size: 11px;
            color: var(--text-secondary);
        }

        /* Live Logs Container */
        .log-container {
            height: 150px;
            overflow-y: auto;
            background: var(--bg-body);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 4px 6px;
            font-family: monospace;
            font-size: 11px;
            margin-top: 8px;
        }
        .log-container .log-entry {
            padding: 1px 0;
            border-bottom: 1px solid var(--border-color);
        }
        .log-container .log-entry .timestamp {
            color: var(--text-muted);
            margin-right: 6px;
        }
        .log-container .log-entry .level-info { color: var(--log-info); }
        .log-container .log-entry .level-success { color: var(--log-success); }
        .log-container .log-entry .level-error { color: var(--log-error); }
        .log-container .log-entry .level-warning { color: var(--log-warning); }

        /* Screenshot Modal */
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }
        .modal-content {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 20px;
            max-width: 90%;
            max-height: 90%;
            position: relative;
        }
        .modal-close {
            position: absolute;
            top: 8px;
            right: 12px;
            font-size: 24px;
            cursor: pointer;
            color: var(--text-secondary);
            background: none;
            border: none;
        }
        .modal-close:hover { color: var(--text-primary); }
        .modal-content img {
            max-width: 100%;
            max-height: 80vh;
            border-radius: 6px;
        }

        @media (max-width: 600px) {
            body { padding: 8px; }
            .header h1 { font-size: 17px; }
            .header-actions { gap: 4px; }
            .status-badge { font-size: 10px; padding: 0 8px; }
            .theme-toggle, .mode-switch { font-size: 15px; padding: 0 6px; height: 26px; }
            .mode-switch input[type="checkbox"] { width: 24px; height: 14px; }
            .mode-switch input[type="checkbox"]::after { width: 10px; height: 10px; }
            .stats-row { grid-template-columns: repeat(3, 1fr); gap: 4px; }
            .stat-item .num { font-size: 18px; }
            .stat-item.highlight .num { font-size: 20px; }
            .stat-item { padding: 4px 2px; }
            .stat-item .label { font-size: 8px; }
            .card { padding: 10px; }
            .form-group input, .form-group select, .form-group textarea { font-size: 16px; padding: 8px; }
            .btn { font-size: 14px; padding: 8px 16px; }
            .workers-panel { position: static; }
            table { font-size: 11px; }
            th, td { padding: 4px 6px; }
            .meeting-code { font-size: 11px; }
            .timer-bar .time-text { font-size: 10px; }
            .log-container { height: 100px; }
        }
    </style>
</head>
<body>
<div class="container">
    <!-- HEADER -->
    <div class="header">
        <h1>🚀 Junaid <span>Members Panel (Zoom)</span></h1>
        <div class="header-actions">
            <div class="status-badge">
                <span class="dot"></span>
                <span id="statusText">Connected</span>
                <span style="color:var(--text-muted);margin-left:4px">|</span>
                <span id="liveTime" style="font-family:monospace;font-size:11px"></span>
            </div>
            <div class="mode-switch" title="Toggle join mode">
                <label>
                    <span class="mode-label" id="modeLabel">Individual</span>
                    <input type="checkbox" id="modeToggle" />
                    <span class="mode-label" id="modeLabel2">Together</span>
                </label>
            </div>
            <button class="theme-toggle" id="themeToggle" aria-label="Toggle theme">🌙</button>
        </div>
    </div>

    <!-- STATS ROW -->
    <div class="stats-row">
        <div class="stat-item highlight">
            <div class="num" id="totalCap">0</div>
            <div class="label">Total Capacity</div>
        </div>
        <div class="stat-item">
            <div class="num green" id="freeCap">0</div>
            <div class="label">Free Capacity</div>
        </div>
        <div class="stat-item">
            <div class="num" id="workersN">0</div>
            <div class="label">Workers</div>
        </div>
        <div class="stat-item">
            <div class="num" id="tasksN">0</div>
            <div class="label">Active Tasks</div>
        </div>
        <div class="stat-item">
            <div class="num" id="botsN">0</div>
            <div class="label">Running Bots</div>
        </div>
    </div>

    <!-- MAIN GRID -->
    <div class="main-grid">
        <!-- LEFT COLUMN -->
        <div>
            <!-- START BOTS CARD -->
            <div class="card">
                <div class="card-title">📌 Start New Meeting</div>
                <div class="form-grid">
                    <div class="form-group"><label>Meeting ID</label><input id="meetingId" placeholder="5415403058"/></div>
                    <div class="form-group"><label>Passcode</label><input id="passcode" placeholder="optional"/></div>
                    <div class="form-group"><label>Bots</label><input type="number" id="botCount" value="10" min="1" max="500" oninput="updCount()"/></div>
                    <div class="form-group"><label>Duration (min)</label><input type="number" id="duration" value="120" min="1"/></div>
                    <div class="form-group" style="grid-column:1/-1">
                        <label>Name Type</label>
                        <select id="nameType" onchange="toggleCustom()">
                            <option value="indian">🇮🇳 Indian (Natural)</option>
                            <option value="english">🇺🇸 English</option>
                            <option value="custom">✏️ Custom Names</option>
                        </select>
                    </div>
                </div>
                <div id="customBox">
                    <label style="font-size:11px;color:var(--text-secondary)">Custom names (one per line)</label>
                    <textarea id="customNames" rows="3" placeholder="Rahul Sharma&#10;Arjun Singh&#10;Priya Patel"></textarea>
                    <div class="name-status">
                        Names: <strong id="nameCount">0</strong> &nbsp;|&nbsp; Need: <strong id="needCount">10</strong>
                        <span id="nameStatus"></span>
                    </div>
                </div>
                <div class="actions">
                    <button class="btn btn-primary" onclick="startBots()">🚀 Start Bots</button>
                    <button class="btn btn-outline" onclick="refresh()">🔄 Refresh</button>
                </div>
                <div id="msg" class="log">✅ Ready</div>
            </div>

            <!-- ACTIVE MEETINGS -->
            <div class="card">
                <div class="card-title" style="display:flex;justify-content:space-between">
                    <span>📋 Active Meetings</span>
                    <span id="taskCount" style="color:var(--text-secondary);font-weight:400;text-transform:none">0 running</span>
                </div>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Task</th><th>Meeting</th><th>Bots</th><th>Type</th><th>Mode</th><th>Time Left</th><th style="text-align:center">Action</th>
                            </tr>
                        </thead>
                        <tbody id="tbody"><tr><td colspan="7" class="empty">No active meetings</td></tr></tbody>
                    </table>
                </div>
            </div>

            <!-- LIVE LOGS -->
            <div class="card">
                <div class="card-title">📝 Live Logs</div>
                <div id="log-container" class="log-container">
                    <div class="empty">Waiting for logs...</div>
                </div>
            </div>
        </div>

        <!-- RIGHT PANEL – WORKERS -->
        <div class="workers-panel">
            <div class="panel-title"><span>🖥️ Connected Workers</span><span id="workerCount">0</span></div>
            <div class="workers-scroll" id="wlist"><div class="empty" style="padding:20px 0">No workers connected</div></div>
            <div class="footer-meta">
                Total: <strong id="totalCapFooter">0</strong> &nbsp;|&nbsp; Free: <strong id="freeCapFooter">0</strong>
            </div>
        </div>
    </div>
</div>

<!-- SCREENSHOT MODAL -->
<div id="screenshot-modal" class="modal" onclick="if(event.target===this) closeModal();">
    <div class="modal-content">
        <button class="modal-close" onclick="closeModal()">&times;</button>
        <img id="screenshot-img" src="" alt="Screenshot" />
    </div>
</div>

<!-- SOCKET.IO CLIENT -->
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<script>
// ===== CONFIG & DOM =====
const API = location.origin;
const $ = id => document.getElementById(id);
const meetingId = $('meetingId');
const passcode = $('passcode');
const botCount = $('botCount');
const duration = $('duration');
const nameType = $('nameType');
const customNames = $('customNames');
const customBox = $('customBox');
const msg = $('msg');
const tbody = $('tbody');
const wlist = $('wlist');
const totalCap = $('totalCap');
const freeCap = $('freeCap');
const workersN = $('workersN');
const tasksN = $('tasksN');
const botsN = $('botsN');
const totalCapFooter = $('totalCapFooter');
const freeCapFooter = $('freeCapFooter');
const workerCount = $('workerCount');
const taskCount = $('taskCount');
const statusText = $('statusText');
const liveTime = $('liveTime');
const themeToggle = $('themeToggle');
const modeToggle = $('modeToggle');
const modeLabel = $('modeLabel');
const modeLabel2 = $('modeLabel2');
const logContainer = $('log-container');
const screenshotImg = $('screenshot-img');

// ===== SOCKET.IO =====
const sio = io(API);
sio.on('connect', () => {
    console.log('Connected to server');
    statusText.textContent = 'Connected';
});
sio.on('disconnect', () => {
    statusText.textContent = 'Offline';
});
sio.on('new_log', (data) => {
    // Append log entry
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    const ts = data.timestamp ? data.timestamp.slice(11,19) : '';
    const level = data.level || 'info';
    const bot = data.bot_tag || '';
    const msg = data.message || '';
    entry.innerHTML = `<span class="timestamp">${ts}</span> <span class="level-${level}">[${level}]</span> <strong>${bot}</strong> ${msg}`;
    logContainer.appendChild(entry);
    logContainer.scrollTop = logContainer.scrollHeight;
    // Limit entries to 200 to avoid memory issues
    while (logContainer.children.length > 200) {
        logContainer.removeChild(logContainer.firstChild);
    }
});
sio.on('screenshot_response', (data) => {
    if (data.error) {
        alert('Screenshot error: ' + data.error);
        return;
    }
    if (data.image) {
        screenshotImg.src = 'data:image/png;base64,' + data.image;
        $('screenshot-modal').style.display = 'flex';
    }
});

// ===== THEME =====
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

// ===== MODE =====
function getMode(){ return localStorage.getItem('junaid_mode') || 'individual'; }
function setMode(mode){
    localStorage.setItem('junaid_mode', mode);
    const checked = mode === 'together';
    modeToggle.checked = checked;
    modeLabel.style.color = checked ? '' : 'var(--accent-blue)';
    modeLabel2.style.color = checked ? 'var(--accent-blue)' : '';
}
const savedMode = getMode();
setMode(savedMode);
modeToggle.addEventListener('change', ()=>{
    const mode = modeToggle.checked ? 'together' : 'individual';
    setMode(mode);
});

// ===== HELPERS =====
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
    st.innerHTML = names.length >= bots ? ' <span class="ok">✅ Enough</span>' : ` <span class="err">❌ Need ${bots - names.length} more</span>`;
}
customNames.addEventListener('input', updCount);

function closeModal() {
    $('screenshot-modal').style.display = 'none';
}

function requestScreenshot(taskId) {
    sio.emit('request_screenshot', { task_id: taskId });
}

// ===== LIVE CLOCK =====
function updateClock(){
    liveTime.textContent = new Date().toLocaleTimeString();
}
setInterval(updateClock, 1000);
updateClock();

// ===== REFRESH =====
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
        const workerCountVal = Object.keys(workers).length;
        const taskCountVal = Object.keys(tasks).length;
        let runningBots = 0;
        Object.values(tasks).forEach(t=>{ runningBots += t.bot_count || 0; });

        totalCap.textContent = total;
        freeCap.textContent = free;
        workersN.textContent = workerCountVal;
        tasksN.textContent = taskCountVal;
        botsN.textContent = runningBots;
        totalCapFooter.textContent = total;
        freeCapFooter.textContent = free;
        workerCount.textContent = workerCountVal;
        taskCount.textContent = taskCountVal + ' running';

        // Workers list
        const wKeys = Object.keys(workers);
        if(!wKeys.length){
            wlist.innerHTML = '<div class="empty">No workers connected</div>';
        } else {
            wlist.innerHTML = wKeys.map(id => {
                const w = workers[id];
                const free = w.free_capacity ?? 0;
                const max = w.max_capacity ?? 0;
                const pct = max > 0 ? Math.round((free/max)*100) : 0;
                return `<div class="worker-item">
                    <span class="name">🟢 ${id}</span>
                    <span class="cap">${free}/${max} <span class="free">(${pct}%)</span></span>
                </div>`;
            }).join('');
        }

        // Tasks table
        const tKeys = Object.keys(tasks);
        if(!tKeys.length){
            tbody.innerHTML = '<tr><td colspan="7" class="empty">No active meetings</td></tr>';
        } else {
            tbody.innerHTML = tKeys.map(tid => {
                const t = tasks[tid];
                const meeting = t.meeting_code || 'N/A';
                const bots = t.bot_count || 0;
                const type = t.name_type || 'indian';
                const mode = t.join_mode || 'individual';
                const remaining = t.remaining_minutes !== undefined ? t.remaining_minutes : t.duration_minutes || 120;
                const totalDur = t.duration_minutes || 120;
                const pct = totalDur > 0 ? ((totalDur - Math.max(0, remaining)) / totalDur * 100) : 0;
                const pctClamped = Math.min(100, Math.max(0, pct));
                const warn = remaining < 5 ? 'danger' : remaining < 15 ? 'warning' : '';
                const typeBadge = type === 'indian' ? 'indian' : type === 'english' ? 'english' : 'custom';
                const modeIcon = mode === 'together' ? '👥' : '🚶';
                return `<tr>
                    <td style="font-family:monospace;font-size:11px;color:var(--text-secondary)">${tid}</td>
                    <td class="meeting-code" onclick="requestScreenshot('${tid}')">${meeting}</td>
                    <td>${bots}</td>
                    <td><span class="badge badge-${typeBadge}">${type}</span></td>
                    <td>${modeIcon}</td>
                    <td>
                        <div class="timer-bar">
                            <div class="progress">
                                <div class="fill" style="width:${pctClamped}%;background:${remaining < 5 ? 'var(--accent-red)' : remaining < 15 ? 'var(--accent-yellow)' : 'var(--accent-green)'}"></div>
                            </div>
                            <span class="time-text ${warn}">${remaining > 0 ? Math.ceil(remaining)+'m' : '0m'}</span>
                        </div>
                    </td>
                    <td style="text-align:center">
                        <button class="btn btn-danger btn-sm" onclick="killTask('${tid}')">✕ Kill</button>
                    </td>
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

// ===== START BOTS =====
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
            show(d.message || 'Started successfully!', 'ok');
            setTimeout(refresh, 1000);
        } else {
            show(d.detail || 'Failed to start', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

// ===== KILL TASK =====
async function killTask(taskId){
    if(!confirm(`Kill task ${taskId}?`)) return;
    try{
        const r = await fetch(API+'/api/terminate', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({task_id: taskId})
        });
        const d = await r.json();
        if(r.ok){
            show(`✅ Kill sent for task ${taskId}`, 'ok');
            setTimeout(refresh, 1000);
        } else {
            show(d.detail || 'Kill failed', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

// ===== AUTO REFRESH + INIT =====
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
