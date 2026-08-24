# ============================================
# ZOOM BOT CENTRAL – FINAL (Stable Counts + Completed Status)
# ============================================
import os
import uuid
import asyncio
import json
import signal
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
import socketio

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", logger=False, engineio_logger=False)
app = FastAPI(title="Zoom Bot Central")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

workers = {}
running_tasks = {}
meeting_groups = {}
scheduled_tasks = {}
session_status = {"logged_in": False, "last_checked": None, "message": "No session file", "login_in_progress": False}

class StartBotRequest(BaseModel):
    meeting_code: str
    passcode: str = ""
    bot_count: int = 10
    duration_minutes: int = 120
    name_type: str = "indian"
    custom_names: Optional[List[str]] = None
    join_mode: str = "individual"

class ScheduleRequest(BaseModel):
    meeting_code: str
    passcode: str = ""
    bot_count: int = 10
    duration_minutes: int = 120
    name_type: str = "indian"
    custom_names: Optional[List[str]] = None
    join_mode: str = "individual"
    schedule_at: str

class TerminateRequest(BaseModel):
    meeting_code: Optional[str] = None
    task_id: Optional[str] = None

@sio.event
async def connect(sid, environ):
    print(f"[SIO] Connected: {sid}")

@sio.event
async def disconnect(sid):
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            tasks_to_remove = [tid for tid, t in running_tasks.items() if t.get("worker_id") == wid]
            for tid in tasks_to_remove:
                task = running_tasks[tid]
                meeting = task.get("meeting_code")
                bot_count = task.get("bot_count", 0)
                if meeting and meeting in meeting_groups:
                    g = meeting_groups[meeting]
                    if tid in g.get("task_ids", []):
                        g["task_ids"].remove(tid)
                    g["completed_bots"] = g.get("completed_bots", 0) + bot_count
                    if not g["task_ids"]:
                        g["status"] = "completed"
                del running_tasks[tid]
            workers[wid]["free_capacity"] = workers[wid]["max_capacity"]
            workers[wid]["sid"] = None
            workers[wid]["last_seen"] = now_ist().isoformat()
            print(f"[SIO] Worker {wid} disconnected")
            break

@sio.event
async def register_worker(sid, data):
    wid = data.get("worker_id", f"worker-{sid[:6]}")
    max_cap = int(data.get("max_capacity", 10))
    now = now_ist().isoformat()
    if wid in workers:
        workers[wid]["sid"] = sid
        workers[wid]["max_capacity"] = max_cap
        workers[wid]["last_seen"] = now
    else:
        workers[wid] = {"sid": sid, "max_capacity": max_cap, "free_capacity": max_cap, "last_seen": now}
    print(f"[SIO] Worker {wid} registered | capacity={max_cap}")
    await sio.emit("registered", {"worker_id": wid, "max_capacity": max_cap}, to=sid)

@sio.event
async def task_completed(sid, data):
    tid = data.get("task_id")
    if not tid or tid not in running_tasks:
        return
    task = running_tasks[tid]
    wid = task.get("worker_id")
    bot_count = task.get("bot_count", 0)
    meeting = task.get("meeting_code")

    if wid and wid in workers:
        workers[wid]["free_capacity"] = min(
            workers[wid]["max_capacity"],
            workers[wid].get("free_capacity", 0) + bot_count
        )

    if meeting and meeting in meeting_groups:
        g = meeting_groups[meeting]
        if tid in g.get("task_ids", []):
            g["task_ids"].remove(tid)
        g["completed_bots"] = g.get("completed_bots", 0) + bot_count
        if not g["task_ids"]:
            g["status"] = "completed"
            print(f"[SIO] Meeting {meeting} → COMPLETED")

    del running_tasks[tid]
    print(f"[SIO] Task {tid} completed | capacity freed")

@app.get("/health")
async def health():
    return {"ok": True, "time": now_ist().isoformat()}

@app.get("/session")
async def get_session():
    if not os.path.exists("zoom_session.json"):
        raise HTTPException(404, "Session not found")
    return FileResponse("zoom_session.json", media_type="application/json")

@app.get("/api/session-status")
async def api_session_status():
    if os.path.exists("zoom_session.json"):
        session_status["logged_in"] = True
        if "updated" not in (session_status.get("message") or "").lower():
            session_status["message"] = "Session file present"
    else:
        session_status["logged_in"] = False
        session_status["message"] = "No session file found"
    session_status["last_checked"] = now_ist().isoformat()
    return session_status

@app.post("/api/update-session")
async def update_session(request: Request):
    try:
        data = await request.json()
        if not isinstance(data, dict) or "cookies" not in data:
            raise HTTPException(400, "Invalid session JSON. Must contain 'cookies'")
        with open("zoom_session.json", "w") as f:
            json.dump(data, f, indent=2)
        session_status.update({
            "logged_in": True,
            "last_checked": now_ist().isoformat(),
            "message": "Session updated successfully ✓",
            "login_in_progress": False
        })
        print("✅ Session JSON updated")
        return {"success": True, "message": "Session saved successfully"}
    except Exception as e:
        raise HTTPException(400, f"Failed to save session: {str(e)}")

@app.get("/status")
@app.get("/api/status")
async def status():
    connected_workers = {wid: info for wid, info in workers.items() if info.get("sid") is not None}
    total_free = sum(w.get("free_capacity", 0) for w in connected_workers.values())
    total_capacity = sum(w.get("max_capacity", 0) for w in connected_workers.values())

    meetings = {}
    for meeting, info in meeting_groups.items():
        meetings[meeting] = {
            "meeting_code": meeting,
            "total_bots": info.get("total_bots", 0),
            "completed_bots": info.get("completed_bots", 0),
            "name_type": info.get("name_type", "indian"),
            "started_at": info.get("started_at"),
            "join_mode": info.get("join_mode", "individual"),
            "status": info.get("status", "running")
        }

    return {
        "workers": connected_workers,
        "total_capacity": total_capacity,
        "total_free_capacity": total_free,
        "meetings": meetings,
        "schedules": scheduled_tasks,
        "session": session_status,
        "timestamp": now_ist().isoformat(),
        "connected_workers_count": len(connected_workers)
    }

@app.post("/api/start-bots")
async def start_bots(req: StartBotRequest):
    if not os.path.exists("zoom_session.json"):
        raise HTTPException(400, "No session file. Please upload zoom_session.json first.")
    if req.bot_count < 1:
        raise HTTPException(400, "bot_count must be >= 1")

    meeting = req.meeting_code.strip().replace(" ", "")
    if not meeting:
        raise HTTPException(400, "meeting_code required")

    remaining = req.bot_count
    assigned = []
    connected = {wid: info for wid, info in workers.items() if info.get("sid")}
    sorted_workers = sorted(connected.items(), key=lambda x: x[1].get("free_capacity", 0), reverse=True)
    name_offset = 0

    for wid, info in sorted_workers:
        if remaining <= 0:
            break
        free = int(info.get("free_capacity", 0))
        if free <= 0:
            continue

        give = min(free, remaining)
        task_id = str(uuid.uuid4())[:8]

        custom_slice = None
        if req.custom_names and req.name_type == "custom":
            custom_slice = req.custom_names[name_offset: name_offset + give]
            name_offset += give

        payload = {
            "task_id": task_id,
            "meeting_code": meeting,
            "passcode": req.passcode or "",
            "bot_count": give,
            "duration_minutes": req.duration_minutes,
            "name_type": req.name_type or "indian",
            "custom_names": custom_slice,
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
            "started_at": now_ist().isoformat(),
            "join_mode": req.join_mode or "individual"
        }

        if meeting not in meeting_groups:
            meeting_groups[meeting] = {
                "task_ids": [],
                "total_bots": 0,
                "completed_bots": 0,
                "name_type": payload["name_type"],
                "join_mode": req.join_mode or "individual",
                "started_at": now_ist().isoformat(),
                "status": "running"
            }

        meeting_groups[meeting]["task_ids"].append(task_id)
        meeting_groups[meeting]["total_bots"] += give
        meeting_groups[meeting]["status"] = "running"

        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give, "task_id": task_id})
        remaining -= give

    if not assigned:
        raise HTTPException(503, "No free capacity or no connected workers.")

    return {
        "success": True,
        "message": f"Started {req.bot_count - remaining} bots for {meeting}",
        "assigned": assigned,
        "remaining_unassigned": remaining
    }

@app.post("/api/schedule")
async def create_schedule(req: ScheduleRequest):
    try:
        schedule_time = datetime.fromisoformat(req.schedule_at.replace("Z", "+00:00"))
        if schedule_time.tzinfo is None:
            schedule_time = schedule_time.replace(tzinfo=IST)
        else:
            schedule_time = schedule_time.astimezone(IST)
    except Exception as e:
        raise HTTPException(400, f"Invalid schedule_at: {e}")
    if schedule_time <= now_ist():
        raise HTTPException(400, "Schedule time must be in future")

    sid = str(uuid.uuid4())[:8]
    scheduled_tasks[sid] = {
        "schedule_id": sid,
        "meeting_code": req.meeting_code.strip().replace(" ", ""),
        "passcode": req.passcode or "",
        "bot_count": req.bot_count,
        "duration_minutes": req.duration_minutes,
        "name_type": req.name_type or "indian",
        "custom_names": req.custom_names,
        "join_mode": req.join_mode or "individual",
        "schedule_at": schedule_time.isoformat(),
        "created_at": now_ist().isoformat()
    }
    return {"success": True, "schedule_id": sid, "message": "Scheduled successfully"}

@app.delete("/api/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    if schedule_id in scheduled_tasks:
        del scheduled_tasks[schedule_id]
        return {"success": True}
    raise HTTPException(404, "Schedule not found")

@app.post("/api/terminate")
async def terminate(req: Optional[TerminateRequest] = None):
    if req and req.meeting_code:
        meeting = req.meeting_code.strip().replace(" ", "")
        to_kill = [tid for tid, t in running_tasks.items() if t.get("meeting_code") == meeting]
        for tid in to_kill:
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
            del running_tasks[tid]
        if meeting in meeting_groups:
            del meeting_groups[meeting]
        return {"success": True, "message": f"Meeting {meeting} terminated"}
    else:
        for tid in list(running_tasks.keys()):
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
        running_tasks.clear()
        meeting_groups.clear()
        return {"success": True, "message": "All tasks terminated"}

@app.post("/api/shutdown")
async def shutdown_server():
    print("🛑 SHUTDOWN requested")
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("shutdown", {"message": "Server shutting down"}, to=info["sid"])
    await asyncio.sleep(1.5)
    os.kill(os.getpid(), signal.SIGTERM)
    return {"success": True, "message": "Shutdown signal sent"}

async def schedule_checker():
    while True:
        await asyncio.sleep(4)
        now = now_ist()
        to_run = []
        for sid, info in list(scheduled_tasks.items()):
            try:
                st = datetime.fromisoformat(info["schedule_at"])
                if st.tzinfo is None:
                    st = st.replace(tzinfo=IST)
                if now >= st:
                    to_run.append(sid)
            except:
                continue
        for sid in to_run:
            info = scheduled_tasks.pop(sid)
            req = StartBotRequest(
                meeting_code=info["meeting_code"],
                passcode=info["passcode"],
                bot_count=info["bot_count"],
                duration_minutes=info["duration_minutes"],
                name_type=info["name_type"],
                custom_names=info["custom_names"],
                join_mode=info["join_mode"]
            )
            try:
                await start_bots(req)
            except Exception as e:
                print(f"[SCHEDULE] Failed: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_checker())
    if os.path.exists("zoom_session.json"):
        session_status.update({
            "logged_in": True,
            "message": "Session file present",
            "last_checked": now_ist().isoformat()
        })
    print("✅ Server started")

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"/>
<title>Zoom Command Center</title>
<style>
:root{--bg:#0b0e13;--card:#141a22;--border:#243044;--primary:#3b82f6;--danger:#ef4444;--warning:#f59e0b;--success:#10b981;--text:#e2e8f0;--muted:#94a3b8}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;color:var(--text);min-height:100vh;padding:12px;padding-bottom:40px}
.container{max-width:1400px;margin:0 auto}
.header{display:flex;justify-content:space-between;align-items:center;background:#111827;border:1px solid var(--border);border-radius:14px;padding:12px 16px;margin-bottom:16px;gap:10px;flex-wrap:wrap}
.header h1{font-size:18px;font-weight:700;color:#93c5fd}
.header-right{display:flex;align-items:center;gap:10px;font-size:13px;flex-wrap:wrap}
.usage{background:#0f172a;border:1px solid var(--border);padding:5px 12px;border-radius:20px;color:var(--muted)}
.usage strong{color:var(--warning)}
.session-badge{padding:5px 12px;border-radius:20px;font-size:12px;font-weight:600}
.session-badge.logged-in{background:#064e3b;color:#34d399}
.session-badge.logged-out{background:#7f1d1d;color:#fca5a5}
.grid{display:grid;grid-template-columns:1fr;gap:16px}
@media(min-width:1000px){.grid{grid-template-columns:380px 1fr}}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px}
.section-title{font-size:14px;font-weight:600;color:#93c5fd;margin-bottom:14px}
.form-group{margin-bottom:12px}
.form-group label{display:block;font-size:12px;color:var(--muted);margin-bottom:5px}
.form-group input,.form-group select,.form-group textarea{width:100%;padding:11px 13px;background:#0f172a;border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:14px;outline:none}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.mode-toggle{display:flex;background:#0f172a;border-radius:12px;padding:4px;border:1px solid var(--border);margin-bottom:14px}
.mode-btn{flex:1;padding:11px 0;text-align:center;border-radius:9px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer}
.mode-btn.active{background:var(--primary);color:white}
.schedule-box{background:#0f172a;border:1px solid var(--border);border-radius:12px;padding:12px;margin-bottom:14px}
.schedule-check{display:flex;align-items:center;gap:10px;font-size:14px;font-weight:500}
.schedule-check input{width:18px;height:18px;accent-color:var(--primary)}
.schedule-fields{display:none;margin-top:12px;gap:10px}
.schedule-fields.show{display:grid;grid-template-columns:1fr 1fr}
.btn-row{display:flex;gap:10px;margin-top:4px;flex-wrap:wrap}
.btn{padding:12px 16px;border:none;border-radius:10px;font-weight:600;font-size:14px;cursor:pointer}
.btn-primary{background:var(--primary);color:white;flex:1}
.btn-danger{background:#7f1d1d;color:#fecaca}
.btn-success{background:#065f46;color:#6ee7b7}
.btn-sm{padding:6px 11px;font-size:12px;border-radius:8px}
.btn-outline{background:transparent;border:1px solid var(--border);color:var(--muted);padding:6px 11px;font-size:13px}
.mobile-only{display:block}.desktop-only{display:none}
.meeting-card{background:#0f172a;border:1px solid var(--border);border-radius:12px;padding:14px;margin-bottom:10px}
.meeting-card.highlight{border-color:var(--primary);background:#1e3a5f}
.mc-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px}
.mc-id{font-weight:700;font-size:15px;color:#93c5fd;word-break:break-all}
.mc-bots{font-size:18px;font-weight:700;color:var(--warning)}
.mc-meta{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}
.mc-bottom{display:flex;justify-content:space-between;align-items:center;font-size:12px;color:var(--muted)}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:11px 9px;color:var(--muted);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid var(--border)}
td{padding:11px 9px;border-bottom:1px solid #1e293b;vertical-align:middle}
tr:hover td{background:#1e293b}
tr.highlight td{background:#1e3a5f!important;border-left:3px solid var(--primary)}
.badge{display:inline-block;padding:3px 9px;border-radius:999px;font-size:11px;font-weight:600;white-space:nowrap}
.badge-slow{background:#422006;color:#fbbf24}.badge-together{background:#064e3b;color:#34d399}
.badge-indian{background:#1e3a5f;color:#93c5fd}.badge-english{background:#064e3b;color:#6ee7b7}.badge-custom{background:#4c1d95;color:#c4b5fd}
.badge-running{background:#064e3b;color:#34d399}.badge-completed{background:#334155;color:#94a3b8}
.countdown{font-family:ui-monospace,monospace;color:var(--warning);font-weight:600;font-size:12px}
.search-row{display:flex;gap:8px;margin-bottom:12px}
.search-row input{flex:1;padding:10px 13px;background:#0f172a;border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:14px;min-width:0}
.log{margin-top:12px;padding:10px 12px;background:#0f172a;border:1px solid var(--border);border-radius:10px;font-size:12px;color:var(--muted);font-family:ui-monospace,monospace;word-break:break-word}
.log .ok{color:var(--success)}.log .err{color:var(--danger)}.log .info{color:var(--primary)}
.empty{text-align:center;color:var(--muted);padding:22px 10px;font-size:14px}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;align-items:center;justify-content:center;z-index:9999;padding:16px}
.modal{background:#1e293b;border:1px solid var(--border);border-radius:14px;padding:24px;max-width:400px;width:100%}
.modal h3{margin-bottom:12px;color:#fca5a5}
.modal input{width:100%;padding:12px;margin:12px 0;background:#0f172a;border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:15px}
@media(min-width:768px){.mobile-only{display:none}.desktop-only{display:block}.header h1{font-size:20px}}
@media(max-width:767px){.form-row{grid-template-columns:1fr}.schedule-fields.show{grid-template-columns:1fr}.btn{padding:13px 14px;font-size:15px}}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>⚡ Zoom Command Center</h1>
    <div class="header-right">
      <div id="sessionBadge" class="session-badge logged-out">Checking...</div>
      <div class="usage"><strong id="totalCap">0</strong>/<strong id="totalCapMax">0</strong></div>
      <span id="liveTime" style="color:var(--muted)"></span>
      <button class="btn btn-outline" onclick="refresh()">↻ Refresh</button>
      <button class="btn btn-danger btn-sm" onclick="openShutdownModal()">🛑 Shutdown</button>
    </div>
  </div>

  <div class="card" style="margin-bottom:16px;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
      <div class="section-title" style="margin:0;">🔑 Zoom Session</div>
      <button class="btn btn-outline btn-sm" id="toggleSessionBtn" onclick="toggleSessionBox()">✏️ Update Session</button>
    </div>
    <div id="sessionStatusLine" style="font-size:13px;color:var(--muted);">Status: <span id="sessionStatusText">Checking...</span></div>
    <div id="sessionEditBox" style="display:none;margin-top:12px;">
      <div style="font-size:12px;color:var(--muted);margin-bottom:8px;">Locally login karke <b>zoom_session.json</b> ka poora content yahan paste karo</div>
      <textarea id="sessionJson" rows="8" placeholder='{"cookies":[...],"origins":[...]}' style="width:100%;padding:12px;background:#0f172a;border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:12px;font-family:monospace;resize:vertical;"></textarea>
      <div style="display:flex;gap:10px;margin-top:10px;">
        <button class="btn btn-success" onclick="saveSession()" style="flex:1;">💾 Save Session</button>
        <button class="btn btn-outline" onclick="cancelSessionEdit()">Cancel</button>
      </div>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="section-title">🚀 Launch / Schedule</div>
      <div class="mode-toggle">
        <div class="mode-btn active" id="modeSlow" onclick="setMode('individual')">🐢 Slow</div>
        <div class="mode-btn" id="modeTogether" onclick="setMode('together')">⚡ Together</div>
      </div>
      <div class="form-group"><label>Meeting ID</label><input id="meetingId" placeholder="98695209590" inputmode="numeric"/></div>
      <div class="form-group"><label>Passcode (optional)</label><input id="passcode" placeholder="Leave blank if none"/></div>
      <div class="form-row">
        <div class="form-group"><label>Bots</label><input type="number" id="botCount" value="20" min="1" max="500" oninput="updCount()"/></div>
        <div class="form-group"><label>Name Type</label>
          <select id="nameType" onchange="toggleCustom()">
            <option value="indian">🇮🇳 Indian</option>
            <option value="english">🇺🇸 English</option>
            <option value="custom">✏️ Custom</option>
          </select>
        </div>
      </div>
      <div id="customBox" style="display:none;margin-top:12px;padding:14px;background:#0f172a;border:1px solid var(--border);border-radius:12px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
          <label style="font-size:13px;font-weight:600;color:#93c5fd;">✏️ Custom Names</label>
          <span style="font-size:11px;color:var(--muted);">One name per line</span>
        </div>
        <textarea id="customNames" rows="6" placeholder="Rahul Sharma&#10;Priya Verma&#10;Aarav Singh&#10;..." style="width:100%;padding:12px;background:#111827;border:1px solid #334155;border-radius:10px;color:var(--text);font-size:13px;font-family:ui-monospace,monospace;resize:vertical;line-height:1.5;"></textarea>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-top:10px;font-size:12px;">
          <div style="color:var(--muted);">
            Names: <strong id="nameCount" style="color:#e2e8f0;">0</strong> &nbsp;|&nbsp; Need: <strong id="needCount" style="color:#e2e8f0;">20</strong>
            <span id="nameStatus"></span>
          </div>
          <button class="btn btn-outline btn-sm" onclick="document.getElementById('customNames').value='';updCount();">Clear</button>
        </div>
      </div>
      <div class="form-group"><label>Duration (minutes)</label><input type="number" id="duration" value="120" min="1"/></div>
      <div class="schedule-box">
        <label class="schedule-check"><input type="checkbox" id="enableSchedule" onchange="toggleSchedule()"/> Enable Scheduling</label>
        <div class="schedule-fields" id="scheduleFields">
          <div class="form-group" style="margin:0"><label>Date</label><input type="date" id="scheduleDate"/></div>
          <div class="form-group" style="margin:0"><label>Time (IST)</label><input type="time" id="scheduleTime"/></div>
        </div>
      </div>
      <div class="btn-row">
        <button class="btn btn-primary" id="startBtn" onclick="handleStart()">▶ Start Now</button>
        <button class="btn btn-danger" onclick="killAll()">Kill All</button>
      </div>
      <div id="msg" class="log">Ready • IST</div>
    </div>

    <div style="display:flex;flex-direction:column;gap:16px;">
      <div class="card">
        <div class="section-title">🟢 Meetings</div>
        <div class="search-row">
          <input id="searchMeeting" placeholder="Search Meeting ID" oninput="filterMeetings()"/>
          <button class="btn btn-danger btn-sm" onclick="killBySearch()">Kill</button>
        </div>
        <div id="activeListMobile" class="mobile-only"><div class="empty">No meetings</div></div>
        <div class="desktop-only table-wrap">
          <table>
            <thead><tr><th>#</th><th>Meeting</th><th>Bots</th><th>Status</th><th>Started</th><th>Mode</th><th></th></tr></thead>
            <tbody id="tbodyActive"><tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No meetings</td></tr></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="section-title">📅 Scheduled</div>
        <div id="scheduleListMobile" class="mobile-only"><div class="empty">No scheduled meetings</div></div>
        <div class="desktop-only table-wrap">
          <table>
            <thead><tr><th>#</th><th>Meeting</th><th>Bots</th><th>When</th><th>Countdown</th><th>Mode</th><th></th></tr></thead>
            <tbody id="tbodySchedule"><tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No scheduled meetings</td></tr></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="modal-overlay" id="shutdownModal">
  <div class="modal">
    <h3>🛑 Shutdown Server?</h3>
    <p style="font-size:13px;color:var(--muted);margin-bottom:8px;">All workers will close. Type <b>yes</b> to confirm.</p>
    <input type="text" id="shutdownConfirm" placeholder="Type yes"/>
    <div style="display:flex;gap:10px;margin-top:8px;">
      <button class="btn btn-danger" style="flex:1" onclick="confirmShutdown()">Shutdown</button>
      <button class="btn btn-outline" onclick="closeShutdownModal()">Cancel</button>
    </div>
  </div>
</div>

<script>
const API=location.origin;const $=id=>document.getElementById(id);
let currentMode='individual',allMeetings={},allSchedules={},isLoggedIn=false;

function setMode(m){currentMode=m;$('modeSlow').classList.toggle('active',m==='individual');$('modeTogether').classList.toggle('active',m==='together')}
function toggleSchedule(){const e=$('enableSchedule').checked;$('scheduleFields').classList.toggle('show',e);$('startBtn').textContent=e?'📅 Schedule':'▶ Start Now'}
function show(m,t='info'){const c=t==='ok'?'ok':t==='err'?'err':'info';msg.innerHTML=`<span class="${c}">[${new Date().toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'})}] ${m}</span>`}
function toggleCustom(){customBox.style.display=nameType.value==='custom'?'block':'none';updCount()}
function updCount(){const b=parseInt(botCount.value)||0;const n=customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);$('nameCount').textContent=n.length;$('needCount').textContent=b;const st=$('nameStatus');if(nameType.value!=='custom'){st.innerHTML='';return}st.innerHTML=n.length>=b?' <span style="color:#10b981">✅ Ready</span>':` <span style="color:#ef4444">❌ ${b-n.length} more</span>`}
customNames.addEventListener('input',updCount);
function updateClock(){liveTime.textContent=new Date().toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'})+' IST'}
setInterval(updateClock,1000);updateClock();
function formatCountdown(iso){try{const t=new Date(iso),n=new Date();let d=Math.floor((t-n)/1000);if(d<=0)return'Triggering...';const h=Math.floor(d/3600),m=Math.floor((d%3600)/60),s=d%60;return h>0?`${h}h ${m}m ${s}s`:`${m}m ${s}s`}catch{return'-'}}

function toggleSessionBox(){const b=$('sessionEditBox'),btn=$('toggleSessionBtn');if(b.style.display==='none'){b.style.display='block';btn.textContent='✕ Close'}else{b.style.display='none';btn.textContent='✏️ Update Session'}}
function cancelSessionEdit(){$('sessionEditBox').style.display='none';$('toggleSessionBtn').textContent='✏️ Update Session';$('sessionJson').value=''}
function updateSessionUI(s){const badge=$('sessionBadge'),st=$('sessionStatusText');isLoggedIn=!!s.logged_in;if(isLoggedIn){badge.className='session-badge logged-in';badge.textContent='🟢 Logged In';if(st){st.textContent=s.message||'Session active';st.style.color='#34d399'}}else{badge.className='session-badge logged-out';badge.textContent='🔴 No Session';if(st){st.textContent=s.message||'No session file';st.style.color='#fca5a5'}}}

async function saveSession(){const raw=$('sessionJson').value.trim();if(!raw)return show('Please paste session JSON','err');let data;try{data=JSON.parse(raw)}catch(e){return show('Invalid JSON','err')}if(!data.cookies)return show('Must contain cookies','err');try{show('Saving...','info');const r=await fetch(API+'/api/update-session',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const d=await r.json();if(r.ok){show('✅ Session saved!','ok');$('sessionEditBox').style.display='none';$('toggleSessionBtn').textContent='✏️ Update Session';$('sessionJson').value='';if($('sessionStatusText')){$('sessionStatusText').textContent='Session updated ✓';$('sessionStatusText').style.color='#34d399'}setTimeout(refresh,600)}else show(d.detail||'Failed','err')}catch(e){show(e.message,'err')}}

function renderActive(meetings){
  allMeetings=meetings;
  const search=($('searchMeeting').value||'').trim().toLowerCase();
  let filtered=Object.entries(meetings);
  if(search) filtered=filtered.filter(([m])=>m.toLowerCase().includes(search));
  if(!filtered.length){
    activeListMobile.innerHTML='<div class="empty">No meetings</div>';
    tbodyActive.innerHTML='<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No meetings</td></tr>';
    return;
  }
  activeListMobile.innerHTML=filtered.map(([meeting,m])=>{
    const total=m.total_bots||0, done=m.completed_bots||0, type=m.name_type||'indian', mode=m.join_mode||'individual';
    const status=m.status||'running', startTime=m.started_at?new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'-';
    const isH=search&&meeting.toLowerCase().includes(search);
    return `<div class="meeting-card ${isH?'highlight':''}">
      <div class="mc-top"><div class="mc-id">${meeting}</div><div class="mc-bots">${done}/${total}</div></div>
      <div class="mc-meta">
        <span class="badge ${status==='completed'?'badge-completed':'badge-running'}">${status==='completed'?'Completed':'In Meeting'}</span>
        <span class="badge ${mode==='together'?'badge-together':'badge-slow'}">${mode==='together'?'Together':'Slow'}</span>
        <span class="badge badge-${type}">${type}</span>
      </div>
      <div class="mc-bottom"><span>Started: ${startTime}</span>
        <button class="btn btn-danger btn-sm" onclick="killMeeting('${meeting}')">Kill</button>
      </div></div>`;
  }).join('');
  let idx=0;
  tbodyActive.innerHTML=filtered.map(([meeting,m])=>{
    idx++;
    const total=m.total_bots||0, done=m.completed_bots||0, type=m.name_type||'indian', mode=m.join_mode||'individual';
    const status=m.status||'running', startTime=m.started_at?new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'-';
    const isH=search&&meeting.toLowerCase().includes(search);
    return `<tr class="${isH?'highlight':''}">
      <td>${idx}</td><td style="font-weight:600;color:#93c5fd">${meeting}</td>
      <td><strong style="color:#fbbf24">${done}/${total}</strong></td>
      <td><span class="badge ${status==='completed'?'badge-completed':'badge-running'}">${status==='completed'?'Completed':'In Meeting'}</span></td>
      <td>${startTime}</td>
      <td><span class="badge ${mode==='together'?'badge-together':'badge-slow'}">${mode==='together'?'Together':'Slow'}</span></td>
      <td><button class="btn btn-danger btn-sm" onclick="killMeeting('${meeting}')">Kill</button></td></tr>`;
  }).join('');
}

function renderSchedules(schedules){
  allSchedules=schedules;
  const entries=Object.entries(schedules);
  if(!entries.length){
    scheduleListMobile.innerHTML='<div class="empty">No scheduled meetings</div>';
    tbodySchedule.innerHTML='<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No scheduled meetings</td></tr>';
    return;
  }
  scheduleListMobile.innerHTML=entries.map(([sid,s])=>{
    const when=new Date(s.schedule_at).toLocaleString('en-IN',{timeZone:'Asia/Kolkata',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});
    return `<div class="meeting-card"><div class="mc-top"><div class="mc-id">${s.meeting_code}</div><div class="mc-bots">${s.bot_count}</div></div>
      <div class="mc-meta"><span class="badge ${s.join_mode==='together'?'badge-together':'badge-slow'}">${s.join_mode==='together'?'Together':'Slow'}</span>
      <span class="badge badge-${s.name_type||'indian'}">${s.name_type||'indian'}</span></div>
      <div class="mc-bottom"><div><div>${when}</div><div class="countdown" id="cd-m-${sid}">${formatCountdown(s.schedule_at)}</div></div>
      <button class="btn btn-danger btn-sm" onclick="deleteSchedule('${sid}')">Cancel</button></div></div>`;
  }).join('');
  let idx=0;
  tbodySchedule.innerHTML=entries.map(([sid,s])=>{
    idx++;
    const when=new Date(s.schedule_at).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'});
    return `<tr><td>${idx}</td><td style="font-weight:600;color:#93c5fd">${s.meeting_code}</td>
      <td><strong style="color:#fbbf24">${s.bot_count}</strong></td><td>${when}</td>
      <td class="countdown" id="cd-d-${sid}">${formatCountdown(s.schedule_at)}</td>
      <td><span class="badge ${s.join_mode==='together'?'badge-together':'badge-slow'}">${s.join_mode==='together'?'Together':'Slow'}</span></td>
      <td><button class="btn btn-danger btn-sm" onclick="deleteSchedule('${sid}')">Cancel</button></td></tr>`;
  }).join('');
}

function filterMeetings(){renderActive(allMeetings)}

async function refresh(){
  try{
    const r=await fetch(API+'/status');
    const d=await r.json();
    if(d.session) updateSessionUI(d.session);
    const connected=d.connected_workers_count||Object.keys(d.workers||{}).length;
    totalCap.textContent=(d.total_capacity||0)-(d.total_free_capacity||0);
    totalCapMax.textContent=d.total_capacity||0;
    renderActive(d.meetings||{});
    renderSchedules(d.schedules||{});
    show(`Refreshed • ${connected} worker(s)`,'ok');
  }catch(e){show(e.message||'Failed','err')}
}

setInterval(()=>{Object.keys(allSchedules).forEach(sid=>{const t=formatCountdown(allSchedules[sid].schedule_at);const e1=document.getElementById('cd-m-'+sid),e2=document.getElementById('cd-d-'+sid);if(e1)e1.textContent=t;if(e2)e2.textContent=t})},1000);

async function handleStart(){
  if(!isLoggedIn) return show('Upload session JSON first','err');
  const meeting=meetingId.value.trim().replace(/\s/g,''),pass=passcode.value.trim(),bots=parseInt(botCount.value)||10,dur=parseInt(duration.value)||120,type=nameType.value;
  let custom=null;
  if(type==='custom'){custom=customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);if(custom.length<bots)return show('Need more custom names','err')}
  if(!meeting) return show('Meeting ID required','err');
  const isSchedule=$('enableSchedule').checked;
  if(isSchedule){
    const date=$('scheduleDate').value,time=$('scheduleTime').value;
    if(!date||!time) return show('Select date & time','err');
    try{show('Scheduling...','info');
      const r=await fetch(API+'/api/schedule',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting,passcode:pass,bot_count:bots,duration_minutes:dur,name_type:type,custom_names:custom,join_mode:currentMode,schedule_at:`${date}T${time}:00`})});
      const d=await r.json();
      if(r.ok){show(d.message||'Scheduled!','ok');$('enableSchedule').checked=false;toggleSchedule();setTimeout(refresh,500)}
      else show(d.detail||'Failed','err');
    }catch(e){show(e.message,'err')}
  }else{
    try{show(`Starting ${bots} bots...`,'info');
      const r=await fetch(API+'/api/start-bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting,passcode:pass,bot_count:bots,duration_minutes:dur,name_type:type,custom_names:custom,join_mode:currentMode})});
      const d=await r.json();
      if(r.ok){show(d.message||'Started!','ok');setTimeout(refresh,500)}
      else show(d.detail||'Failed','err');
    }catch(e){show(e.message,'err')}
  }
}

async function killMeeting(meeting){if(!confirm(`Kill all bots for ${meeting}?`))return;try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting})});const d=await r.json();if(r.ok){show(d.message||'Killed','ok');setTimeout(refresh,500)}else show(d.detail||'Failed','err')}catch(e){show(e.message,'err')}}
async function killBySearch(){const meeting=$('searchMeeting').value.trim().replace(/\s/g,'');if(!meeting)return show('Enter Meeting ID','err');await killMeeting(meeting)}
async function killAll(){if(!confirm('Kill ALL meetings?'))return;try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});const d=await r.json();if(r.ok){show('All killed','ok');setTimeout(refresh,500)}else show(d.detail||'Failed','err')}catch(e){show(e.message,'err')}}
async function deleteSchedule(sid){if(!confirm('Cancel schedule?'))return;try{const r=await fetch(API+'/api/schedule/'+sid,{method:'DELETE'});if(r.ok){show('Cancelled','ok');setTimeout(refresh,400)}else show('Failed','err')}catch(e){show(e.message,'err')}}

function openShutdownModal(){$('shutdownModal').style.display='flex';$('shutdownConfirm').value='';$('shutdownConfirm').focus()}
function closeShutdownModal(){$('shutdownModal').style.display='none'}
async function confirmShutdown(){const val=$('shutdownConfirm').value.trim().toLowerCase();if(val!=='yes'){alert('Type "yes" to confirm');return}try{show('Shutting down...','info');await fetch(API+'/api/shutdown',{method:'POST'});show('Shutdown sent','ok');closeShutdownModal()}catch(e){show('Server shutting down...','ok');closeShutdownModal()}}

setInterval(refresh,5000);refresh();
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
