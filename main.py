# ============================================
# ZOOM BOT CENTRAL – FULL SINGLE FILE
# ============================================
import os, uuid, asyncio, json, signal
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
import socketio

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", logger=False, engineio_logger=False)
app = FastAPI(title="Zoom Bot Central")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

workers, running_tasks, meeting_groups, scheduled_tasks = {}, {}, {}, {}
session_status = {"logged_in": False, "last_checked": None, "message": "No session file", "login_in_progress": False}
meeting_logs, global_logs = {}, deque(maxlen=400)

def add_log(meeting, message, level="info"):
    ts = now_ist().strftime("%H:%M:%S")
    line = {"time": ts, "meeting": meeting or "-", "message": message, "level": level}
    global_logs.append(line)
    if meeting and meeting != "-":
        meeting_logs.setdefault(meeting, deque(maxlen=500)).append(line)
    print(f"[{ts}] [{meeting or '-'}] {message}", flush=True)

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
    print(f"[SIO] Connected: {sid}", flush=True)

@sio.event
async def disconnect(sid):
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            for tid in [t for t, x in running_tasks.items() if x.get("worker_id") == wid]:
                task = running_tasks[tid]
                m, bc = task.get("meeting_code"), task.get("bot_count", 0)
                if m in meeting_groups:
                    g = meeting_groups[m]
                    if tid in g.get("task_ids", []): g["task_ids"].remove(tid)
                    g["completed_bots"] = g.get("completed_bots", 0) + bc
                    if not g["task_ids"]: g["status"] = "completed"
                del running_tasks[tid]
            workers[wid]["free_capacity"] = workers[wid]["max_capacity"]
            workers[wid]["sid"] = None
            add_log("-", f"Worker {wid} disconnected", "err")
            break

@sio.event
async def register_worker(sid, data):
    wid = data.get("worker_id", f"worker-{sid[:6]}")
    max_cap = int(data.get("max_capacity", 10))
    now = now_ist().isoformat()
    if wid in workers:
        workers[wid].update({"sid": sid, "max_capacity": max_cap, "last_seen": now})
    else:
        workers[wid] = {"sid": sid, "max_capacity": max_cap, "free_capacity": max_cap, "last_seen": now}
    add_log("-", f"Worker {wid} registered | capacity={max_cap}", "ok")
    await sio.emit("registered", {"worker_id": wid, "max_capacity": max_cap}, to=sid)

@sio.event
async def task_completed(sid, data):
    tid = data.get("task_id")
    if not tid or tid not in running_tasks: return
    task = running_tasks[tid]
    wid, bc, m = task.get("worker_id"), task.get("bot_count", 0), task.get("meeting_code")
    if wid in workers:
        workers[wid]["free_capacity"] = min(workers[wid]["max_capacity"], workers[wid].get("free_capacity", 0) + bc)
    if m in meeting_groups:
        g = meeting_groups[m]
        if tid in g.get("task_ids", []): g["task_ids"].remove(tid)
        g["completed_bots"] = g.get("completed_bots", 0) + bc
        if not g["task_ids"]:
            g["status"] = "completed"
            add_log(m, f"Meeting COMPLETED ({g['completed_bots']}/{g['total_bots']})", "ok")
    del running_tasks[tid]
    add_log(m or "-", f"Task {tid} completed | +{bc} capacity")

@sio.event
async def bot_log(sid, data):
    add_log(data.get("meeting_code", ""), data.get("message", ""), data.get("level", "info"))

@app.get("/health")
async def health(): return {"ok": True}

@app.get("/session")
async def get_session():
    if not os.path.exists("zoom_session.json"): raise HTTPException(404, "Session not found")
    return FileResponse("zoom_session.json", media_type="application/json")

@app.get("/api/session-status")
async def api_session_status():
    session_status["logged_in"] = os.path.exists("zoom_session.json")
    session_status["message"] = "Session file present" if session_status["logged_in"] else "No session file"
    session_status["last_checked"] = now_ist().isoformat()
    return session_status

@app.post("/api/update-session")
async def update_session(request: Request):
    data = await request.json()
    if not isinstance(data, dict) or "cookies" not in data: raise HTTPException(400, "Invalid JSON")
    with open("zoom_session.json", "w") as f: json.dump(data, f, indent=2)
    session_status.update({"logged_in": True, "message": "Session updated ✓", "last_checked": now_ist().isoformat()})
    add_log("-", "✅ Session JSON updated", "ok")
    return {"success": True, "message": "Session saved"}

@app.get("/api/logs")
async def get_logs(meeting: str = None, limit: int = 200):
    logs = list(meeting_logs.get(meeting, []))[-limit:] if meeting else list(global_logs)[-limit:]
    return {"logs": logs, "meeting": meeting}

@app.get("/status")
@app.get("/api/status")
async def status():
    connected = {w: i for w, i in workers.items() if i.get("sid")}
    meetings = {m: {
        "meeting_code": m, "total_bots": i.get("total_bots", 0), "completed_bots": i.get("completed_bots", 0),
        "name_type": i.get("name_type", "indian"), "started_at": i.get("started_at"),
        "join_mode": i.get("join_mode", "individual"), "status": i.get("status", "running")
    } for m, i in meeting_groups.items()}
    return {
        "workers": connected,
        "total_capacity": sum(x.get("max_capacity", 0) for x in connected.values()),
        "total_free_capacity": sum(x.get("free_capacity", 0) for x in connected.values()),
        "meetings": meetings, "schedules": scheduled_tasks, "session": session_status,
        "connected_workers_count": len(connected), "recent_logs": list(global_logs)[-40:]
    }

@app.post("/api/start-bots")
async def start_bots(req: StartBotRequest):
    if not os.path.exists("zoom_session.json"): raise HTTPException(400, "No session file")
    if req.bot_count < 1: raise HTTPException(400, "bot_count >= 1")
    meeting = req.meeting_code.strip().replace(" ", "")
    if not meeting: raise HTTPException(400, "meeting required")
    passcode = "" if req.passcode is None else str(req.passcode)
    remaining, assigned, name_offset = req.bot_count, [], 0
    connected = {w: i for w, i in workers.items() if i.get("sid")}
    for wid, info in sorted(connected.items(), key=lambda x: x[1].get("free_capacity", 0), reverse=True):
        if remaining <= 0: break
        free = int(info.get("free_capacity", 0))
        if free <= 0: continue
        give = min(free, remaining)
        task_id = str(uuid.uuid4())[:8]
        custom_slice = None
        if req.custom_names and req.name_type == "custom":
            custom_slice = req.custom_names[name_offset:name_offset + give]
            name_offset += give
        payload = {"task_id": task_id, "meeting_code": meeting, "passcode": passcode, "bot_count": give,
                   "duration_minutes": req.duration_minutes, "name_type": req.name_type or "indian",
                   "custom_names": custom_slice, "join_mode": req.join_mode or "individual"}
        await sio.emit("new_task", payload, to=info["sid"])
        running_tasks[task_id] = {"task_id": task_id, "meeting_code": meeting, "bot_count": give, "worker_id": wid,
            "name_type": payload["name_type"], "duration_minutes": req.duration_minutes,
            "started_at": now_ist().isoformat(), "join_mode": req.join_mode or "individual"}
        if meeting not in meeting_groups:
            meeting_groups[meeting] = {"task_ids": [], "total_bots": 0, "completed_bots": 0,
                "name_type": payload["name_type"], "join_mode": req.join_mode or "individual",
                "started_at": now_ist().isoformat(), "status": "running"}
        meeting_groups[meeting]["task_ids"].append(task_id)
        meeting_groups[meeting]["total_bots"] += give
        meeting_groups[meeting]["status"] = "running"
        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give})
        remaining -= give
    if not assigned: raise HTTPException(503, "No free workers")
    started = req.bot_count - remaining
    add_log(meeting, f"🚀 Started {started} bots | mode={req.join_mode}", "ok")
    return {"success": True, "message": f"Started {started} bots for {meeting}", "assigned": assigned, "remaining_unassigned": remaining}

@app.post("/api/schedule")
async def create_schedule(req: ScheduleRequest):
    try:
        st = datetime.fromisoformat(req.schedule_at.replace("Z", "+00:00"))
        st = st.replace(tzinfo=IST) if st.tzinfo is None else st.astimezone(IST)
    except Exception as e:
        raise HTTPException(400, str(e))
    if st <= now_ist(): raise HTTPException(400, "Must be future")
    sid = str(uuid.uuid4())[:8]
    scheduled_tasks[sid] = {"schedule_id": sid, "meeting_code": req.meeting_code.strip().replace(" ", ""),
        "passcode": "" if req.passcode is None else str(req.passcode), "bot_count": req.bot_count,
        "duration_minutes": req.duration_minutes, "name_type": req.name_type or "indian",
        "custom_names": req.custom_names, "join_mode": req.join_mode or "individual",
        "schedule_at": st.isoformat(), "created_at": now_ist().isoformat()}
    add_log(req.meeting_code, f"📅 Scheduled {req.bot_count} bots", "info")
    return {"success": True, "schedule_id": sid, "message": "Scheduled"}

@app.delete("/api/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    if schedule_id in scheduled_tasks:
        del scheduled_tasks[schedule_id]
        return {"success": True}
    raise HTTPException(404)

@app.post("/api/terminate")
async def terminate(req: Optional[TerminateRequest] = None):
    if req and req.meeting_code:
        meeting = req.meeting_code.strip().replace(" ", "")
        for tid in [t for t, x in list(running_tasks.items()) if x.get("meeting_code") == meeting]:
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
            if wid in workers:
                workers[wid]["free_capacity"] = min(workers[wid]["max_capacity"], workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0))
            del running_tasks[tid]
        meeting_groups.pop(meeting, None)
        add_log(meeting, "🛑 Terminated", "err")
        return {"success": True, "message": f"Meeting {meeting} terminated"}
    for tid in list(running_tasks.keys()):
        wid = running_tasks[tid].get("worker_id")
        if wid in workers and workers[wid].get("sid"):
            await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
        if wid in workers:
            workers[wid]["free_capacity"] = min(workers[wid]["max_capacity"], workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0))
    running_tasks.clear(); meeting_groups.clear()
    add_log("-", "🛑 ALL terminated", "err")
    return {"success": True, "message": "All terminated"}

@app.post("/api/shutdown")
async def shutdown_server():
    add_log("-", "🛑 SHUTDOWN", "err")
    for wid, info in workers.items():
        if info.get("sid"): await sio.emit("shutdown", {}, to=info["sid"])
    await asyncio.sleep(1.5)
    os.kill(os.getpid(), signal.SIGTERM)
    return {"success": True}

async def schedule_checker():
    while True:
        await asyncio.sleep(4)
        now = now_ist()
        to_run = []
        for sid, info in list(scheduled_tasks.items()):
            try:
                st = datetime.fromisoformat(info["schedule_at"])
                if st.tzinfo is None: st = st.replace(tzinfo=IST)
                if now >= st: to_run.append(sid)
            except: continue
        for sid in to_run:
            info = scheduled_tasks.pop(sid)
            try:
                await start_bots(StartBotRequest(meeting_code=info["meeting_code"], passcode=info["passcode"],
                    bot_count=info["bot_count"], duration_minutes=info["duration_minutes"],
                    name_type=info["name_type"], custom_names=info["custom_names"], join_mode=info["join_mode"]))
            except Exception as e:
                add_log(info.get("meeting_code", "-"), f"Schedule fail: {e}", "err")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_checker())
    if os.path.exists("zoom_session.json"):
        session_status.update({"logged_in": True, "message": "Session present", "last_checked": now_ist().isoformat()})
    add_log("-", "✅ Server started", "ok")

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>Zoom Command Center</title>
<style>
:root{--bg:#070b14;--bg2:#0b1220;--surface:rgba(15,23,42,.78);--surface2:rgba(17,28,49,.9);--input:#0b1424;--border:rgba(148,163,184,.16);--border2:rgba(96,165,250,.3);--text:#eef5ff;--muted:#91a4bd;--primary:#5b8cff;--primary2:#7c5cff;--success:#24d6a0;--danger:#ff5d73;--warning:#ffbf5a;--cyan:#37d7ff;--shadow:0 22px 70px rgba(0,0,0,.34);--radius:20px;--glow:rgba(91,140,255,.2)}
:root[data-theme=light]{--bg:#eef4fb;--bg2:#f7faff;--surface:rgba(255,255,255,.84);--surface2:#fff;--input:#f5f8fc;--border:rgba(15,23,42,.1);--border2:rgba(37,99,235,.25);--text:#122033;--muted:#64748b;--primary:#2563eb;--primary2:#7c3aed;--success:#059669;--danger:#dc3545;--warning:#d97706;--cyan:#0284c7;--shadow:0 18px 55px rgba(30,64,175,.12);--glow:rgba(37,99,235,.1)}
*{margin:0;padding:0;box-sizing:border-box}
body{min-height:100vh;padding:18px;padding-bottom:50px;color:var(--text);font-family:Inter,system-ui,sans-serif;background:radial-gradient(900px 500px at 8% -10%,var(--glow),transparent 65%),linear-gradient(145deg,var(--bg),var(--bg2))}
.container{max-width:1540px;margin:auto}
.header{display:flex;justify-content:space-between;align-items:center;gap:14px;flex-wrap:wrap;padding:15px 18px;margin-bottom:16px;border:1px solid var(--border);border-radius:24px;background:var(--surface);backdrop-filter:blur(18px);box-shadow:var(--shadow);position:sticky;top:12px;z-index:50}
.brand{display:flex;align-items:center;gap:12px}
.brand-mark{width:42px;height:42px;border-radius:14px;display:grid;place-items:center;font-size:20px;background:linear-gradient(135deg,var(--primary),var(--primary2))}
.header h1{font-size:18px}
.brand-sub{font-size:11px;color:var(--muted)}
.header-right{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.usage,.session-badge,.clock{border:1px solid var(--border);background:var(--input);padding:7px 11px;border-radius:999px;font-size:12px;color:var(--muted)}
.usage strong{color:var(--warning)}
.session-badge{font-weight:700}.session-badge.logged-in{color:var(--success)}.session-badge.logged-out{color:var(--danger)}
.grid{display:grid;grid-template-columns:390px 1fr;gap:16px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;box-shadow:var(--shadow)}
.section-title{font-size:13px;font-weight:800;text-transform:uppercase;margin-bottom:14px;display:flex;align-items:center;gap:7px}
.section-title:after{content:"";height:1px;flex:1;background:var(--border)}
.form-group{margin-bottom:12px}
.form-group label{display:block;font-size:11px;color:var(--muted);margin:0 0 6px 2px;font-weight:700;text-transform:uppercase}
.form-group input,.form-group select,.form-group textarea,.search-row input{width:100%;padding:12px 13px;background:var(--input);border:1px solid var(--border);border-radius:12px;color:var(--text);font-size:14px;outline:none}
.form-group input:focus,.search-row input:focus{border-color:var(--primary);box-shadow:0 0 0 4px var(--glow)}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.mode-toggle{display:grid;grid-template-columns:1fr 1fr;gap:4px;background:var(--input);padding:4px;border:1px solid var(--border);border-radius:13px;margin-bottom:14px}
.mode-btn{padding:10px;text-align:center;border-radius:10px;font-size:12px;font-weight:800;color:var(--muted);cursor:pointer}
.mode-btn.active{color:#fff;background:linear-gradient(135deg,var(--primary),var(--primary2))}
.schedule-box{background:var(--input);border:1px solid var(--border);border-radius:14px;padding:12px;margin-bottom:14px}
.schedule-check{display:flex;align-items:center;gap:9px;font-size:13px;font-weight:700}
.schedule-fields{display:none;margin-top:12px;gap:10px}.schedule-fields.show{display:grid;grid-template-columns:1fr 1fr}
.btn-row{display:flex;gap:9px;margin-top:4px;flex-wrap:wrap}
.btn{padding:11px 14px;border:1px solid transparent;border-radius:11px;font-weight:800;font-size:13px;cursor:pointer}
.btn-primary{background:linear-gradient(135deg,var(--primary),var(--primary2));color:#fff;flex:1}
.btn-danger{background:rgba(220,53,69,.13);border-color:rgba(220,53,69,.28);color:var(--danger)}
.btn-success{background:rgba(5,150,105,.13);border-color:rgba(5,150,105,.25);color:var(--success)}
.btn-sm{padding:6px 10px;font-size:11px;border-radius:9px}
.btn-outline{background:var(--input);border-color:var(--border);color:var(--muted)}
.mobile-only{display:block}.desktop-only{display:none}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px}
.stat{padding:14px;border:1px solid var(--border);border-radius:15px;background:var(--input)}
.stat-k{font-size:10px;color:var(--muted);font-weight:800;text-transform:uppercase}
.stat-v{font-size:22px;font-weight:900;margin-top:5px}
.stat-s{font-size:10px;color:var(--muted)}
.meeting-card{background:var(--input);border:1px solid var(--border);border-radius:15px;padding:14px;margin-bottom:9px;cursor:pointer}
.meeting-card:hover,.meeting-card.selected{border-color:var(--primary);box-shadow:0 12px 30px var(--glow)}
.mc-top{display:flex;justify-content:space-between;margin-bottom:10px;gap:8px}
.mc-id{font-weight:800;font-size:14px;word-break:break-all}
.mc-bots{font-size:17px;font-weight:900;color:var(--warning)}
.mc-meta{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:10px}
.mc-bottom{display:flex;justify-content:space-between;font-size:11px;color:var(--muted)}
.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;padding:10px;color:var(--muted);font-weight:800;font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--border)}
td{padding:11px 9px;border-bottom:1px solid var(--border)}
tr.selected td{background:rgba(91,140,255,.08)}
.badge{display:inline-block;padding:4px 8px;border-radius:999px;font-size:10px;font-weight:800}
.badge-slow{background:rgba(217,119,6,.12);color:var(--warning)}
.badge-together{background:rgba(5,150,105,.12);color:var(--success)}
.badge-indian{background:rgba(37,99,235,.12);color:var(--primary)}
.badge-english{background:rgba(5,150,105,.12);color:var(--success)}
.badge-custom{background:rgba(124,58,237,.13);color:#a78bfa}
.badge-running{background:rgba(5,150,105,.12);color:var(--success)}
.badge-completed{background:rgba(100,116,139,.13);color:var(--muted)}
.countdown{font-family:ui-monospace,monospace;color:var(--warning);font-weight:800;font-size:11px}
.search-row{display:flex;gap:8px;margin-bottom:11px}.search-row input{flex:1;min-width:0}
.log{margin-top:12px;padding:10px 12px;background:var(--input);border:1px solid var(--border);border-radius:11px;font-size:11px;color:var(--muted);font-family:ui-monospace,monospace}
.log .ok{color:var(--success)}.log .err{color:var(--danger)}.log .info{color:var(--primary)}
.log-panel{background:#050912;border:1px solid var(--border);border-radius:14px;padding:13px;max-height:330px;overflow-y:auto;font-family:ui-monospace,monospace;font-size:11px;line-height:1.65}
:root[data-theme=light] .log-panel{background:#f5f8fc}
.log-line{margin-bottom:4px;word-break:break-word}
.log-line .t{color:#64748b;margin-right:8px}.log-line .m{color:var(--cyan);margin-right:6px}
.log-line.ok{color:var(--success)}.log-line.err{color:var(--danger)}.log-line.info{color:var(--text)}
.empty{text-align:center;color:var(--muted);padding:25px;font-size:13px}
.modal-overlay{position:fixed;inset:0;background:rgba(2,6,23,.72);display:none;align-items:center;justify-content:center;z-index:9999;padding:16px}
.modal{background:var(--surface2);border:1px solid var(--border2);border-radius:20px;padding:24px;max-width:420px;width:100%}
.modal input{width:100%;padding:12px;margin:12px 0;background:var(--input);border:1px solid var(--border);border-radius:10px;color:var(--text)}
.theme-pop{position:relative}
.theme-menu{display:none;position:absolute;right:0;top:42px;padding:9px;background:var(--surface2);border:1px solid var(--border);border-radius:14px;min-width:150px;z-index:100}
.theme-menu.show{display:block}
.theme-menu button{display:block;width:100%;text-align:left;padding:9px;border:0;background:transparent;color:var(--text);border-radius:8px;cursor:pointer}
@media(min-width:1000px){.mobile-only{display:none}.desktop-only{display:block}}
@media(max-width:999px){.grid{grid-template-columns:1fr}.header{position:relative;top:0}}
@media(max-width:767px){body{padding:10px}.form-row,.schedule-fields.show{grid-template-columns:1fr}.stats{grid-template-columns:1fr 1fr 1fr}}
</style></head>
<body>
<div class="container">
<div class="header">
  <div class="brand"><div class="brand-mark">⚡</div><div><h1>Zoom Command Center</h1><div class="brand-sub">Operations • Capacity • Live Control</div></div></div>
  <div class="header-right">
    <div id="sessionBadge" class="session-badge logged-out">Checking...</div>
    <div class="usage">CAP <strong id="totalCap">0</strong>/<strong id="totalCapMax">0</strong></div>
    <span id="liveTime" class="clock"></span>
    <button class="btn btn-outline btn-sm" onclick="refresh()">↻ Refresh</button>
    <div class="theme-pop">
      <button class="btn btn-outline btn-sm" onclick="toggleThemeMenu()">◐ Theme</button>
      <div id="themeMenu" class="theme-menu">
        <button onclick="setTheme('dark')">🌙 Night</button>
        <button onclick="setTheme('light')">☀️ Day</button>
        <button onclick="setTheme('system')">🖥 System</button>
      </div>
    </div>
    <button class="btn btn-danger btn-sm" onclick="openShutdownModal()">🛑 Shutdown</button>
  </div>
</div>

<div class="card" style="margin-bottom:16px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
    <div class="section-title" style="margin:0">🔑 Zoom Session</div>
    <button class="btn btn-outline btn-sm" id="toggleSessionBtn" onclick="toggleSessionBox()">✏️ Update Session</button>
  </div>
  <div style="font-size:13px;color:var(--muted)">Status: <span id="sessionStatusText">Checking...</span></div>
  <div id="sessionEditBox" style="display:none;margin-top:12px">
    <textarea id="sessionJson" rows="7" placeholder='{"cookies":[...]}' style="width:100%;padding:12px;background:var(--input);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:12px;font-family:monospace"></textarea>
    <div style="display:flex;gap:10px;margin-top:10px">
      <button class="btn btn-success" style="flex:1" onclick="saveSession()">💾 Save</button>
      <button class="btn btn-outline" onclick="cancelSessionEdit()">Cancel</button>
    </div>
  </div>
</div>

<div class="stats">
  <div class="stat"><div class="stat-k">Active Capacity</div><div class="stat-v"><span id="dashActive">0</span></div><div class="stat-s">allocated</div></div>
  <div class="stat"><div class="stat-k">Meetings</div><div class="stat-v"><span id="dashMeetings">0</span></div><div class="stat-s">live</div></div>
  <div class="stat"><div class="stat-k">Workers</div><div class="stat-v" id="dashMode">0</div><div class="stat-s">connected</div></div>
</div>

<div class="grid">
<div class="card">
  <div class="section-title">🚀 Launch</div>
  <div class="mode-toggle">
    <div class="mode-btn active" id="modeSlow" onclick="setMode('individual')">🐢 Slow</div>
    <div class="mode-btn" id="modeTogether" onclick="setMode('together')">⚡ Together</div>
  </div>
  <form onsubmit="handleStart();return false">
    <div class="form-group"><label>Meeting ID</label><input id="meetingId" placeholder="98695209590" inputmode="numeric"/></div>
    <div class="form-group"><label>Passcode (blank=none, 0=valid)</label><input id="passcode" placeholder="Leave blank if none"/></div>
    <div class="form-row">
      <div class="form-group"><label>Bots</label><input type="number" id="botCount" value="20" min="1" max="500" oninput="updCount()"/></div>
      <div class="form-group"><label>Names</label>
        <select id="nameType" onchange="toggleCustom()">
          <option value="indian">🇮🇳 Indian</option>
          <option value="english">🇺🇸 English</option>
          <option value="custom">✏️ Custom</option>
        </select>
      </div>
    </div>
    <div id="customBox" style="display:none;margin:12px 0;padding:14px;background:var(--input);border:1px solid var(--border);border-radius:12px">
      <textarea id="customNames" rows="5" placeholder="One name per line" style="width:100%;padding:12px;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-family:monospace"></textarea>
      <div style="font-size:12px;color:var(--muted);margin-top:8px">Names: <b id="nameCount">0</b> | Need: <b id="needCount">20</b> <span id="nameStatus"></span></div>
    </div>
    <div class="form-group"><label>Duration (min)</label><input type="number" id="duration" value="120" min="1"/></div>
    <div class="schedule-box">
      <label class="schedule-check"><input type="checkbox" id="enableSchedule" onchange="toggleSchedule()"/> Enable Scheduling</label>
      <div class="schedule-fields" id="scheduleFields">
        <div class="form-group" style="margin:0"><label>Date</label><input type="date" id="scheduleDate"/></div>
        <div class="form-group" style="margin:0"><label>Time IST</label><input type="time" id="scheduleTime"/></div>
      </div>
    </div>
    <div class="btn-row">
      <button type="submit" class="btn btn-primary" id="startBtn">▶ Start Now</button>
      <button type="button" class="btn btn-danger" onclick="killAll()">Kill All</button>
    </div>
  </form>
  <div id="msg" class="log">Ready • Press Enter to start</div>
</div>

<div style="display:flex;flex-direction:column;gap:16px">
  <div class="card">
    <div class="section-title">🟢 Meetings <span style="font-weight:400;font-size:11px;color:var(--muted)">(click for logs)</span></div>
    <div class="search-row">
      <input id="searchMeeting" placeholder="Search Meeting ID" oninput="filterMeetings()"/>
      <button class="btn btn-danger btn-sm" onclick="killBySearch()">Kill</button>
    </div>
    <div id="activeListMobile" class="mobile-only"><div class="empty">No meetings</div></div>
    <div class="desktop-only table-wrap">
      <table>
        <thead><tr><th>#</th><th>Meeting</th><th>Bots</th><th>Status</th><th>Started</th><th>Mode</th><th></th></tr></thead>
        <tbody id="tbodyActive"><tr><td colspan="7" class="empty">No meetings</td></tr></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="section-title">📅 Scheduled</div>
    <div id="scheduleListMobile" class="mobile-only"><div class="empty">None</div></div>
    <div class="desktop-only table-wrap">
      <table>
        <thead><tr><th>#</th><th>Meeting</th><th>Bots</th><th>When</th><th>Countdown</th><th>Mode</th><th></th></tr></thead>
        <tbody id="tbodySchedule"><tr><td colspan="7" class="empty">None</td></tr></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
      <div class="section-title" style="margin:0">📜 Live Logs <span id="logFilterLabel" style="font-weight:400;color:var(--muted);font-size:12px"></span></div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-outline btn-sm" onclick="clearLogFilter()">All</button>
        <button class="btn btn-outline btn-sm" onclick="refreshLogs()">↻</button>
      </div>
    </div>
    <div id="logPanel" class="log-panel"><div style="color:var(--muted)">👆 Click a meeting to view bot logs</div></div>
  </div>
</div>
</div>
</div>

<div class="modal-overlay" id="shutdownModal">
  <div class="modal">
    <h3>🛑 Shutdown?</h3>
    <p style="font-size:13px;color:var(--muted)">Type <b>yes</b></p>
    <input id="shutdownConfirm" placeholder="yes"/>
    <div style="display:flex;gap:10px">
      <button class="btn btn-danger" style="flex:1" onclick="confirmShutdown()">Shutdown</button>
      <button class="btn btn-outline" onclick="closeShutdownModal()">Cancel</button>
    </div>
  </div>
</div>

<script>
const API=location.origin,$=id=>document.getElementById(id);
let currentMode='individual',allMeetings={},allSchedules={},isLoggedIn=false,activeLogMeeting=null;
(function(){const s=localStorage.getItem('zcc-theme')||'dark';if(s!=='system')document.documentElement.setAttribute('data-theme',s)})();
function setTheme(t){if(t==='system')document.documentElement.removeAttribute('data-theme');else document.documentElement.setAttribute('data-theme',t);localStorage.setItem('zcc-theme',t);$('themeMenu').classList.remove('show')}
function toggleThemeMenu(){$('themeMenu').classList.toggle('show')}
document.addEventListener('click',e=>{const p=document.querySelector('.theme-pop');if(p&&!p.contains(e.target))$('themeMenu').classList.remove('show')});
function setMode(m){currentMode=m;$('modeSlow').classList.toggle('active',m==='individual');$('modeTogether').classList.toggle('active',m==='together')}
function toggleSchedule(){const e=$('enableSchedule').checked;$('scheduleFields').classList.toggle('show',e);$('startBtn').textContent=e?'📅 Schedule':'▶ Start Now'}
function show(m,t='info'){msg.innerHTML=`<span class="${t==='ok'?'ok':t==='err'?'err':'info'}">[${new Date().toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'})}] ${m}</span>`}
function toggleCustom(){customBox.style.display=nameType.value==='custom'?'block':'none';updCount()}
function updCount(){const b=parseInt(botCount.value)||0,n=customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);nameCount.textContent=n.length;needCount.textContent=b;nameStatus.innerHTML=nameType.value!=='custom'?'':(n.length>=b?' <span style="color:var(--success)">✅</span>':` <span style="color:var(--danger)">❌ ${b-n.length}</span>`)}
customNames.addEventListener('input',updCount);
setInterval(()=>{liveTime.textContent=new Date().toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'})+' IST'},1000);
function formatCountdown(iso){try{let d=Math.floor((new Date(iso)-new Date())/1000);if(d<=0)return'Triggering...';const h=Math.floor(d/3600),m=Math.floor((d%3600)/60),s=d%60;return h>0?`${h}h ${m}m ${s}s`:`${m}m ${s}s`}catch{return'-'}}
function toggleSessionBox(){const b=$('sessionEditBox'),btn=$('toggleSessionBtn');if(b.style.display==='none'){b.style.display='block';btn.textContent='✕ Close'}else{b.style.display='none';btn.textContent='✏️ Update Session'}}
function cancelSessionEdit(){$('sessionEditBox').style.display='none';$('toggleSessionBtn').textContent='✏️ Update Session';sessionJson.value=''}
function updateSessionUI(s){isLoggedIn=!!s.logged_in;sessionBadge.className='session-badge '+(isLoggedIn?'logged-in':'logged-out');sessionBadge.textContent=isLoggedIn?'🟢 Logged In':'🔴 No Session';sessionStatusText.textContent=s.message||'';sessionStatusText.style.color=isLoggedIn?'var(--success)':'var(--danger)'}
async function saveSession(){const raw=sessionJson.value.trim();if(!raw)return show('Paste JSON','err');let data;try{data=JSON.parse(raw)}catch{return show('Invalid JSON','err')}if(!data.cookies)return show('Need cookies','err');try{show('Saving...','info');const r=await fetch(API+'/api/update-session',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const d=await r.json();if(r.ok){show('✅ Saved','ok');cancelSessionEdit();setTimeout(refresh,500)}else show(d.detail||'Fail','err')}catch(e){show(e.message,'err')}}
function renderLogs(logs){const panel=$('logPanel');if(!activeLogMeeting){panel.innerHTML='<div style="color:var(--muted)">👆 Click a meeting to view bot logs</div>';logFilterLabel.textContent='';return}if(!logs||!logs.length){panel.innerHTML='<div style="color:var(--muted)">No logs yet</div>';return}panel.innerHTML=logs.map(l=>`<div class="log-line ${l.level==='ok'?'ok':l.level==='err'?'err':'info'}"><span class="t">${l.time}</span><span class="m">[${l.meeting}]</span>${l.message}</div>`).join('');panel.scrollTop=panel.scrollHeight}
async function refreshLogs(){if(!activeLogMeeting){renderLogs([]);return}try{const r=await fetch(API+'/api/logs?meeting='+encodeURIComponent(activeLogMeeting)+'&limit=200');const d=await r.json();renderLogs(d.logs||[]);logFilterLabel.textContent='• '+activeLogMeeting}catch(e){}}
function selectMeetingLogs(m){activeLogMeeting=m;refreshLogs();show('Logs: '+m,'info')}
function clearLogFilter(){activeLogMeeting=null;renderLogs([])}
function renderActive(meetings){allMeetings=meetings;const search=(searchMeeting.value||'').trim().toLowerCase();let f=Object.entries(meetings);if(search)f=f.filter(([m])=>m.toLowerCase().includes(search));if(!f.length){activeListMobile.innerHTML='<div class="empty">No meetings</div>';tbodyActive.innerHTML='<tr><td colspan="7" class="empty">No meetings</td></tr>';return}
activeListMobile.innerHTML=f.map(([meeting,m])=>{const total=m.total_bots||0,done=m.completed_bots||0,st=m.status||'running',mode=m.join_mode||'individual',type=m.name_type||'indian',t=m.started_at?new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'-';return`<div class="meeting-card ${activeLogMeeting===meeting?'selected':''}" onclick="selectMeetingLogs('${meeting}')"><div class="mc-top"><div class="mc-id">${meeting}</div><div class="mc-bots">${done}/${total}</div></div><div class="mc-meta"><span class="badge ${st==='completed'?'badge-completed':'badge-running'}">${st==='completed'?'Completed':'In Meeting'}</span><span class="badge ${mode==='together'?'badge-together':'badge-slow'}">${mode}</span><span class="badge badge-${type}">${type}</span></div><div class="mc-bottom"><span>${t}</span><button class="btn btn-danger btn-sm" onclick="event.stopPropagation();killMeeting('${meeting}')">Kill</button></div></div>`}).join('');
let i=0;tbodyActive.innerHTML=f.map(([meeting,m])=>{i++;const total=m.total_bots||0,done=m.completed_bots||0,st=m.status||'running',mode=m.join_mode||'individual',t=m.started_at?new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'-';return`<tr class="${activeLogMeeting===meeting?'selected':''}" style="cursor:pointer" onclick="selectMeetingLogs('${meeting}')"><td>${i}</td><td style="font-weight:700">${meeting}</td><td><b style="color:var(--warning)">${done}/${total}</b></td><td><span class="badge ${st==='completed'?'badge-completed':'badge-running'}">${st==='completed'?'Completed':'In Meeting'}</span></td><td>${t}</td><td><span class="badge ${mode==='together'?'badge-together':'badge-slow'}">${mode}</span></td><td><button class="btn btn-danger btn-sm" onclick="event.stopPropagation();killMeeting('${meeting}')">Kill</button></td></tr>`}).join('')}
function renderSchedules(s){allSchedules=s;const e=Object.entries(s);if(!e.length){scheduleListMobile.innerHTML='<div class="empty">None</div>';tbodySchedule.innerHTML='<tr><td colspan="7" class="empty">None</td></tr>';return}
scheduleListMobile.innerHTML=e.map(([sid,x])=>{const when=new Date(x.schedule_at).toLocaleString('en-IN',{timeZone:'Asia/Kolkata',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});return`<div class="meeting-card"><div class="mc-top"><div class="mc-id">${x.meeting_code}</div><div class="mc-bots">${x.bot_count}</div></div><div class="mc-bottom"><div>${when}<div class="countdown" id="cd-m-${sid}">${formatCountdown(x.schedule_at)}</div></div><button class="btn btn-danger btn-sm" onclick="deleteSchedule('${sid}')">Cancel</button></div></div>`}).join('');
let i=0;tbodySchedule.innerHTML=e.map(([sid,x])=>{i++;return`<tr><td>${i}</td><td>${x.meeting_code}</td><td><b style="color:var(--warning)">${x.bot_count}</b></td><td>${new Date(x.schedule_at).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})}</td><td class="countdown" id="cd-d-${sid}">${formatCountdown(x.schedule_at)}</td><td>${x.join_mode}</td><td><button class="btn btn-danger btn-sm" onclick="deleteSchedule('${sid}')">Cancel</button></td></tr>`}).join('')}
function filterMeetings(){renderActive(allMeetings)}
async function refresh(){try{const r=await fetch(API+'/status');const d=await r.json();if(d.session)updateSessionUI(d.session);const c=d.connected_workers_count||0,a=(d.total_capacity||0)-(d.total_free_capacity||0);totalCap.textContent=a;totalCapMax.textContent=d.total_capacity||0;dashActive.textContent=a;dashMeetings.textContent=Object.keys(d.meetings||{}).length;dashMode.textContent=c;renderActive(d.meetings||{});renderSchedules(d.schedules||{});await refreshLogs();show('Refreshed • '+c+' worker(s)','ok')}catch(e){show(e.message||'Fail','err')}}
setInterval(()=>{Object.keys(allSchedules).forEach(sid=>{const t=formatCountdown(allSchedules[sid].schedule_at);const a=document.getElementById('cd-m-'+sid),b=document.getElementById('cd-d-'+sid);if(a)a.textContent=t;if(b)b.textContent=t})},1000);
async function handleStart(){if(!isLoggedIn)return show('Upload session first','err');const meeting=meetingId.value.trim().replace(/\s/g,''),pass=passcode.value,bots=parseInt(botCount.value)||10,dur=parseInt(duration.value)||120,type=nameType.value;let custom=null;if(type==='custom'){custom=customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);if(custom.length<bots)return show('Need more names','err')}if(!meeting)return show('Meeting ID required','err');if($('enableSchedule').checked){const date=scheduleDate.value,time=scheduleTime.value;if(!date||!time)return show('Date & time','err');try{show('Scheduling...','info');const r=await fetch(API+'/api/schedule',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting,passcode:pass,bot_count:bots,duration_minutes:dur,name_type:type,custom_names:custom,join_mode:currentMode,schedule_at:date+'T'+time+':00'})});const d=await r.json();if(r.ok){show(d.message||'OK','ok');$('enableSchedule').checked=false;toggleSchedule();setTimeout(refresh,400)}else show(d.detail||'Fail','err')}catch(e){show(e.message,'err')}}else{try{show('Starting '+bots+'...','info');const r=await fetch(API+'/api/start-bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting,passcode:pass,bot_count:bots,duration_minutes:dur,name_type:type,custom_names:custom,join_mode:currentMode})});const d=await r.json();if(r.ok){show(d.message||'Started','ok');setTimeout(refresh,400)}else show(d.detail||'Fail','err')}catch(e){show(e.message,'err')}}}
async function killMeeting(m){if(!confirm('Kill '+m+'?'))return;try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:m})});const d=await r.json();if(r.ok){show(d.message||'Killed','ok');setTimeout(refresh,400)}else show(d.detail||'Fail','err')}catch(e){show(e.message,'err')}}
async function killBySearch(){const m=searchMeeting.value.trim().replace(/\s/g,'');if(!m)return show('Enter ID','err');await killMeeting(m)}
async function killAll(){if(!confirm('Kill ALL?'))return;try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});const d=await r.json();if(r.ok){show('All killed','ok');setTimeout(refresh,400)}else show(d.detail||'Fail','err')}catch(e){show(e.message,'err')}}
async function deleteSchedule(sid){if(!confirm('Cancel?'))return;try{const r=await fetch(API+'/api/schedule/'+sid,{method:'DELETE'});if(r.ok){show('Cancelled','ok');setTimeout(refresh,300)}}catch(e){show(e.message,'err')}}
function openShutdownModal(){shutdownModal.style.display='flex';shutdownConfirm.value='';shutdownConfirm.focus()}
function closeShutdownModal(){shutdownModal.style.display='none'}
async function confirmShutdown(){if(shutdownConfirm.value.trim().toLowerCase()!=='yes')return alert('Type yes');try{await fetch(API+'/api/shutdown',{method:'POST'});show('Shutdown sent','ok');closeShutdownModal()}catch(e){show('Shutting down...','ok');closeShutdownModal()}}
setInterval(refresh,5000);setInterval(refreshLogs,2500);refresh();
</script>
</body></html>"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(asgi_app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
