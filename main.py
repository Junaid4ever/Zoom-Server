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
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"/>
<title>Zoom Command Center</title>
<style>
:root{
 --bg:#07101d;--panel:#0d1828;--panel2:#111f33;--input:#081321;--line:#22334b;
 --text:#edf5ff;--muted:#8da2bc;--blue:#4f8cff;--violet:#7c5cff;
 --green:#20d39a;--red:#ff5b70;--yellow:#ffbd55;--cyan:#36d9ff;
}
:root.light{
 --bg:#eef4fa;--panel:#ffffff;--panel2:#f7faff;--input:#f2f6fa;--line:#dbe4ee;
 --text:#142033;--muted:#64748b;--blue:#2563eb;--violet:#7c3aed;
 --green:#059669;--red:#dc3545;--yellow:#d97706;--cyan:#0284c7;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{width:100%;height:100%;overflow:hidden}
body{
 background:radial-gradient(circle at 15% 0%,rgba(79,140,255,.16),transparent 32%),
 linear-gradient(135deg,var(--bg),var(--bg));color:var(--text);
 font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
 font-size:13px
}
button,input,select,textarea{font:inherit}
button{cursor:pointer}
.container{width:100%;height:100vh;padding:10px;display:flex;flex-direction:column;gap:10px}

/* TOP BAR */
.header{
 flex:0 0 58px;display:flex;align-items:center;justify-content:space-between;gap:8px;
 padding:9px 13px;background:var(--panel);border:1px solid var(--line);
 border-radius:15px;box-shadow:0 8px 28px rgba(0,0,0,.18)
}
.brand{display:flex;align-items:center;gap:9px;min-width:0}
.brand-icon{
 width:36px;height:36px;border-radius:11px;display:grid;place-items:center;
 background:linear-gradient(135deg,var(--blue),var(--violet));font-size:18px;flex:none
}
.header h1{font-size:16px;white-space:nowrap}.brand-sub{font-size:9px;color:var(--muted)}
.header-right{display:flex;align-items:center;gap:6px}
.usage,.session-badge,.clock{
 padding:6px 9px;border:1px solid var(--line);background:var(--input);
 border-radius:99px;font-size:10px;white-space:nowrap
}
.session-badge.logged-in{color:var(--green)}.session-badge.logged-out{color:var(--red)}
.clock{font-family:monospace}

/* SIMPLE DAY/NIGHT SWITCH */
.theme-switch{position:relative;width:48px;height:25px;flex:none}
.theme-switch input{display:none}
.slider{
 position:absolute;inset:0;border-radius:99px;background:#18263a;border:1px solid var(--line);
 cursor:pointer;transition:.25s
}
.slider:before{
 content:"🌙";position:absolute;width:19px;height:19px;left:2px;top:2px;
 border-radius:50%;display:grid;place-items:center;background:#07101d;font-size:10px;
 transition:.25s
}
.theme-switch input:checked+.slider{background:#dbeafe}
.theme-switch input:checked+.slider:before{transform:translateX(23px);content:"☀️";background:white}

/* MAIN NO-SCROLL AREA */
.grid{
 min-height:0;flex:1;display:grid;grid-template-columns:335px minmax(0,1fr);gap:10px
}
.left,.right{min-height:0;display:flex;flex-direction:column;gap:10px}
.card{
 min-height:0;background:var(--panel);border:1px solid var(--line);border-radius:15px;
 padding:13px;box-shadow:0 8px 28px rgba(0,0,0,.14)
}
.section-title{
 font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.55px;
 color:var(--text);margin-bottom:9px;display:flex;align-items:center;gap:6px
}
.section-title:after{content:"";height:1px;flex:1;background:var(--line)}
.form-group{margin-bottom:8px}
.form-group label{display:block;font-size:9px;text-transform:uppercase;color:var(--muted);font-weight:800;margin:0 0 4px 1px}
.form-group input,.form-group select,.form-group textarea,.search-row input{
 width:100%;background:var(--input);color:var(--text);border:1px solid var(--line);
 border-radius:9px;padding:8px 10px;outline:none;font-size:12px
}
.form-group input:focus,.form-group select:focus,.form-group textarea:focus,.search-row input:focus{
 border-color:var(--blue);box-shadow:0 0 0 3px rgba(79,140,255,.10)
}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:7px}
.mode-toggle{display:grid;grid-template-columns:1fr 1fr;gap:3px;padding:3px;background:var(--input);border:1px solid var(--line);border-radius:10px;margin-bottom:9px}
.mode-btn{padding:7px;text-align:center;color:var(--muted);border-radius:7px;font-size:11px;font-weight:800}
.mode-btn.active{background:linear-gradient(135deg,var(--blue),var(--violet));color:white}
.schedule-box{padding:9px;background:var(--input);border:1px solid var(--line);border-radius:10px;margin-bottom:9px}
.schedule-check{display:flex;align-items:center;gap:7px;font-size:11px;font-weight:700}
.schedule-check input{accent-color:var(--blue)}
.schedule-fields{display:none;margin-top:7px;gap:7px}.schedule-fields.show{display:grid;grid-template-columns:1fr 1fr}
.btn-row{display:flex;gap:7px}.btn{
 border:1px solid transparent;border-radius:9px;padding:8px 10px;font-size:11px;font-weight:800
}
.btn-primary{flex:1;background:linear-gradient(135deg,var(--blue),var(--violet));color:white}
.btn-danger{background:rgba(220,53,69,.10);border-color:rgba(220,53,69,.25);color:var(--red)}
.btn-success{background:rgba(5,150,105,.10);border-color:rgba(5,150,105,.22);color:var(--green)}
.btn-outline{background:var(--input);border-color:var(--line);color:var(--muted)}
.btn-sm{padding:5px 8px;font-size:10px}
#customBox{background:var(--input)!important;border-color:var(--line)!important}
#customNames{background:var(--panel2)!important;border-color:var(--line)!important;color:var(--text)!important}

/* RIGHT SIDE: fixed compact panels */
.right>.card:nth-child(1){flex:1.05}
.right>.card:nth-child(2){flex:.75}
.right>.card:nth-child(3){flex:1;display:flex;flex-direction:column}
.table-wrap{height:calc(100% - 26px);overflow:auto}
table{width:100%;border-collapse:collapse;font-size:10px}
th{padding:6px;color:var(--muted);font-size:8px;text-transform:uppercase;text-align:left;border-bottom:1px solid var(--line)}
td{padding:7px;border-bottom:1px solid var(--line)}
tr:hover td{background:rgba(79,140,255,.05)}
.search-row{display:flex;gap:6px;margin-bottom:7px}
.search-row input{flex:1;min-width:0}
.mobile-only{display:none}
.desktop-only{display:block}
.meeting-card{background:var(--input);border:1px solid var(--line);border-radius:10px;padding:9px;margin-bottom:6px}
.mc-top{display:flex;justify-content:space-between;gap:6px;margin-bottom:6px}.mc-id{font-weight:800}.mc-bots{color:var(--yellow);font-weight:900}
.mc-meta{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:6px}.mc-bottom{display:flex;justify-content:space-between;color:var(--muted);font-size:9px}
.badge{display:inline-block;padding:3px 6px;border-radius:99px;font-size:8px;font-weight:800}
.badge-slow{background:rgba(217,119,6,.12);color:var(--yellow)}
.badge-together,.badge-running{background:rgba(5,150,105,.12);color:var(--green)}
.badge-indian{background:rgba(37,99,235,.12);color:var(--blue)}
.badge-english{background:rgba(5,150,105,.12);color:var(--green)}
.badge-custom{background:rgba(124,58,237,.12);color:#a78bfa}
.badge-completed{background:rgba(100,116,139,.12);color:var(--muted)}
.countdown{font-family:monospace;color:var(--yellow);font-size:9px;font-weight:800}
.log-panel{
 flex:1;min-height:0;overflow:auto;background:#050a12;color:#cbd5e1;
 border:1px solid var(--line);border-radius:10px;padding:8px;font:9px/1.55 monospace
}
:root.light .log-panel{background:#f4f7fb;color:#334155}
.log-line{margin-bottom:2px}.log-line .t{color:#64748b;margin-right:5px}.log-line .m{color:var(--cyan);margin-right:5px}
.log-line.ok{color:var(--green)}.log-line.err{color:var(--red)}.log-line.info{color:var(--text)}
.log{margin-top:7px;padding:7px 9px;background:var(--input);border:1px solid var(--line);border-radius:9px;font:9px monospace;color:var(--muted)}
.log .ok{color:var(--green)}.log .err{color:var(--red)}.log .info{color:var(--blue)}
.empty{text-align:center;color:var(--muted);padding:12px;font-size:10px}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;align-items:center;justify-content:center;z-index:9999;padding:15px}
.modal{width:min(400px,100%);background:var(--panel);border:1px solid var(--line);border-radius:15px;padding:18px}
.modal input{width:100%;padding:10px;background:var(--input);color:var(--text);border:1px solid var(--line);border-radius:9px;margin:9px 0}

/* TABLET / MOBILE */
@media(max-width:999px){
 html,body{overflow:auto}
 .container{height:auto;min-height:100vh}
 .header{position:relative;flex-wrap:nowrap}
 .header-right .usage,.header-right .clock{display:none}
 .grid{display:flex;flex-direction:column}
 .left,.right{min-height:auto}
 .right>.card{flex:none!important}
 .log-panel{height:260px;flex:none}
}
@media(max-width:600px){
 body{font-size:12px}.container{padding:7px;gap:7px}
 .header{padding:8px 9px;border-radius:12px}.brand-sub{display:none}.header h1{font-size:14px}
 .brand-icon{width:31px;height:31px;font-size:15px;border-radius:9px}
 .header-right{gap:4px}.session-badge{font-size:9px;padding:5px 7px}
 .grid{gap:7px}.card{padding:10px;border-radius:12px}
 .form-row,.schedule-fields.show{grid-template-columns:1fr 1fr}
 .btn{padding:8px;font-size:10px}
 .desktop-only{display:none}.mobile-only{display:block}
 .right>.card:nth-child(1),.right>.card:nth-child(2){max-height:none}
 .log-panel{height:230px}
}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <div class="brand">
      <div class="brand-icon">⚡</div>
      <div><h1>Zoom Command Center</h1><div class="brand-sub">LIVE CONTROL DECK</div></div>
    </div>
    <div class="header-right">
      <div id="sessionBadge" class="session-badge logged-out">Checking...</div>
      <div class="usage"><strong id="totalCap">0</strong>/<strong id="totalCapMax">0</strong></div>
      <span id="liveTime" class="clock"></span>
      <button class="btn btn-outline btn-sm" onclick="refresh()">↻</button>
      <label class="theme-switch" title="Day / Night">
        <input id="themeToggle" type="checkbox" onchange="toggleDayNight()">
        <span class="slider"></span>
      </label>
      <button class="btn btn-danger btn-sm" onclick="openShutdownModal()">🛑</button>
    </div>
  </div>

  <div class="card" style="margin-bottom:16px;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
      <div class="section-title" style="margin:0;">🔑 Zoom Session</div>
      <button class="btn btn-outline btn-sm" id="toggleSessionBtn" onclick="toggleSessionBox()">✏️ Update Session</button>
    </div>
    <div id="sessionStatusLine" style="font-size:13px;color:var(--muted);">Status: <span id="sessionStatusText">Checking...</span></div>
    <div id="sessionEditBox" style="display:none;margin-top:12px;">
      <div style="font-size:12px;color:var(--muted);margin-bottom:8px;">Paste full <b>zoom_session.json</b> content</div>
      <textarea id="sessionJson" rows="8" placeholder='{"cookies":[...],"origins":[...]}' style="width:100%;padding:12px;background:#0f172a;border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:12px;font-family:monospace;resize:vertical;"></textarea>
      <div style="display:flex;gap:10px;margin-top:10px;">
        <button class="btn btn-success" onclick="saveSession()" style="flex:1;">💾 Save Session</button>
        <button class="btn btn-outline" onclick="cancelSessionEdit()">Cancel</button>
      </div>
    </div>
  </div>

  <div class="grid">
    <div class="left"><div class="card">
      <div class="section-title">🚀 Launch / Schedule</div>
      <div class="mode-toggle">
        <div class="mode-btn active" id="modeSlow" onclick="setMode('individual')">🐢 Slow</div>
        <div class="mode-btn" id="modeTogether" onclick="setMode('together')">⚡ Together</div>
      </div>
      <form id="launchForm" onsubmit="handleStart(); return false;">
        <div class="form-group"><label>Meeting ID</label><input id="meetingId" placeholder="98695209590" inputmode="numeric"/></div>
        <div class="form-group"><label>Passcode (optional — use 0 if passcode is 0)</label><input id="passcode" placeholder="Leave blank if none"/></div>
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
          <textarea id="customNames" rows="6" placeholder="Rahul Sharma&#10;Priya Verma&#10;..." style="width:100%;padding:12px;background:#111827;border:1px solid #334155;border-radius:10px;color:var(--text);font-size:13px;font-family:ui-monospace,monospace;resize:vertical;line-height:1.5;"></textarea>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-top:10px;font-size:12px;">
            <div style="color:var(--muted);">Names: <strong id="nameCount" style="color:#e2e8f0;">0</strong> | Need: <strong id="needCount" style="color:#e2e8f0;">20</strong> <span id="nameStatus"></span></div>
            <button type="button" class="btn btn-outline btn-sm" onclick="document.getElementById('customNames').value='';updCount();">Clear</button>
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
          <button type="submit" class="btn btn-primary" id="startBtn">▶ Start Now</button>
          <button type="button" class="btn btn-danger" onclick="killAll()">Kill All</button>
        </div>
      </form>
      <div id="msg" class="log">Ready • IST • Press Enter to start</div>
    </div></div>

    <div class="right">
      <div class="card">
        <div class="section-title">🟢 Meetings <span style="font-weight:400;font-size:12px;color:var(--muted)">(click for logs)</span></div>
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
        <div id="scheduleListMobile" class="mobile-only"><div class="empty">No scheduled</div></div>
        <div class="desktop-only table-wrap">
          <table>
            <thead><tr><th>#</th><th>Meeting</th><th>Bots</th><th>When</th><th>Countdown</th><th>Mode</th><th></th></tr></thead>
            <tbody id="tbodySchedule"><tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No scheduled</td></tr></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
          <div class="section-title" style="margin:0;">📜 Live Logs <span id="logFilterLabel" style="font-weight:400;color:var(--muted);font-size:12px;"></span></div>
          <div style="display:flex;gap:8px;">
            <button class="btn btn-outline btn-sm" onclick="clearLogFilter()">All</button>
            <button class="btn btn-outline btn-sm" onclick="refreshLogs()">↻</button>
          </div>
        </div>
        <div id="logPanel" class="log-panel"><div style="color:var(--muted)">Waiting for logs...</div></div>
      </div>
    </div>
  </div>
</div>

<div class="modal-overlay" id="shutdownModal">
  <div class="modal">
    <h3>🛑 Shutdown Server?</h3>
    <p style="font-size:13px;color:var(--muted);margin-bottom:8px;">Type <b>yes</b> to confirm.</p>
    <input type="text" id="shutdownConfirm" placeholder="Type yes"/>
    <div style="display:flex;gap:10px;margin-top:8px;">
      <button class="btn btn-danger" style="flex:1" onclick="confirmShutdown()">Shutdown</button>
      <button class="btn btn-outline" onclick="closeShutdownModal()">Cancel</button>
    </div>
  </div>
</div>

<script>
const API=location.origin;const $=id=>document.getElementById(id);
function applyTheme(light){
  document.documentElement.classList.toggle('light',light);
  localStorage.setItem('zcc-theme',light?'light':'dark');
  const t=$('themeToggle'); if(t)t.checked=light;
}
function toggleDayNight(){applyTheme($('themeToggle').checked)}
applyTheme(localStorage.getItem('zcc-theme')==='light');

let currentMode='individual',allMeetings={},allSchedules={},isLoggedIn=false,activeLogMeeting=null;

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
function updateSessionUI(s){const badge=$('sessionBadge'),st=$('sessionStatusText');isLoggedIn=!!s.logged_in;if(isLoggedIn){badge.className='session-badge logged-in';badge.textContent='🟢 Logged In';if(st){st.textContent=s.message||'Session active';st.style.color='#34d399'}}else{badge.className='session-badge logged-out';badge.textContent='🔴 No Session';if(st){st.textContent=s.message||'No session';st.style.color='#fca5a5'}}}

async function saveSession(){const raw=$('sessionJson').value.trim();if(!raw)return show('Paste session JSON','err');let data;try{data=JSON.parse(raw)}catch(e){return show('Invalid JSON','err')}if(!data.cookies)return show('Must contain cookies','err');try{show('Saving...','info');const r=await fetch(API+'/api/update-session',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const d=await r.json();if(r.ok){show('✅ Session saved!','ok');$('sessionEditBox').style.display='none';$('toggleSessionBtn').textContent='✏️ Update Session';$('sessionJson').value='';if($('sessionStatusText')){$('sessionStatusText').textContent='Session updated ✓';$('sessionStatusText').style.color='#34d399'}setTimeout(refresh,600)}else show(d.detail||'Failed','err')}catch(e){show(e.message,'err')}}

function renderLogs(logs){const panel=$('logPanel');if(!logs||!logs.length){panel.innerHTML='<div style="color:var(--muted)">No logs yet</div>';return}panel.innerHTML=logs.map(l=>{const cls=l.level==='ok'?'ok':l.level==='err'?'err':'info';return `<div class="log-line ${cls}"><span class="t">${l.time}</span><span class="m">[${l.meeting}]</span>${l.message}</div>`}).join('');panel.scrollTop=panel.scrollHeight}
async function refreshLogs(){try{const q=activeLogMeeting?`?meeting=${encodeURIComponent(activeLogMeeting)}&limit=150`:'?limit=120';const r=await fetch(API+'/api/logs'+q);const d=await r.json();renderLogs(d.logs||[]);$('logFilterLabel').textContent=activeLogMeeting?'• '+activeLogMeeting:'• All'}catch(e){}}
function selectMeetingLogs(meeting){activeLogMeeting=meeting;refreshLogs();show('Logs: '+meeting,'info')}
function clearLogFilter(){activeLogMeeting=null;refreshLogs()}

function renderActive(meetings){
  allMeetings=meetings;
  const search=($('searchMeeting').value||'').trim().toLowerCase();
  let filtered=Object.entries(meetings);
  if(search) filtered=filtered.filter(([m])=>m.toLowerCase().includes(search));
  if(!filtered.length){activeListMobile.innerHTML='<div class="empty">No meetings</div>';tbodyActive.innerHTML='<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No meetings</td></tr>';return}
  activeListMobile.innerHTML=filtered.map(([meeting,m])=>{
    const total=m.total_bots||0,done=m.completed_bots||0,type=m.name_type||'indian',mode=m.join_mode||'individual';
    const status=m.status||'running',startTime=m.started_at?new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'-';
    const sel=activeLogMeeting===meeting?'selected':'';
    return `<div class="meeting-card ${sel}" onclick="selectMeetingLogs('${meeting}')">
      <div class="mc-top"><div class="mc-id">${meeting}</div><div class="mc-bots">${done}/${total}</div></div>
      <div class="mc-meta">
        <span class="badge ${status==='completed'?'badge-completed':'badge-running'}">${status==='completed'?'Completed':'In Meeting'}</span>
        <span class="badge ${mode==='together'?'badge-together':'badge-slow'}">${mode==='together'?'Together':'Slow'}</span>
        <span class="badge badge-${type}">${type}</span>
      </div>
      <div class="mc-bottom"><span>${startTime}</span>
        <button class="btn btn-danger btn-sm" onclick="event.stopPropagation();killMeeting('${meeting}')">Kill</button>
      </div></div>`}).join('');
  let idx=0;
  tbodyActive.innerHTML=filtered.map(([meeting,m])=>{
    idx++;
    const total=m.total_bots||0,done=m.completed_bots||0,type=m.name_type||'indian',mode=m.join_mode||'individual';
    const status=m.status||'running',startTime=m.started_at?new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'-';
    const sel=activeLogMeeting===meeting?'selected':'';
    return `<tr class="${sel}" style="cursor:pointer" onclick="selectMeetingLogs('${meeting}')">
      <td>${idx}</td><td style="font-weight:600;color:#93c5fd">${meeting}</td>
      <td><strong style="color:#fbbf24">${done}/${total}</strong></td>
      <td><span class="badge ${status==='completed'?'badge-completed':'badge-running'}">${status==='completed'?'Completed':'In Meeting'}</span></td>
      <td>${startTime}</td>
      <td><span class="badge ${mode==='together'?'badge-together':'badge-slow'}">${mode==='together'?'Together':'Slow'}</span></td>
      <td><button class="btn btn-danger btn-sm" onclick="event.stopPropagation();killMeeting('${meeting}')">Kill</button></td></tr>`}).join('');
}

function renderSchedules(schedules){
  allSchedules=schedules;const entries=Object.entries(schedules);
  if(!entries.length){scheduleListMobile.innerHTML='<div class="empty">No scheduled</div>';tbodySchedule.innerHTML='<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">No scheduled</td></tr>';return}
  scheduleListMobile.innerHTML=entries.map(([sid,s])=>{
    const when=new Date(s.schedule_at).toLocaleString('en-IN',{timeZone:'Asia/Kolkata',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});
    return `<div class="meeting-card"><div class="mc-top"><div class="mc-id">${s.meeting_code}</div><div class="mc-bots">${s.bot_count}</div></div>
      <div class="mc-meta"><span class="badge ${s.join_mode==='together'?'badge-together':'badge-slow'}">${s.join_mode==='together'?'Together':'Slow'}</span>
      <span class="badge badge-${s.name_type||'indian'}">${s.name_type||'indian'}</span></div>
      <div class="mc-bottom"><div><div>${when}</div><div class="countdown" id="cd-m-${sid}">${formatCountdown(s.schedule_at)}</div></div>
      <button class="btn btn-danger btn-sm" onclick="deleteSchedule('${sid}')">Cancel</button></div></div>`}).join('');
  let idx=0;
  tbodySchedule.innerHTML=entries.map(([sid,s])=>{
    idx++;const when=new Date(s.schedule_at).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'});
    return `<tr><td>${idx}</td><td style="font-weight:600;color:#93c5fd">${s.meeting_code}</td>
      <td><strong style="color:#fbbf24">${s.bot_count}</strong></td><td>${when}</td>
      <td class="countdown" id="cd-d-${sid}">${formatCountdown(s.schedule_at)}</td>
      <td><span class="badge ${s.join_mode==='together'?'badge-together':'badge-slow'}">${s.join_mode==='together'?'Together':'Slow'}</span></td>
      <td><button class="btn btn-danger btn-sm" onclick="deleteSchedule('${sid}')">Cancel</button></td></tr>`}).join('');
}

function filterMeetings(){renderActive(allMeetings)}
async function refresh(){try{const r=await fetch(API+'/status');const d=await r.json();if(d.session)updateSessionUI(d.session);const connected=d.connected_workers_count||Object.keys(d.workers||{}).length;totalCap.textContent=(d.total_capacity||0)-(d.total_free_capacity||0);totalCapMax.textContent=d.total_capacity||0;renderActive(d.meetings||{});renderSchedules(d.schedules||{});if(d.recent_logs&&!activeLogMeeting)renderLogs(d.recent_logs);else await refreshLogs();show(`Refreshed • ${connected} worker(s)`,'ok')}catch(e){show(e.message||'Failed','err')}}
setInterval(()=>{Object.keys(allSchedules).forEach(sid=>{const t=formatCountdown(allSchedules[sid].schedule_at);const e1=document.getElementById('cd-m-'+sid),e2=document.getElementById('cd-d-'+sid);if(e1)e1.textContent=t;if(e2)e2.textContent=t})},1000);

async function handleStart(){
  if(!isLoggedIn) return show('Upload session JSON first','err');
  const meeting=meetingId.value.trim().replace(/\s/g,'');
  const pass=passcode.value; // keep "0" as valid
  const bots=parseInt(botCount.value)||10,dur=parseInt(duration.value)||120,type=nameType.value;
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

async function killMeeting(meeting){if(!confirm(`Kill ${meeting}?`))return;try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting})});const d=await r.json();if(r.ok){show(d.message||'Killed','ok');setTimeout(refresh,500)}else show(d.detail||'Failed','err')}catch(e){show(e.message,'err')}}
async function killBySearch(){const meeting=$('searchMeeting').value.trim().replace(/\s/g,'');if(!meeting)return show('Enter Meeting ID','err');await killMeeting(meeting)}
async function killAll(){if(!confirm('Kill ALL?'))return;try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});const d=await r.json();if(r.ok){show('All killed','ok');setTimeout(refresh,500)}else show(d.detail||'Failed','err')}catch(e){show(e.message,'err')}}
async function deleteSchedule(sid){if(!confirm('Cancel?'))return;try{const r=await fetch(API+'/api/schedule/'+sid,{method:'DELETE'});if(r.ok){show('Cancelled','ok');setTimeout(refresh,400)}else show('Failed','err')}catch(e){show(e.message,'err')}}
function openShutdownModal(){$('shutdownModal').style.display='flex';$('shutdownConfirm').value='';$('shutdownConfirm').focus()}
function closeShutdownModal(){$('shutdownModal').style.display='none'}
async function confirmShutdown(){const val=$('shutdownConfirm').value.trim().toLowerCase();if(val!=='yes'){alert('Type yes');return}try{show('Shutting down...','info');await fetch(API+'/api/shutdown',{method:'POST'});show('Shutdown sent','ok');closeShutdownModal()}catch(e){show('Shutting down...','ok');closeShutdownModal()}}

setInterval(refresh,5000);
setInterval(refreshLogs,3000);
refresh();
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(asgi_app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
