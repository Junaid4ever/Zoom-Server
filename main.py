# ============================================
# ZOOM BOT CENTRAL – KILL FIX + SESSION REFRESH
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
    if not tid or tid not in running_tasks:
        return
    task = running_tasks[tid]
    wid, bc, m = task.get("worker_id"), task.get("bot_count", 0), task.get("meeting_code")
    if wid in workers:
        workers[wid]["free_capacity"] = min(workers[wid]["max_capacity"], workers[wid].get("free_capacity", 0) + bc)
    if m in meeting_groups:
        g = meeting_groups[m]
        if tid in g.get("task_ids", []):
            g["task_ids"].remove(tid)
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
async def health():
    return {"ok": True}

@app.get("/session")
async def get_session():
    if not os.path.exists("zoom_session.json"):
        raise HTTPException(404, "Session not found")
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
    if not isinstance(data, dict) or "cookies" not in data:
        raise HTTPException(400, "Invalid JSON")
    with open("zoom_session.json", "w") as f:
        json.dump(data, f, indent=2)
    session_status.update({"logged_in": True, "message": "Session updated ✓", "last_checked": now_ist().isoformat()})
    add_log("-", "✅ Session JSON updated (will be used by next tasks)", "ok")
    # Notify all workers to refresh session on next task
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("session_updated", {"message": "new session available"}, to=info["sid"])
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
    if not os.path.exists("zoom_session.json"):
        raise HTTPException(400, "No session file")
    if req.bot_count < 1:
        raise HTTPException(400, "bot_count >= 1")
    meeting = req.meeting_code.strip().replace(" ", "")
    if not meeting:
        raise HTTPException(400, "meeting required")
    passcode = "" if req.passcode is None else str(req.passcode)
    remaining, assigned, name_offset = req.bot_count, [], 0
    connected = {w: i for w, i in workers.items() if i.get("sid")}
    for wid, info in sorted(connected.items(), key=lambda x: x[1].get("free_capacity", 0), reverse=True):
        if remaining <= 0:
            break
        free = int(info.get("free_capacity", 0))
        if free <= 0:
            continue
        give = min(free, remaining)
        task_id = str(uuid.uuid4())[:8]
        custom_slice = None
        if req.custom_names and req.name_type == "custom":
            custom_slice = req.custom_names[name_offset:name_offset + give]
            name_offset += give
        payload = {
            "task_id": task_id, "meeting_code": meeting, "passcode": passcode, "bot_count": give,
            "duration_minutes": req.duration_minutes, "name_type": req.name_type or "indian",
            "custom_names": custom_slice, "join_mode": req.join_mode or "individual"
        }
        await sio.emit("new_task", payload, to=info["sid"])
        running_tasks[task_id] = {
            "task_id": task_id, "meeting_code": meeting, "bot_count": give, "worker_id": wid,
            "name_type": payload["name_type"], "duration_minutes": req.duration_minutes,
            "started_at": now_ist().isoformat(), "join_mode": req.join_mode or "individual"
        }
        if meeting not in meeting_groups:
            meeting_groups[meeting] = {
                "task_ids": [], "total_bots": 0, "completed_bots": 0,
                "name_type": payload["name_type"], "join_mode": req.join_mode or "individual",
                "started_at": now_ist().isoformat(), "status": "running"
            }
        meeting_groups[meeting]["task_ids"].append(task_id)
        meeting_groups[meeting]["total_bots"] += give
        meeting_groups[meeting]["status"] = "running"
        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give, "task_id": task_id})
        remaining -= give
    if not assigned:
        raise HTTPException(503, "No free workers")
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
    if st <= now_ist():
        raise HTTPException(400, "Must be future")
    sid = str(uuid.uuid4())[:8]
    scheduled_tasks[sid] = {
        "schedule_id": sid, "meeting_code": req.meeting_code.strip().replace(" ", ""),
        "passcode": "" if req.passcode is None else str(req.passcode), "bot_count": req.bot_count,
        "duration_minutes": req.duration_minutes, "name_type": req.name_type or "indian",
        "custom_names": req.custom_names, "join_mode": req.join_mode or "individual",
        "schedule_at": st.isoformat(), "created_at": now_ist().isoformat()
    }
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
    """Kill by meeting: broadcast to ALL workers so no bot is left behind."""
    if req and req.meeting_code:
        meeting = req.meeting_code.strip().replace(" ", "")
        # 1) Broadcast meeting-level kill to EVERY connected worker
        for wid, info in list(workers.items()):
            if info.get("sid"):
                await sio.emit("terminate_meeting", {"meeting_code": meeting}, to=info["sid"])
        # 2) Also send per-task terminate for bookkeeping
        for tid in [t for t, x in list(running_tasks.items()) if x.get("meeting_code") == meeting]:
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid, "meeting_code": meeting}, to=workers[wid]["sid"])
            if wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
            del running_tasks[tid]
        meeting_groups.pop(meeting, None)
        add_log(meeting, "🛑 HARD KILL sent to all workers", "err")
        return {"success": True, "message": f"Meeting {meeting} fully terminated"}

    # Kill ALL
    for wid, info in list(workers.items()):
        if info.get("sid"):
            await sio.emit("terminate_all", {}, to=info["sid"])
    for tid in list(running_tasks.keys()):
        wid = running_tasks[tid].get("worker_id")
        if wid in workers and workers[wid].get("sid"):
            await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
        if wid in workers:
            workers[wid]["free_capacity"] = min(
                workers[wid]["max_capacity"],
                workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
            )
    running_tasks.clear()
    meeting_groups.clear()
    add_log("-", "🛑 ALL meetings hard-killed", "err")
    return {"success": True, "message": "All terminated"}

@app.post("/api/shutdown")
async def shutdown_server():
    add_log("-", "🛑 SHUTDOWN", "err")
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("shutdown", {}, to=info["sid"])
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
                if st.tzinfo is None:
                    st = st.replace(tzinfo=IST)
                if now >= st:
                    to_run.append(sid)
            except Exception:
                continue
        for sid in to_run:
            info = scheduled_tasks.pop(sid)
            try:
                await start_bots(StartBotRequest(
                    meeting_code=info["meeting_code"], passcode=info["passcode"],
                    bot_count=info["bot_count"], duration_minutes=info["duration_minutes"],
                    name_type=info["name_type"], custom_names=info["custom_names"],
                    join_mode=info["join_mode"]
                ))
            except Exception as e:
                add_log(info.get("meeting_code", "-"), f"Schedule fail: {e}", "err")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_checker())
    if os.path.exists("zoom_session.json"):
        session_status.update({"logged_in": True, "message": "Session present", "last_checked": now_ist().isoformat()})
    add_log("-", "✅ Server started", "ok")

# Keep your existing DASHBOARD_HTML from previous message here.
# If you need the HTML block again, say "HTML do" — backend kill/session is the critical fix above.

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"/>
<title>Zoom Command Center</title>
<style>
:root{--bg:#070b14;--bg2:#0b1220;--surface:rgba(15,23,42,.78);--input:#0b1424;--border:rgba(148,163,184,.16);--text:#eef5ff;--muted:#91a4bd;--primary:#5b8cff;--primary2:#7c5cff;--success:#24d6a0;--danger:#ff5d73;--warning:#ffbf5a;--cyan:#37d7ff;--glow:rgba(91,140,255,.2)}
*{margin:0;padding:0;box-sizing:border-box}
body{min-height:100vh;padding:16px;color:var(--text);font-family:system-ui,sans-serif;background:linear-gradient(145deg,var(--bg),var(--bg2))}
.container{max-width:1400px;margin:auto}
.header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;padding:14px;margin-bottom:14px;border:1px solid var(--border);border-radius:16px;background:var(--surface)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:16px;margin-bottom:14px}
.grid{display:grid;grid-template-columns:1fr;gap:14px}@media(min-width:1000px){.grid{grid-template-columns:380px 1fr}}
input,select,textarea{width:100%;padding:11px;margin:6px 0 12px;background:var(--input);border:1px solid var(--border);border-radius:10px;color:var(--text)}
.btn{padding:10px 14px;border:none;border-radius:10px;font-weight:700;cursor:pointer}
.btn-primary{background:linear-gradient(135deg,var(--primary),var(--primary2));color:#fff}
.btn-danger{background:rgba(255,93,115,.15);color:var(--danger);border:1px solid rgba(255,93,115,.3)}
.btn-outline{background:var(--input);color:var(--muted);border:1px solid var(--border)}
.btn-sm{padding:6px 10px;font-size:12px}
.mode-toggle{display:flex;gap:6px;margin-bottom:12px}
.mode-btn{flex:1;padding:10px;text-align:center;border-radius:10px;background:var(--input);color:var(--muted);cursor:pointer;font-weight:700}
.mode-btn.active{background:linear-gradient(135deg,var(--primary),var(--primary2));color:#fff}
.meeting-card{background:var(--input);border:1px solid var(--border);border-radius:12px;padding:12px;margin-bottom:8px;cursor:pointer}
.meeting-card.selected{border-color:var(--primary)}
.badge{display:inline-block;padding:3px 8px;border-radius:999px;font-size:11px;font-weight:700;margin-right:4px}
.badge-running{background:rgba(36,214,160,.15);color:var(--success)}
.badge-completed{background:rgba(148,163,184,.15);color:var(--muted)}
.log-panel{background:#050912;border:1px solid var(--border);border-radius:12px;padding:12px;max-height:300px;overflow:auto;font-family:monospace;font-size:11px;line-height:1.6}
.log-line .t{color:#64748b;margin-right:6px}.log-line .m{color:var(--cyan);margin-right:6px}
.log-line.ok{color:var(--success)}.log-line.err{color:var(--danger)}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.session-badge{padding:6px 12px;border-radius:999px;font-size:12px;font-weight:700}
.logged-in{color:var(--success)}.logged-out{color:var(--danger)}
</style></head>
<body>
<div class="container">
  <div class="header">
    <h2>⚡ Zoom Command Center</h2>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <span id="sessionBadge" class="session-badge logged-out">Checking...</span>
      <span id="capInfo" style="color:var(--muted);font-size:13px">0/0</span>
      <button class="btn btn-outline btn-sm" onclick="refresh()">↻</button>
      <button class="btn btn-danger btn-sm" onclick="if(confirm('Shutdown?')&&prompt('Type yes')==='yes')fetch(API+'/api/shutdown',{method:'POST'})">🛑</button>
    </div>
  </div>
  <div class="card">
    <b>🔑 Session</b>
    <button class="btn btn-outline btn-sm" style="float:right" onclick="sessBox.style.display=sessBox.style.display==='none'?'block':'none'">Update</button>
    <div id="sessStatus" style="color:var(--muted);font-size:13px;margin-top:8px">—</div>
    <div id="sessBox" style="display:none;margin-top:10px">
      <textarea id="sessionJson" rows="6" placeholder='{"cookies":[...]}'></textarea>
      <button class="btn btn-primary" onclick="saveSession()">💾 Save Session</button>
    </div>
  </div>
  <div class="grid">
    <div class="card">
      <div class="mode-toggle">
        <div class="mode-btn active" id="modeSlow" onclick="setMode('individual')">🐢 Slow</div>
        <div class="mode-btn" id="modeTogether" onclick="setMode('together')">⚡ Together</div>
      </div>
      <form onsubmit="handleStart();return false">
        <label style="font-size:12px;color:var(--muted)">Meeting ID</label>
        <input id="meetingId" required/>
        <label style="font-size:12px;color:var(--muted)">Passcode (blank=none, 0=ok)</label>
        <input id="passcode"/>
        <div class="form-row">
          <div><label style="font-size:12px;color:var(--muted)">Bots</label><input type="number" id="botCount" value="20" min="1"/></div>
          <div><label style="font-size:12px;color:var(--muted)">Names</label>
            <select id="nameType"><option value="indian">Indian</option><option value="english">English</option><option value="custom">Custom</option></select>
          </div>
        </div>
        <textarea id="customNames" rows="3" placeholder="Custom names (one per line)" style="display:none"></textarea>
        <label style="font-size:12px;color:var(--muted)">Duration (min)</label>
        <input type="number" id="duration" value="120"/>
        <div style="display:flex;gap:8px;margin-top:8px">
          <button class="btn btn-primary" style="flex:1" type="submit">▶ Start</button>
          <button class="btn btn-danger" type="button" onclick="killAll()">Kill All</button>
        </div>
      </form>
      <div id="msg" style="margin-top:10px;font-size:12px;color:var(--muted)">Ready</div>
    </div>
    <div>
      <div class="card">
        <b>🟢 Meetings</b> <span style="color:var(--muted);font-size:12px">(click → logs)</span>
        <div id="meetList" style="margin-top:10px"><div style="color:var(--muted)">None</div></div>
      </div>
      <div class="card">
        <b>📜 Logs</b> <span id="logFilter" style="color:var(--muted);font-size:12px"></span>
        <button class="btn btn-outline btn-sm" style="float:right" onclick="activeLog=null;renderLogs([])">Clear</button>
        <div id="logPanel" class="log-panel" style="margin-top:10px">Click a meeting</div>
      </div>
    </div>
  </div>
</div>
<script>
const API=location.origin; let currentMode='individual', activeLog=null, isLoggedIn=false;
nameType.onchange=()=>{customNames.style.display=nameType.value==='custom'?'block':'none'};
function setMode(m){currentMode=m;modeSlow.className='mode-btn'+(m==='individual'?' active':'');modeTogether.className='mode-btn'+(m==='together'?' active':'')}
function show(t,c){msg.innerHTML=`<span style="color:${c==='ok'?'var(--success)':c==='err'?'var(--danger)':'var(--primary)'}">${t}</span>`}
async function saveSession(){try{const data=JSON.parse(sessionJson.value);const r=await fetch(API+'/api/update-session',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});if(r.ok){show('✅ Session saved','ok');sessBox.style.display='none';sessionJson.value='';refresh()}else show('Fail','err')}catch(e){show(e.message,'err')}}
function renderLogs(logs){if(!activeLog){logPanel.innerHTML='Click a meeting';logFilter.textContent='';return}if(!logs.length){logPanel.innerHTML='No logs';return}logPanel.innerHTML=logs.map(l=>`<div class="log-line ${l.level||'info'}"><span class="t">${l.time}</span><span class="m">[${l.meeting}]</span>${l.message}</div>`).join('');logPanel.scrollTop=99999;logFilter.textContent='• '+activeLog}
async function loadLogs(){if(!activeLog)return;try{const r=await fetch(API+'/api/logs?meeting='+encodeURIComponent(activeLog)+'&limit=200');const d=await r.json();renderLogs(d.logs||[])}catch(e){}}
function selectLog(m){activeLog=m;loadLogs()}
async function refresh(){try{const r=await fetch(API+'/status');const d=await r.json();isLoggedIn=!!(d.session&&d.session.logged_in);sessionBadge.className='session-badge '+(isLoggedIn?'logged-in':'logged-out');sessionBadge.textContent=isLoggedIn?'🟢 Logged In':'🔴 No Session';sessStatus.textContent=(d.session&&d.session.message)||'—';const a=(d.total_capacity||0)-(d.total_free_capacity||0);capInfo.textContent=a+'/'+(d.total_capacity||0);const ms=d.meetings||{};const keys=Object.keys(ms);meetList.innerHTML=keys.length?keys.map(m=>{const x=ms[m];return`<div class="meeting-card ${activeLog===m?'selected':''}" onclick="selectLog('${m}')"><b>${m}</b> · ${x.completed_bots||0}/${x.total_bots||0} <span class="badge ${x.status==='completed'?'badge-completed':'badge-running'}">${x.status||'running'}</span><button class="btn btn-danger btn-sm" style="float:right" onclick="event.stopPropagation();killMeeting('${m}')">Kill</button></div>`}).join(''):'<div style="color:var(--muted)">None</div>';await loadLogs()}catch(e){show(e.message,'err')}}
async function handleStart(){if(!isLoggedIn)return show('Upload session','err');const meeting=meetingId.value.trim().replace(/\s/g,'');if(!meeting)return show('Meeting required','err');const bots=+botCount.value||10;let custom=null;if(nameType.value==='custom'){custom=customNames.value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);if(custom.length<bots)return show('Need more names','err')}show('Starting...','info');try{const r=await fetch(API+'/api/start-bots',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:meeting,passcode:passcode.value,bot_count:bots,duration_minutes:+duration.value||120,name_type:nameType.value,custom_names:custom,join_mode:currentMode})});const d=await r.json();show(r.ok?(d.message||'OK'):(d.detail||'Fail'),r.ok?'ok':'err');setTimeout(refresh,500)}catch(e){show(e.message,'err')}}
async function killMeeting(m){if(!confirm('HARD KILL '+m+'?'))return;show('Killing...','info');try{const r=await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:m})});const d=await r.json();show(d.message||'Killed','ok');setTimeout(refresh,500)}catch(e){show(e.message,'err')}}
async function killAll(){if(!confirm('Kill ALL?'))return;await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});show('All killed','ok');setTimeout(refresh,500)}
setInterval(refresh,4000);setInterval(loadLogs,2000);refresh();
</script>
</body></html>"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(asgi_app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
