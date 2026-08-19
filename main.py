# ============================================
# ZOOM BOT CENTRAL - Railway (FULL)
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

# Store workers and tasks
workers = {}
running_tasks = {}

class StartBotRequest(BaseModel):
    meeting_code: str
    passcode: str = ""
    bot_count: int = 10
    duration_minutes: int = 120
    name_type: str = "indian"
    custom_names: Optional[List[str]] = None

class TerminateRequest(BaseModel):
    meeting_code: Optional[str] = None

# ----- SOCKET.IO EVENTS -----
@sio.event
async def connect(sid, environ):
    print(f"[SIO] Connected: {sid}")

@sio.event
async def disconnect(sid):
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            del workers[wid]
            print(f"[SIO] Worker removed: {wid}")
            break

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
        # Update capacity for the worker who completed this task
        wid = running_tasks[tid].get("worker_id")
        if wid and wid in workers:
            workers[wid]["free_capacity"] = min(
                workers[wid]["max_capacity"],
                workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
            )
        del running_tasks[tid]
        print(f"[SIO] Task completed: {tid}")

# ----- API ENDPOINTS -----
@app.get("/health")
async def health():
    return {"ok": True, "workers": len(workers)}

@app.get("/status")
@app.get("/api/status")
async def status():
    total_free = sum(w.get("free_capacity", 0) for w in workers.values())
    # Calculate remaining time for each task
    now = datetime.now()
    for tid, task in running_tasks.items():
        if "started_at" in task:
            started = datetime.fromisoformat(task["started_at"])
            elapsed = (now - started).total_seconds() / 60
            task["elapsed_minutes"] = round(elapsed, 1)
            task["remaining_minutes"] = max(0, round(task.get("duration_minutes", 120) - elapsed, 1))
            if task["remaining_minutes"] <= 0:
                # Auto-complete task if time is up
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
        give = min(free, remaining)
        task_id = str(uuid.uuid4())[:8]
        payload = {
            "task_id": task_id,
            "meeting_code": meeting,
            "passcode": req.passcode or "",
            "bot_count": give,
            "duration_minutes": req.duration_minutes,
            "name_type": req.name_type or "indian",
            "custom_names": req.custom_names
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
            "remaining_minutes": req.duration_minutes
        }
        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give, "task_id": task_id})
        remaining -= give
        print(f"[API] Task {task_id} → {wid} ({give} bots)")

    if not assigned:
        raise HTTPException(503, "No free capacity. Start Colab worker first.")

    return {
        "success": True,
        "message": f"Started {req.bot_count - remaining} bots",
        "assigned": assigned,
        "remaining_unassigned": remaining
    }

@app.post("/api/terminate")
@app.post("/api/kill-meeting")
async def terminate(req: Optional[TerminateRequest] = None):
    meeting = req.meeting_code if req else None
    if meeting:
        # Kill specific meeting
        await sio.emit("terminate", {"meeting_code": meeting})
        # Remove from running_tasks
        for tid in list(running_tasks.keys()):
            if running_tasks[tid].get("meeting_code") == meeting:
                # Restore capacity for the worker
                wid = running_tasks[tid].get("worker_id")
                if wid and wid in workers:
                    workers[wid]["free_capacity"] = min(
                        workers[wid]["max_capacity"],
                        workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                    )
                del running_tasks[tid]
        print(f"[API] Terminate → {meeting}")
    else:
        # Kill ALL (only if explicitly called)
        await sio.emit("terminate", {"meeting_code": None})
        for tid in list(running_tasks.keys()):
            wid = running_tasks[tid].get("worker_id")
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
        running_tasks.clear()
        print(f"[API] Terminate → ALL")
    
    return {"success": True, "message": "Terminate sent"}

# ============================================
# REDESIGNED DASHBOARD - "Junaid Members Panel (Zoom)"
# ============================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Junaid Members Panel (Zoom)</title>
<style>
/* ----- RESET & BASE ----- */
*{margin:0;padding:0;box-sizing:border-box}
body{
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  background: #0a0e17;
  color: #e6edf3;
  min-height: 100vh;
  padding: 20px;
}
.container{max-width:1400px;margin:0 auto}

/* ----- HEADER ----- */
.header{
  display:flex;justify-content:space-between;align-items:center;
  padding:16px 24px;
  background:linear-gradient(135deg,#0d1b2a,#1b2d45);
  border-radius:16px;
  border:1px solid #1e3a5f;
  margin-bottom:24px;
  flex-wrap:wrap;gap:12px;
}
.header h1{
  font-size:26px;font-weight:700;
  background:linear-gradient(90deg,#58a6ff,#79c0ff);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  letter-spacing:0.5px;
}
.header h1 span{font-weight:300;color:#8b949e;-webkit-text-fill-color:#8b949e}
.header .status-badge{
  display:flex;align-items:center;gap:8px;
  background:#0d1117;padding:6px 16px;border-radius:20px;
  border:1px solid #238636;font-size:13px;
}
.header .status-badge .dot{
  width:10px;height:10px;border-radius:50%;
  background:#3fb950;animation:pulse 2s infinite;
}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.4}}

/* ----- MAIN LAYOUT: LEFT (Input) + RIGHT (Workers) ----- */
.main-grid{
  display:grid;grid-template-columns:1fr 320px;gap:20px;
}
@media(max-width:900px){.main-grid{grid-template-columns:1fr}}

/* ----- CARDS ----- */
.card{
  background:#0d1117;border:1px solid #21262d;border-radius:12px;
  padding:20px;margin-bottom:16px;
}
.card-title{
  font-size:14px;font-weight:600;color:#8b949e;
  text-transform:uppercase;letter-spacing:0.5px;margin-bottom:14px;
}

/* ----- LEFT SIDE: INPUT FORM ----- */
.form-grid{
  display:grid;grid-template-columns:1fr 1fr;gap:12px;
}
@media(max-width:600px){.form-grid{grid-template-columns:1fr}}

.form-group{display:flex;flex-direction:column;gap:4px}
.form-group label{font-size:12px;color:#8b949e;font-weight:500}
.form-group input,.form-group select,.form-group textarea{
  padding:10px 12px;background:#0d1117;border:1px solid #30363d;
  border-radius:8px;color:#e6edf3;font-size:14px;
  transition:border-color 0.2s;
}
.form-group input:focus,.form-group select:focus,.form-group textarea:focus{
  outline:none;border-color:#58a6ff;box-shadow:0 0 0 3px rgba(88,166,255,0.15);
}
.form-group textarea{resize:vertical;font-family:monospace;font-size:13px}

/* ----- CUSTOM NAMES BOX ----- */
#customBox{
  display:none;margin-top:12px;padding:14px;
  background:#0d1117;border:1px solid #30363d;border-radius:8px;
}
#customBox .name-status{font-size:12px;color:#8b949e;margin-top:6px}
#customBox .name-status .ok{color:#3fb950}
#customBox .name-status .err{color:#f85149}

/* ----- BUTTONS ----- */
.btn{
  padding:10px 22px;border:none;border-radius:8px;
  font-weight:600;font-size:14px;cursor:pointer;
  transition:all 0.2s;display:inline-flex;align-items:center;gap:6px;
}
.btn-primary{background:#238636;color:#fff}
.btn-primary:hover{background:#2ea043;transform:translateY(-1px)}
.btn-danger{background:#da3633;color:#fff}
.btn-danger:hover{background:#f85149;transform:translateY(-1px)}
.btn-outline{background:transparent;color:#8b949e;border:1px solid #30363d}
.btn-outline:hover{background:#21262d;color:#e6edf3}
.btn-sm{padding:4px 12px;font-size:12px}
.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}

/* ----- RIGHT SIDE: WORKERS PANEL ----- */
.workers-panel{
  background:#0d1117;border:1px solid #21262d;border-radius:12px;
  padding:16px;height:fit-content;position:sticky;top:20px;
}
.workers-panel .panel-title{
  font-size:14px;font-weight:600;color:#8b949e;
  text-transform:uppercase;letter-spacing:0.5px;
  margin-bottom:12px;display:flex;justify-content:space-between;
}
.workers-panel .panel-title span{color:#58a6ff}
.workers-scroll{
  max-height:500px;overflow-y:auto;padding-right:4px;
}
.workers-scroll::-webkit-scrollbar{width:4px}
.workers-scroll::-webkit-scrollbar-track{background:#0d1117}
.workers-scroll::-webkit-scrollbar-thumb{background:#30363d;border-radius:4px}

.worker-item{
  display:flex;justify-content:space-between;align-items:center;
  padding:10px 12px;background:#0d1117;border:1px solid #21262d;
  border-radius:8px;margin-bottom:6px;
  font-size:13px;font-family:monospace;
}
.worker-item .name{color:#58a6ff}
.worker-item .cap{color:#8b949e}
.worker-item .cap .free{color:#3fb950}
.worker-item .online{color:#3fb950;font-size:10px}

/* ----- ACTIVE MEETINGS TABLE ----- */
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #21262d}
th{color:#8b949e;font-weight:500;font-size:12px;text-transform:uppercase;letter-spacing:0.3px}
tr:hover td{background:#161b22}
.meeting-code{font-weight:600;color:#58a6ff;font-family:monospace}
.badge{
  display:inline-block;padding:2px 10px;border-radius:12px;
  font-size:11px;font-weight:500;
}
.badge-indian{background:#1a3a2a;color:#3fb950}
.badge-english{background:#1a2a4a;color:#58a6ff}
.badge-custom{background:#3a2a1a;color:#d29922}

.timer-bar{
  display:flex;align-items:center;gap:10px;
}
.timer-bar .progress{
  flex:1;height:4px;background:#21262d;border-radius:4px;overflow:hidden;
}
.timer-bar .progress .fill{
  height:100%;border-radius:4px;transition:width 1s linear;
}
.timer-bar .time-text{
  font-family:monospace;font-size:13px;min-width:50px;text-align:right;
  color:#8b949e;
}
.timer-bar .time-text.warning{color:#d29922}
.timer-bar .time-text.danger{color:#f85149}

/* ----- STATUS / LOG ----- */
.log{
  margin-top:12px;padding:10px 14px;background:#0d1117;
  border:1px solid #21262d;border-radius:8px;
  font-family:monospace;font-size:13px;min-height:40px;
  color:#8b949e;
}
.log .ok{color:#3fb950}
.log .err{color:#f85149}
.log .info{color:#58a6ff}

/* ----- EMPTY STATE ----- */
.empty{text-align:center;color:#8b949e;padding:30px 0;font-size:14px}

/* ----- RESPONSIVE TWEAKS ----- */
@media(max-width:600px){
  .header h1{font-size:18px}
  .workers-panel{position:static}
  .btn{padding:8px 14px;font-size:13px}
}
</style>
</head>
<body>
<div class="container">

  <!-- HEADER -->
  <div class="header">
    <h1>🚀 Junaid <span>Members Panel (Zoom)</span></h1>
    <div class="status-badge">
      <span class="dot"></span>
      <span id="statusText">Connected</span>
      <span style="color:#8b949e;margin-left:8px">|</span>
      <span id="liveTime" style="font-family:monospace;font-size:12px;color:#8b949e"></span>
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
          <div class="form-group">
            <label>Meeting ID</label>
            <input id="meetingId" placeholder="5415403058"/>
          </div>
          <div class="form-group">
            <label>Passcode</label>
            <input id="passcode" placeholder="optional"/>
          </div>
          <div class="form-group">
            <label>Bots</label>
            <input type="number" id="botCount" value="10" min="1" max="500" oninput="updCount()"/>
          </div>
          <div class="form-group">
            <label>Duration (min)</label>
            <input type="number" id="duration" value="120" min="1"/>
          </div>
          <div class="form-group" style="grid-column:1/-1">
            <label>Name Type</label>
            <select id="nameType" onchange="toggleCustom()">
              <option value="indian">🇮🇳 Indian (Natural)</option>
              <option value="english">🇺🇸 English</option>
              <option value="custom">✏️ Custom Names</option>
            </select>
          </div>
        </div>

        <!-- Custom Names Box -->
        <div id="customBox">
          <label style="font-size:12px;color:#8b949e">Custom names (one per line)</label>
          <textarea id="customNames" rows="4" placeholder="Rahul Sharma&#10;Arjun Singh&#10;Priya Patel"></textarea>
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

      <!-- ACTIVE MEETINGS CARD -->
      <div class="card">
        <div class="card-title" style="display:flex;justify-content:space-between">
          <span>📋 Active Meetings</span>
          <span id="taskCount" style="color:#8b949e;font-weight:400;text-transform:none">0 running</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Meeting</th>
                <th>Bots</th>
                <th>Type</th>
                <th>Started</th>
                <th>Time Left</th>
                <th style="text-align:center">Action</th>
              </tr>
            </thead>
            <tbody id="tbody">
              <tr><td colspan="6" class="empty">No active meetings</td></tr>
            </tbody>
          </table>
        </div>
      </div>

    </div>

    <!-- RIGHT COLUMN: WORKERS -->
    <div class="workers-panel">
      <div class="panel-title">
        <span>🖥️ Connected Workers</span>
        <span id="workerCount">0</span>
      </div>
      <div class="workers-scroll" id="wlist">
        <div class="empty" style="padding:20px 0">No workers connected</div>
      </div>
      <div style="margin-top:12px;padding-top:12px;border-top:1px solid #21262d;font-size:12px;color:#8b949e">
        Total Capacity: <strong id="totalCap">0</strong> &nbsp;|&nbsp; Free: <strong id="freeCap">0</strong>
      </div>
    </div>

  </div>
</div>

<script>
// ===== CONFIG =====
const API = location.origin;

// ===== DOM REFS =====
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
const workerCount = $('workerCount');
const taskCount = $('taskCount');
const statusText = $('statusText');
const liveTime = $('liveTime');

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

// ===== API CALLS =====
async function refresh(){
  try{
    const r = await fetch(API+'/status');
    const d = await r.json();
    const workers = d.workers || {};
    const tasks = d.running_tasks || {};

    // Update stats
    let total=0, free=0;
    Object.values(workers).forEach(w=>{
      total += w.max_capacity || 0;
      free += w.free_capacity || 0;
    });
    totalCap.textContent = total;
    freeCap.textContent = free;
    workerCount.textContent = Object.keys(workers).length;
    taskCount.textContent = Object.keys(tasks).length + ' running';

    // Workers list
    const wKeys = Object.keys(workers);
    if(!wKeys.length){
      wlist.innerHTML = '<div class="empty" style="padding:20px 0">No workers connected</div>';
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
      tbody.innerHTML = '<tr><td colspan="6" class="empty">No active meetings</td></tr>';
    } else {
      tbody.innerHTML = tKeys.map(tid => {
        const t = tasks[tid];
        const meeting = t.meeting_code || 'N/A';
        const bots = t.bot_count || 0;
        const type = t.name_type || 'indian';
        const started = t.started_at ? new Date(t.started_at).toLocaleTimeString() : '-';
        const remaining = t.remaining_minutes !== undefined ? t.remaining_minutes : t.duration_minutes || 120;
        const totalDur = t.duration_minutes || 120;
        const pct = totalDur > 0 ? ((totalDur - Math.max(0, remaining)) / totalDur * 100) : 0;
        const pctClamped = Math.min(100, Math.max(0, pct));
        const warn = remaining < 5 ? 'danger' : remaining < 15 ? 'warning' : '';
        const typeBadge = type === 'indian' ? 'indian' : type === 'english' ? 'english' : 'custom';
        return `<tr>
          <td class="meeting-code">${meeting}</td>
          <td>${bots}</td>
          <td><span class="badge badge-${typeBadge}">${type}</span></td>
          <td>${started}</td>
          <td>
            <div class="timer-bar">
              <div class="progress">
                <div class="fill" style="width:${pctClamped}%;background:${remaining < 5 ? '#f85149' : remaining < 15 ? '#d29922' : '#3fb950'}"></div>
              </div>
              <span class="time-text ${warn}">${remaining > 0 ? Math.ceil(remaining)+'m' : '0m'}</span>
            </div>
          </td>
          <td style="text-align:center">
            <button class="btn btn-danger btn-sm" onclick="killMeeting('${meeting}')">✕ Kill</button>
          </td>
        </tr>`;
      }).join('');
    }

    // Update live time
    liveTime.textContent = new Date().toLocaleTimeString();
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
        duration_minutes: dur, name_type: type, custom_names: custom
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

// ===== KILL MEETING =====
async function killMeeting(code){
  if(!confirm(`Kill meeting ${code}?`)) return;
  try{
    const r = await fetch(API+'/api/terminate', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({meeting_code: code})
    });
    const d = await r.json();
    if(r.ok){
      show(`✅ Kill sent for ${code}`, 'ok');
      setTimeout(refresh, 1000);
    } else {
      show(d.detail || 'Kill failed', 'err');
    }
  } catch(e){ show(e.message, 'err'); }
}

// ===== AUTO REFRESH (every 5s) =====
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
