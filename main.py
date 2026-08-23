# ============================================
# ZOOM BOT CENTRAL – RECONNECT + CUMULATIVE + IST
# ============================================
import os
import uuid
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import socketio

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def ist_str(dt=None):
    if dt is None:
        dt = now_ist()
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except:
            return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).strftime("%H:%M:%S")

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
running_tasks = {}          # task_id -> task info
meeting_groups = {}         # meeting_code -> list of task_ids (for cumulative)

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
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            workers[wid]["sid"] = None
            workers[wid]["last_seen"] = now_ist().isoformat()
            print(f"[SIO] Worker {wid} offline (tasks preserved)")
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
        print(f"[SIO] Worker {wid} reconnected | free={workers[wid]['free_capacity']}")
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
        workers[wid]["last_seen"] = now_ist().isoformat()

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
        meeting = running_tasks[tid].get("meeting_code")
        if meeting and meeting in meeting_groups and tid in meeting_groups[meeting]:
            meeting_groups[meeting].remove(tid)
            if not meeting_groups[meeting]:
                del meeting_groups[meeting]
        del running_tasks[tid]
        print(f"[SIO] Task completed: {tid}")

@app.get("/health")
async def health():
    return {"ok": True, "workers": len(workers), "time": now_ist().isoformat()}

@app.get("/status")
@app.get("/api/status")
async def status():
    total_free = sum(w.get("free_capacity", 0) for w in workers.values())
    now = now_ist()

    # Build cumulative view by meeting
    meetings = {}
    for tid, task in list(running_tasks.items()):
        meeting = task.get("meeting_code", "unknown")
        if meeting not in meetings:
            meetings[meeting] = {
                "meeting_code": meeting,
                "total_bots": 0,
                "tasks": [],
                "name_type": task.get("name_type", "indian"),
                "started_at": task.get("started_at"),
                "duration_minutes": task.get("duration_minutes", 120),
                "join_mode": task.get("join_mode", "individual")
            }
        meetings[meeting]["total_bots"] += task.get("bot_count", 0)
        meetings[meeting]["tasks"].append(tid)

        # Update remaining time
        if "started_at" in task:
            try:
                started = datetime.fromisoformat(task["started_at"])
                if started.tzinfo is None:
                    started = started.replace(tzinfo=IST)
                elapsed = (now - started).total_seconds() / 60
                task["elapsed_minutes"] = round(elapsed, 1)
                task["remaining_minutes"] = max(0, round(task.get("duration_minutes", 120) - elapsed, 1))
            except:
                task["remaining_minutes"] = task.get("duration_minutes", 120)

        # Latest start time for the group
        if task.get("started_at") and (meetings[meeting]["started_at"] is None or task["started_at"] > meetings[meeting]["started_at"]):
            meetings[meeting]["started_at"] = task["started_at"]
            meetings[meeting]["duration_minutes"] = task.get("duration_minutes", 120)

    return {
        "workers": workers,
        "running_tasks": running_tasks,
        "meetings": meetings,               # cumulative view
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
            "started_at": now_ist().isoformat(),
            "remaining_minutes": req.duration_minutes,
            "join_mode": req.join_mode or "individual"
        }

        # Track in meeting group
        if meeting not in meeting_groups:
            meeting_groups[meeting] = []
        meeting_groups[meeting].append(task_id)

        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give, "task_id": task_id})
        remaining -= give
        print(f"[API] Task {task_id} → {wid} ({give} bots) | Meeting: {meeting}")

    if not assigned:
        raise HTTPException(503, "No free capacity or no connected workers.")

    return {
        "success": True,
        "message": f"Started {req.bot_count - remaining} bots for {meeting}",
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
        if wid and wid in workers:
            workers[wid]["free_capacity"] = min(
                workers[wid]["max_capacity"],
                workers[wid].get("free_capacity", 0) + running_tasks[task_id].get("bot_count", 0)
            )
        if meeting and meeting in meeting_groups and task_id in meeting_groups[meeting]:
            meeting_groups[meeting].remove(task_id)
            if not meeting_groups[meeting]:
                del meeting_groups[meeting]
        del running_tasks[task_id]
        print(f"[API] Terminate task {task_id}")
        return {"success": True, "message": f"Task {task_id} terminated"}

    elif req and req.meeting_code:
        meeting = req.meeting_code.strip().replace(" ", "")
        to_kill = [tid for tid, t in running_tasks.items() if t.get("meeting_code") == meeting]
        if not to_kill:
            raise HTTPException(404, f"No active tasks for meeting {meeting}")
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
        if meeting in meeting_groups:
            del meeting_groups[meeting]
        print(f"[API] Terminate meeting {meeting} ({len(to_kill)} tasks)")
        return {"success": True, "message": f"Meeting {meeting} terminated ({len(to_kill)} tasks)"}

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
        meeting_groups.clear()
        print(f"[API] Terminate ALL")
        return {"success": True, "message": "All tasks terminated"}

# ============================================
# DASHBOARD – Professional + IST + Search
# ============================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Zoom Control Panel</title>
<style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body {
        background: #0b1420;
        font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
        color: #d8e4f0;
        padding: 16px;
        min-height: 100vh;
    }
    .container { max-width: 1400px; margin:0 auto; }
    .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: linear-gradient(90deg, #122033, #0f1c2e);
        border-radius: 10px;
        padding: 14px 20px;
        margin-bottom: 18px;
        border: 1px solid #1e3a5f;
    }
    .header h1 {
        font-size: 22px;
        font-weight: 700;
        color: #7eb6ff;
        letter-spacing: 0.3px;
    }
    .header h1 span { color: #4a9eff; }
    .header-right {
        display: flex;
        align-items: center;
        gap: 18px;
        font-size: 14px;
    }
    .usage {
        background: #0a1525;
        padding: 5px 14px;
        border-radius: 6px;
        border: 1px solid #1e3a5f;
        color: #8ab4f8;
    }
    .usage strong { color: #ffd166; }
    .main-grid {
        display: grid;
        grid-template-columns: 340px 1fr;
        gap: 18px;
    }
    @media (max-width: 900px) { .main-grid { grid-template-columns: 1fr; } }
    .left-panel, .right-panel {
        background: #122033;
        border-radius: 10px;
        padding: 16px;
        border: 1px solid #1e3a5f;
    }
    .section-title {
        font-size: 14px;
        font-weight: 600;
        color: #7eb6ff;
        border-bottom: 1px solid #1e3a5f;
        padding-bottom: 8px;
        margin-bottom: 14px;
    }
    .form-group { margin-bottom: 12px; }
    .form-group label {
        display: block;
        font-size: 12px;
        color: #89a9c9;
        margin-bottom: 4px;
    }
    .form-group input, .form-group select, .form-group textarea {
        width: 100%;
        padding: 8px 10px;
        background: #0a1525;
        border: 1px solid #1e3a5f;
        border-radius: 6px;
        color: #e0e8f0;
        font-size: 13px;
        outline: none;
    }
    .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
        border-color: #4a9eff;
        box-shadow: 0 0 0 3px rgba(74,158,255,0.15);
    }
    .form-row {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
    }
    .btn-row {
        display: flex;
        gap: 10px;
        margin-top: 8px;
    }
    .btn {
        padding: 8px 18px;
        border: none;
        border-radius: 6px;
        font-weight: 600;
        font-size: 13px;
        cursor: pointer;
        transition: all 0.2s;
    }
    .btn-primary { background: #2a7acc; color: white; }
    .btn-primary:hover { background: #3a8adc; }
    .btn-danger { background: #c44; color: white; }
    .btn-danger:hover { background: #d55; }
    .btn-outline { background: transparent; border: 1px solid #2a4a6a; color: #8ab4f8; }
    .btn-outline:hover { background: #1a2a3a; }
    .btn-sm { padding: 4px 10px; font-size: 11px; }
    .right-panel .table-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 12px;
        flex-wrap: wrap;
        gap: 10px;
    }
    .right-panel .table-header h2 {
        font-size: 16px;
        font-weight: 600;
        color: #7eb6ff;
    }
    .search-box {
        display: flex;
        gap: 8px;
        align-items: center;
    }
    .search-box input {
        padding: 6px 10px;
        background: #0a1525;
        border: 1px solid #1e3a5f;
        border-radius: 6px;
        color: #e0e8f0;
        font-size: 13px;
        width: 160px;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    th {
        text-align: left;
        padding: 10px 8px;
        color: #89a9c9;
        font-weight: 500;
        border-bottom: 1px solid #1e3a5f;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.4px;
    }
    td {
        padding: 10px 8px;
        border-bottom: 1px solid #0f1a2b;
        vertical-align: middle;
    }
    tr:hover td { background: #1a2a3a; }
    .meeting-id {
        font-weight: 600;
        color: #7eb6ff;
    }
    .badge-type {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 10px;
        font-weight: 500;
        border: 1px solid;
    }
    .badge-indian { border-color: #4a9eff; color: #4a9eff; }
    .badge-english { border-color: #6aaa6a; color: #6aaa6a; }
    .badge-custom { border-color: #c29a4a; color: #c29a4a; }
    .log {
        margin-top: 12px;
        padding: 8px 12px;
        background: #0a1525;
        border: 1px solid #1e3a5f;
        border-radius: 6px;
        font-size: 12px;
        color: #89a9c9;
        font-family: 'Cascadia Code', 'Fira Code', monospace;
        min-height: 32px;
    }
    .log .ok { color: #6aaa6a; }
    .log .err { color: #e66; }
    .log .info { color: #4a9eff; }
    #customBox {
        display: none;
        margin-top: 8px;
        padding: 10px;
        background: #0a1525;
        border: 1px solid #1e3a5f;
        border-radius: 6px;
    }
    .name-status { font-size: 11px; color: #89a9c9; margin-top: 6px; }
    .name-status .ok { color: #6aaa6a; }
    .name-status .err { color: #e66; }
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🔵 ZOOM <span>Control Panel</span></h1>
        <div class="header-right">
            <span class="usage">Usage: <strong id="totalCap">0</strong> / <strong id="totalCapMax">0</strong></span>
            <span id="liveTime" style="color:#89a9c9;"></span>
            <button class="btn btn-outline btn-sm" onclick="refresh()">⟳ Refresh</button>
        </div>
    </div>

    <div class="main-grid">
        <div class="left-panel">
            <div class="section-title">Launch Bots</div>
            <div class="form-group">
                <label>Meeting ID</label>
                <input id="meetingId" placeholder="98695209590" />
            </div>
            <div class="form-group">
                <label>Passcode (optional)</label>
                <input id="passcode" placeholder="Leave empty if none" />
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label>Bots (1-100)</label>
                    <input type="number" id="botCount" value="10" min="1" max="200" oninput="updCount()" />
                </div>
                <div class="form-group">
                    <label>Name Type</label>
                    <select id="nameType" onchange="toggleCustom()">
                        <option value="indian">🇮🇳 Indian</option>
                        <option value="english">🇺🇸 English</option>
                        <option value="custom">✏️ Custom</option>
                    </select>
                </div>
            </div>
            <div id="customBox">
                <label style="font-size:11px;color:#89a9c9;">Custom names (one per line)</label>
                <textarea id="customNames" rows="3" placeholder="Rahul Sharma&#10;Arjun Singh"></textarea>
                <div class="name-status">Names: <strong id="nameCount">0</strong> | Need: <strong id="needCount">10</strong> <span id="nameStatus"></span></div>
            </div>
            <div class="form-group">
                <label>Duration (minutes)</label>
                <input type="number" id="duration" value="120" min="1" />
            </div>
            <div class="btn-row">
                <button class="btn btn-primary" onclick="startBots()">▶ Start</button>
                <button class="btn btn-danger" onclick="killAll()">⏹ Kill All</button>
            </div>
            <div id="msg" class="log">Ready • All times in IST</div>
        </div>

        <div class="right-panel">
            <div class="table-header">
                <h2>Active Meetings</h2>
                <div class="search-box">
                    <input id="searchMeeting" placeholder="Search Meeting ID" />
                    <button class="btn btn-danger btn-sm" onclick="killBySearch()">Kill</button>
                </div>
                <span class="badge" id="taskCount" style="background:#0a1525;padding:4px 12px;border-radius:20px;font-size:12px;color:#89a9c9;">0 active</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Meeting ID</th>
                        <th>Total Bots</th>
                        <th>Started (IST)</th>
                        <th>Remaining</th>
                        <th>Names</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody id="tbody">
                    <tr><td colspan="7" style="text-align:center;color:#89a9c9;padding:20px;">No active meetings</td></tr>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
const API = location.origin;
const $ = id => document.getElementById(id);

function show(m, type='info'){
    const cls = type==='ok'?'ok':type==='err'?'err':'info';
    msg.innerHTML = `<span class="${cls}">[${new Date().toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'})}] ${m}</span>`;
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
    liveTime.textContent = new Date().toLocaleTimeString('en-IN', {timeZone:'Asia/Kolkata'}) + ' IST';
}
setInterval(updateClock, 1000);
updateClock();

async function refresh(){
    try{
        const r = await fetch(API+'/status');
        const d = await r.json();
        const workers = d.workers || {};
        const meetings = d.meetings || {};

        let total=0, free=0;
        Object.values(workers).forEach(w=>{
            total += w.max_capacity || 0;
            free += w.free_capacity || 0;
        });
        totalCap.textContent = total - free;
        totalCapMax.textContent = total;
        taskCount.textContent = Object.keys(meetings).length + ' meetings';

        if(!Object.keys(meetings).length){
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#89a9c9;padding:20px;">No active meetings</td></tr>';
        } else {
            let idx = 0;
            tbody.innerHTML = Object.entries(meetings).map(([meeting, m])=>{
                idx++;
                const bots = m.total_bots || 0;
                const type = m.name_type || 'indian';
                const startTime = m.started_at ? new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}) : '-';
                const remaining = m.duration_minutes || 120;
                const typeBadge = type === 'indian' ? 'indian' : type === 'english' ? 'english' : 'custom';
                return `<tr>
                    <td>${idx}</td>
                    <td class="meeting-id">${meeting}</td>
                    <td><strong>${bots}</strong></td>
                    <td>${startTime}</td>
                    <td>${remaining} min</td>
                    <td><span class="badge-type badge-${typeBadge}">${type}</span></td>
                    <td>
                        <button class="btn btn-danger btn-sm" onclick="killMeeting('${meeting}')">Kill</button>
                    </td>
                </tr>`;
            }).join('');
        }
        show('Status refreshed', 'ok');
    } catch(e){
        show(e.message || 'Refresh failed', 'err');
    }
}

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
        show('Starting bots...', 'info');
        const r = await fetch(API+'/api/start-bots', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({
                meeting_code: meeting,
                passcode: pass,
                bot_count: bots,
                duration_minutes: dur,
                name_type: type,
                custom_names: custom,
                join_mode: 'individual'
            })
        });
        const d = await r.json();
        if(r.ok){
            show(d.message || 'Started!', 'ok');
            setTimeout(refresh, 800);
        } else {
            show(d.detail || 'Failed', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

async function killMeeting(meeting){
    if(!confirm(`Kill all bots for meeting ${meeting}?`)) return;
    try{
        const r = await fetch(API+'/api/terminate', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({meeting_code: meeting})
        });
        const d = await r.json();
        if(r.ok){
            show(d.message || 'Killed', 'ok');
            setTimeout(refresh, 800);
        } else {
            show(d.detail || 'Kill failed', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

async function killBySearch(){
    const meeting = $('searchMeeting').value.trim().replace(/\s/g,'');
    if(!meeting) return show('Enter Meeting ID to kill', 'err');
    await killMeeting(meeting);
}

async function killAll(){
    if(!confirm('Kill ALL meetings and bots?')) return;
    try{
        const r = await fetch(API+'/api/terminate', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({})
        });
        const d = await r.json();
        if(r.ok){
            show('All tasks killed', 'ok');
            setTimeout(refresh, 800);
        } else {
            show(d.detail || 'Kill all failed', 'err');
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
