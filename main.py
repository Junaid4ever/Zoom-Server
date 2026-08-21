# ============================================
# ZOOM BOT CENTRAL – OLD‑SCHOOL PANEL STYLE
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
    wid_to_remove = None
    for wid, info in workers.items():
        if info.get("sid") == sid:
            wid_to_remove = wid
            break
    if wid_to_remove:
        for tid in list(running_tasks.keys()):
            if running_tasks[tid].get("worker_id") == wid_to_remove:
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
# DASHBOARD – OLD‑SCHOOL PANEL (NO SCHEDULE/REACTIONS)
# ============================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes"/>
<title>Zoom Panel</title>
<style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body {
        background: #0f1a2b;
        font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
        color: #e0e8f0;
        padding: 12px;
        min-height: 100vh;
    }
    .container { max-width: 1600px; margin:0 auto; }

    /* header */
    .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: #142433;
        border-radius: 6px;
        padding: 8px 16px;
        margin-bottom: 12px;
        border-bottom: 2px solid #2a4a6a;
    }
    .header h1 {
        font-size: 20px;
        font-weight: 600;
        color: #8ab4f8;
        letter-spacing: 0.5px;
    }
    .header h1 span { color: #5c8fc7; }
    .header-right {
        display: flex;
        align-items: center;
        gap: 16px;
        font-size: 14px;
    }
    .header-right .usage {
        background: #0a1520;
        padding: 4px 12px;
        border-radius: 4px;
        border: 1px solid #2a4a6a;
        color: #8ab4f8;
    }
    .header-right .usage strong { color: #ffd966; }

    /* main grid */
    .main-grid {
        display: grid;
        grid-template-columns: 320px 1fr;
        gap: 16px;
    }
    @media (max-width: 860px) { .main-grid { grid-template-columns: 1fr; } }

    /* left panel */
    .left-panel {
        background: #142433;
        border-radius: 6px;
        padding: 14px;
        border: 1px solid #1f3a55;
    }
    .left-panel .section-title {
        font-size: 13px;
        font-weight: 600;
        color: #8ab4f8;
        border-bottom: 1px solid #1f3a55;
        padding-bottom: 6px;
        margin-bottom: 12px;
    }
    .form-group {
        margin-bottom: 10px;
    }
    .form-group label {
        display: block;
        font-size: 12px;
        color: #89a9c9;
        margin-bottom: 2px;
    }
    .form-group input, .form-group select, .form-group textarea {
        width: 100%;
        padding: 6px 8px;
        background: #0a1520;
        border: 1px solid #1f3a55;
        border-radius: 4px;
        color: #e0e8f0;
        font-size: 13px;
        outline: none;
    }
    .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
        border-color: #4a8bc2;
        box-shadow: 0 0 0 2px rgba(74,139,194,0.2);
    }
    .form-group textarea { resize: vertical; font-size: 12px; }
    .form-row {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
    }
    .btn-row {
        display: flex;
        gap: 10px;
        margin-top: 6px;
    }
    .btn {
        padding: 6px 18px;
        border: none;
        border-radius: 4px;
        font-weight: 600;
        font-size: 13px;
        cursor: pointer;
        transition: all 0.2s;
    }
    .btn-primary {
        background: #2a6a9a;
        color: white;
    }
    .btn-primary:hover { background: #3a7aaa; }
    .btn-danger {
        background: #8a3a3a;
        color: white;
    }
    .btn-danger:hover { background: #aa4a4a; }
    .btn-outline {
        background: transparent;
        border: 1px solid #2a4a6a;
        color: #8ab4f8;
    }
    .btn-outline:hover { background: #1a2a3a; }
    .btn-sm { padding: 2px 10px; font-size: 11px; }

    /* right panel - table */
    .right-panel {
        background: #142433;
        border-radius: 6px;
        padding: 12px;
        border: 1px solid #1f3a55;
        overflow-x: auto;
    }
    .right-panel .table-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 10px;
    }
    .right-panel .table-header h2 {
        font-size: 16px;
        font-weight: 600;
        color: #8ab4f8;
    }
    .right-panel .table-header .badge {
        background: #1a2a3a;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        color: #89a9c9;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    th {
        text-align: left;
        padding: 8px 6px;
        color: #89a9c9;
        font-weight: 500;
        border-bottom: 1px solid #1f3a55;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }
    td {
        padding: 8px 6px;
        border-bottom: 1px solid #0f1a2b;
        vertical-align: middle;
    }
    tr:hover td { background: #1a2a3a; }
    .meeting-id {
        font-weight: 600;
        color: #8ab4f8;
        cursor: pointer;
    }
    .meeting-id:hover { text-decoration: underline; }
    .badge-type {
        display: inline-block;
        padding: 0 8px;
        border-radius: 12px;
        font-size: 10px;
        font-weight: 500;
        line-height: 18px;
        border: 1px solid;
    }
    .badge-indian { border-color: #4a8bc2; color: #4a8bc2; }
    .badge-english { border-color: #6a8a6a; color: #6a8a6a; }
    .badge-custom { border-color: #c29a4a; color: #c29a4a; }
    .status-valid { color: #6aaa6a; }
    .status-invalid { color: #aa6a6a; }

    .action-btns {
        display: flex;
        gap: 4px;
        flex-wrap: wrap;
    }
    .action-btns .btn { font-size: 11px; padding: 2px 8px; }

    .log {
        margin-top: 10px;
        padding: 6px 10px;
        background: #0a1520;
        border: 1px solid #1f3a55;
        border-radius: 4px;
        font-size: 12px;
        color: #89a9c9;
        font-family: monospace;
        min-height: 28px;
    }
    .log .ok { color: #6aaa6a; }
    .log .err { color: #aa6a6a; }
    .log .info { color: #4a8bc2; }

    /* custom names toggle */
    #customBox {
        display: none;
        margin-top: 6px;
        padding: 8px;
        background: #0a1520;
        border: 1px solid #1f3a55;
        border-radius: 4px;
    }
    #customBox .name-status { font-size: 11px; color: #89a9c9; margin-top: 4px; }
    #customBox .name-status .ok { color: #6aaa6a; }
    #customBox .name-status .err { color: #aa6a6a; }

    @media (max-width: 600px) {
        .header h1 { font-size: 16px; }
        .header-right { font-size: 12px; gap: 8px; }
        .form-row { grid-template-columns: 1fr; }
    }
</style>
</head>
<body>
<div class="container">
    <!-- header -->
    <div class="header">
        <h1>🔵 ZOOM <span>Panel</span></h1>
        <div class="header-right">
            <span class="usage">Usage: <strong id="totalCap">0</strong>/<strong id="totalCapMax">0</strong></span>
            <span id="liveTime" style="color:#89a9c9; font-size:13px;"></span>
            <button class="btn btn-outline btn-sm" onclick="refresh()">⟳</button>
        </div>
    </div>

    <!-- main -->
    <div class="main-grid">
        <!-- left panel -->
        <div class="left-panel">
            <div class="section-title">Add custom names</div>
            <div class="form-group">
                <label>Meeting ID:</label>
                <input id="meetingId" placeholder="98695209590" />
            </div>
            <div class="form-group">
                <label>Meeting Password:</label>
                <input id="passcode" placeholder="optional" />
            </div>
            <div class="form-row">
                <div class="form-group">
                    <label>Members (1-100):</label>
                    <input type="number" id="botCount" value="10" min="1" max="500" oninput="updCount()" />
                </div>
                <div class="form-group">
                    <label>Name:</label>
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
                <div class="name-status">Names: <strong id="nameCount">0</strong> &nbsp;|&nbsp; Need: <strong id="needCount">10</strong> <span id="nameStatus"></span></div>
            </div>
            <div class="form-group">
                <label>Timeout (in seconds):</label>
                <input type="number" id="duration" value="7200" min="60" />
            </div>
            <div class="btn-row">
                <button class="btn btn-primary" onclick="startBots()">▶ Start</button>
                <button class="btn btn-danger" onclick="killAll()">⏹ Kill All</button>
            </div>
            <div id="msg" class="log">Ready</div>
        </div>

        <!-- right panel -->
        <div class="right-panel">
            <div class="table-header">
                <h2>All Meetings</h2>
                <span class="badge" id="taskCount">0 active</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Meeting ID</th>
                        <th>Qty</th>
                        <th>Start</th>
                        <th>Time out</th>
                        <th>Nm Type</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody id="tbody">
                    <tr><td colspan="7" style="text-align:center;color:#89a9c9;">No active meetings</td></tr>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
const API = location.origin;
const $ = id => document.getElementById(id);
const meetingId = $('meetingId'), passcode = $('passcode'), botCount = $('botCount');
const duration = $('duration'), nameType = $('nameType'), customNames = $('customNames');
const customBox = $('customBox'), msg = $('msg'), tbody = $('tbody');
const totalCap = $('totalCap'), totalCapMax = $('totalCapMax');
const taskCount = $('taskCount'), liveTime = $('liveTime');

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
        totalCap.textContent = total - free;
        totalCapMax.textContent = total;
        taskCount.textContent = Object.keys(tasks).length + ' active';

        if(!Object.keys(tasks).length){
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#89a9c9;">No active meetings</td></tr>';
        } else {
            let idx = 0;
            tbody.innerHTML = Object.entries(tasks).map(([tid,t])=>{
                idx++;
                const meeting = t.meeting_code || 'N/A';
                const bots = t.bot_count || 0;
                const type = t.name_type || 'indian';
                const remaining = t.remaining_minutes !== undefined ? t.remaining_minutes : t.duration_minutes || 120;
                const startTime = t.started_at ? new Date(t.started_at).toLocaleTimeString() : '-';
                const totalDur = t.duration_minutes || 120;
                const timeOut = Math.ceil(remaining) + ' min';
                const typeBadge = type === 'indian' ? 'indian' : type === 'english' ? 'english' : 'custom';
                const status = remaining > 0 ? 'valid' : 'invalid';
                return `<tr>
                    <td>${idx}</td>
                    <td class="meeting-id" onclick="alert('Meeting: ${meeting}')">${meeting}</td>
                    <td>${bots}</td>
                    <td>${startTime}</td>
                    <td>${timeOut}</td>
                    <td><span class="badge-type badge-${typeBadge}">${type}</span></td>
                    <td>
                        <div class="action-btns">
                            <button class="btn btn-outline btn-sm" onclick="refillTask('${tid}')">Refill</button>
                            <button class="btn btn-danger btn-sm" onclick="killTask('${tid}')">Kill</button>
                        </div>
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
    const dur = parseInt(duration.value) / 60 || 120;
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
                duration_minutes: Math.ceil(dur), name_type: type, custom_names: custom,
                join_mode: 'individual'
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
            show(`Kill sent`, 'ok');
            setTimeout(refresh, 1000);
        } else {
            show(d.detail || 'Kill failed', 'err');
        }
    } catch(e){ show(e.message, 'err'); }
}

async function refillTask(taskId){
    // Refill logic: just show info for now (you can implement add bots)
    alert('Refill feature: add more bots to existing task (not implemented)');
}

async function killAll(){
    if(!confirm('Kill ALL tasks?')) return;
    try{
        const r = await fetch(API+'/api/terminate', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({})
        });
        const d = await r.json();
        if(r.ok){
            show('All tasks killed', 'ok');
            setTimeout(refresh, 1000);
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
