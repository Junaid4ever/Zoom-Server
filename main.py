# ============================================
# ZOOM BOT CENTRAL - Railway (FULL)
# ============================================
import os
import uuid
from datetime import datetime
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

class TerminateRequest(BaseModel):
    meeting_code: Optional[str] = None

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
        del running_tasks[tid]
        print(f"[SIO] Task completed: {tid}")

@app.get("/health")
async def health():
    return {"ok": True, "workers": len(workers)}

@app.get("/status")
@app.get("/api/status")
async def status():
    total_free = sum(w.get("free_capacity", 0) for w in workers.values())
    return {
        "workers": workers,
        "running_tasks": running_tasks,
        "total_free_capacity": total_free
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
            "started_at": datetime.now().isoformat()
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
    await sio.emit("terminate", {"meeting_code": meeting})
    if meeting:
        for tid in list(running_tasks.keys()):
            if running_tasks[tid].get("meeting_code") == meeting:
                del running_tasks[tid]
    else:
        running_tasks.clear()
    for wid in workers:
        workers[wid]["free_capacity"] = workers[wid]["max_capacity"]
    print(f"[API] Terminate → {meeting or 'ALL'}")
    return {"success": True, "message": "Terminate sent"}

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Zoom Master Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Segoe UI,sans-serif;background:#0d1117;color:#c9d1d9;padding:20px}
.container{max-width:1200px;margin:0 auto}
h1{color:#58a6ff;margin-bottom:8px}
.subtitle{color:#8b949e;margin-bottom:20px;font-size:14px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:20px}
.stat{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px;text-align:center}
.stat .n{font-size:24px;font-weight:700;color:#58a6ff}
.stat .l{font-size:11px;color:#8b949e;margin-top:4px}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:18px;margin-bottom:16px}
.card h2{font-size:16px;margin-bottom:12px;color:#f0f6fc}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.fg{display:flex;flex-direction:column;gap:4px}
.fg label{font-size:11px;color:#8b949e;text-transform:uppercase}
.fg input,.fg select,textarea{padding:8px 10px;background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#c9d1d9;font-size:14px}
.actions{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}
.btn{padding:8px 16px;border:none;border-radius:6px;font-weight:600;cursor:pointer;font-size:13px}
.btn-p{background:#238636;color:#fff}
.btn-d{background:#da3633;color:#fff}
.btn-s{background:#21262d;color:#c9d1d9;border:1px solid #30363d}
.btn-sm{padding:4px 10px;font-size:12px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:8px;border-bottom:1px solid #21262d;text-align:left}
th{color:#8b949e}
.workers{display:flex;flex-wrap:wrap;gap:8px}
.w{background:#0d1117;border:1px solid #30363d;border-radius:4px;padding:4px 10px;font-family:monospace;font-size:12px}
#customBox{display:none;margin-top:14px;padding:12px;background:#0d1117;border:1px solid #30363d;border-radius:8px}
.log{margin-top:10px;padding:8px;background:#0d1117;border:1px solid #30363d;border-radius:6px;font-family:monospace;font-size:12px}
.ok{color:#3fb950}.err{color:#f85149}.info{color:#58a6ff}
</style>
</head>
<body>
<div class="container">
  <h1>🚀 Zoom Master Dashboard</h1>
  <div class="subtitle">Colab workers · capacity accumulates</div>
  <div class="stats">
    <div class="stat"><div class="n" id="totalCap">0</div><div class="l">Total Capacity</div></div>
    <div class="stat"><div class="n" id="freeCap">0</div><div class="l">Free Capacity</div></div>
    <div class="stat"><div class="n" id="workersN">0</div><div class="l">Workers</div></div>
    <div class="stat"><div class="n" id="tasksN">0</div><div class="l">Active Tasks</div></div>
    <div class="stat"><div class="n" id="botsN">0</div><div class="l">Running Bots</div></div>
  </div>
  <div class="card">
    <h2>📌 Start Bots</h2>
    <div class="grid">
      <div class="fg"><label>Meeting ID</label><input id="meetingId" placeholder="5415403058"/></div>
      <div class="fg"><label>Passcode</label><input id="passcode" placeholder="optional"/></div>
      <div class="fg"><label>Bots</label><input type="number" id="botCount" value="10" min="1" max="500" oninput="updCount()"/></div>
      <div class="fg"><label>Duration (min)</label><input type="number" id="duration" value="120" min="1"/></div>
      <div class="fg">
        <label>Name Type</label>
        <select id="nameType" onchange="toggleCustom()">
          <option value="indian">🇮🇳 Indian (Natural)</option>
          <option value="english">🇺🇸 English</option>
          <option value="custom">✏️ Custom Names</option>
        </select>
      </div>
    </div>
    <div id="customBox">
      <label style="font-size:12px;color:#8b949e">Custom names (one per line)</label>
      <textarea id="customNames" rows="4" placeholder="Rahul Sharma&#10;arjun - 786"></textarea>
      <div style="font-size:12px;color:#8b949e;margin-top:6px">
        Names: <strong id="nameCount">0</strong> | Need: <strong id="needCount">10</strong>
        <span id="nameStatus"></span>
      </div>
    </div>
    <div class="actions">
      <button class="btn btn-p" onclick="startBots()">🚀 Start Bots</button>
      <button class="btn btn-d" onclick="killAll()">⏹️ Kill All</button>
      <button class="btn btn-s" onclick="refresh()">🔄 Refresh</button>
    </div>
    <div id="msg" class="log">Ready</div>
  </div>
  <div class="card">
    <h2>📋 Active Tasks</h2>
    <table>
      <thead><tr><th>Task</th><th>Meeting</th><th>Bots</th><th>Type</th><th>Started</th><th></th></tr></thead>
      <tbody id="tbody"><tr><td colspan="6" style="text-align:center;color:#8b949e">No tasks</td></tr></tbody>
    </table>
  </div>
  <div class="card">
    <h2>🖥️ Workers</h2>
    <div id="wlist" class="workers"><span style="color:#8b949e">No workers — run Colab cell</span></div>
  </div>
</div>
<script>
const API = location.origin;
function toggleCustom(){
  document.getElementById('customBox').style.display =
    document.getElementById('nameType').value==='custom'?'block':'none';
  updCount();
}
function updCount(){
  const bots = parseInt(document.getElementById('botCount').value)||0;
  const names = document.getElementById('customNames').value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);
  document.getElementById('nameCount').textContent = names.length;
  document.getElementById('needCount').textContent = bots;
  const st = document.getElementById('nameStatus');
  if(document.getElementById('nameType').value!=='custom'){ st.innerHTML=''; return; }
  st.innerHTML = names.length>=bots ? ' <span class="ok">✅ Enough</span>' : ` <span class="err">❌ Need ${bots-names.length} more</span>`;
}
document.getElementById('customNames').addEventListener('input', updCount);
function show(m,t='info'){
  document.getElementById('msg').innerHTML = `<span class="${t}">[${new Date().toLocaleTimeString()}] ${m}</span>`;
}
async function refresh(){
  try{
    const r = await fetch(API+'/status');
    const d = await r.json();
    const workers = d.workers||{};
    const tasks = d.running_tasks||{};
    let total=0, free=d.total_free_capacity||0, running=0;
    Object.values(workers).forEach(w=> total += w.max_capacity||0);
    Object.values(tasks).forEach(t=> running += t.bot_count||0);
    document.getElementById('totalCap').textContent = total;
    document.getElementById('freeCap').textContent = free;
    document.getElementById('workersN').textContent = Object.keys(workers).length;
    document.getElementById('tasksN').textContent = Object.keys(tasks).length;
    document.getElementById('botsN').textContent = running;
    const wl = document.getElementById('wlist');
    if(!Object.keys(workers).length) wl.innerHTML='<span style="color:#8b949e">No workers — run Colab cell</span>';
    else wl.innerHTML = Object.entries(workers).map(([id,w])=>
      `<div class="w">🟢 ${id} → ${w.free_capacity}/${w.max_capacity}</div>`).join('');
    const tb = document.getElementById('tbody');
    const keys = Object.keys(tasks);
    if(!keys.length) tb.innerHTML='<tr><td colspan="6" style="text-align:center;color:#8b949e">No tasks</td></tr>';
    else tb.innerHTML = keys.map(tid=>{
      const t=tasks[tid];
      return `<tr>
        <td>${tid}</td><td><b>${t.meeting_code}</b></td><td>${t.bot_count}</td>
        <td>${t.name_type||'-'}</td>
        <td>${t.started_at?new Date(t.started_at).toLocaleTimeString():'-'}</td>
        <td><button class="btn btn-d btn-sm" onclick="kill('${t.meeting_code}')">Kill</button></td>
      </tr>`;
    }).join('');
    show('Status refreshed','ok');
  }catch(e){ show(e.message,'err'); }
}
async function startBots(){
  const meetingId = document.getElementById('meetingId').value.trim().replace(/\s/g,'');
  const passcode = document.getElementById('passcode').value.trim();
  const botCount = parseInt(document.getElementById('botCount').value)||10;
  const duration = parseInt(document.getElementById('duration').value)||120;
  const nameType = document.getElementById('nameType').value;
  let custom = null;
  if(nameType==='custom'){
    custom = document.getElementById('customNames').value.split(/[\n,]/).map(s=>s.trim()).filter(Boolean);
    if(custom.length < botCount) return show('Need '+(botCount-custom.length)+' more names','err');
  }
  if(!meetingId) return show('Meeting ID required','err');
  try{
    show('Starting...','info');
    const r = await fetch(API+'/api/start-bots',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        meeting_code: meetingId, passcode, bot_count: botCount,
        duration_minutes: duration, name_type: nameType, custom_names: custom
      })
    });
    const d = await r.json();
    if(r.ok){ show(d.message||'Started','ok'); setTimeout(refresh,1500); }
    else show(d.detail||'Failed','err');
  }catch(e){ show(e.message,'err'); }
}
async function kill(code){
  if(!confirm('Kill '+code+'?')) return;
  await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({meeting_code:code})});
  show('Kill sent','ok'); setTimeout(refresh,1500);
}
async function killAll(){
  if(!confirm('Kill ALL?')) return;
  await fetch(API+'/api/terminate',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
  show('Kill all sent','ok'); setTimeout(refresh,1500);
}
setInterval(refresh,8000);
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
