# ============================================
# ZOOM BOT CENTRAL – Railway FULL
# + HF Wallet + Space Control (FIXED)
# ============================================
import os, uuid, asyncio, json, signal, random, httpx, time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel
import socketio

try:
    from huggingface_hub import HfApi, list_spaces
except ImportError:
    HfApi = None
    list_spaces = None
try:
    import indian_names
except Exception:
    indian_names = None
try:
    from faker import Faker
    _faker = Faker()
except Exception:
    _faker = None

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", logger=False, engineio_logger=False)
app = FastAPI(title="Zoom Bot Central")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

workers, running_tasks, meeting_groups, scheduled_tasks = {}, {}, {}, {}
session_status = {"logged_in": False, "last_checked": None, "message": "No session file", "login_in_progress": False}
meeting_logs, global_logs = {}, deque(maxlen=400)
meeting_used_firsts = {}
STATE_FILE = "bot_state.json"
BANNED_FIRSTS = {"katappa", "mj", "m j", "m.j"}
pause_state = False
_hf_cache = {"spaces": None, "timestamp": 0, "ttl": 30}

def add_log(meeting, message, level="info"):
    ts = now_ist().strftime("%H:%M:%S")
    line = {"time": ts, "meeting": meeting or "-", "message": message, "level": level}
    global_logs.append(line)
    if meeting and meeting != "-":
        meeting_logs.setdefault(meeting, deque(maxlen=500)).append(line)
    print(f"[{ts}] [{meeting or '-'}] {message}", flush=True)

def save_state():
    try:
        data = {
            "running_tasks": running_tasks,
            "meeting_groups": meeting_groups,
            "scheduled_tasks": scheduled_tasks,
            "meeting_used_firsts": {k: list(v) for k, v in meeting_used_firsts.items()},
            "workers_cap": {
                wid: {
                    "max_capacity": w.get("max_capacity", 50),
                    "free_capacity": w.get("free_capacity", 0),
                }
                for wid, w in workers.items()
            },
            "pause_state": pause_state,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"save_state err: {e}", flush=True)

def load_state():
    global running_tasks, meeting_groups, scheduled_tasks, meeting_used_firsts, pause_state
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        running_tasks.update(data.get("running_tasks") or {})
        meeting_groups.update(data.get("meeting_groups") or {})
        scheduled_tasks.update(data.get("scheduled_tasks") or {})
        meeting_used_firsts.update({k: set(v) for k, v in (data.get("meeting_used_firsts") or {}).items()})
        pause_state = data.get("pause_state", False)
        print(f"[STATE] restored meetings={len(meeting_groups)} tasks={len(running_tasks)} pause={pause_state}", flush=True)
    except Exception as e:
        print(f"load_state err: {e}", flush=True)

def _ok_first(name, used):
    if not name:
        return False
    k = str(name).strip().lower()
    if k in used or k in BANNED_FIRSTS:
        return False
    if "katappa" in k or k.startswith("user"):
        return False
    return True

def allocate_unique_firsts(meeting: str, count: int, name_type: str) -> List[str]:
    used = meeting_used_firsts.setdefault(meeting, set())
    out = []
    tries = 0
    while len(out) < count and tries < count * 50:
        tries += 1
        if name_type == "english" and _faker:
            first = _faker.first_name()
        elif indian_names:
            gender = random.choice(["male", "female", None])
            first = indian_names.get_first_name(gender=gender) if gender else indian_names.get_first_name()
        else:
            first = random.choice(["Aarav", "Priya", "Rohan", "Ananya", "Diya", "Arjun", "Kavya", "Ishaan", "Navya", "Kabir"])
        first = str(first).strip().split()[0]
        if _ok_first(first, used):
            used.add(first.lower())
            out.append(first)
    while len(out) < count:
        extra = ("Aarav" if name_type != "english" else "Alex") + random.choice(["esh", "ansh", "yan", "ika", "vi", "en"])
        if _ok_first(extra, used):
            used.add(extra.lower())
            out.append(extra)
    return out

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

class HFSpaceAction(BaseModel):
    space_id: str

@sio.event
async def connect(sid, environ):
    print(f"[SIO] Connected: {sid}", flush=True)

@sio.event
async def disconnect(sid):
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            workers[wid]["sid"] = None
            workers[wid]["last_seen"] = now_ist().isoformat()
            orphan = [t for t, x in running_tasks.items() if x.get("worker_id") == wid]
            add_log("-", f"Worker {wid} disconnected | {len(orphan)} reserved — Kill to free", "err")
            save_state()
            break

@sio.event
async def register_worker(sid, data):
    wid = data.get("worker_id", f"worker-{sid[:6]}")
    max_cap = int(data.get("max_capacity", 10))
    now = now_ist().isoformat()
    saved_cap = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                saved_cap = (json.load(f).get("workers_cap") or {}).get(wid) or {}
        except Exception:
            saved_cap = {}
    reserved = sum(t.get("bot_count", 0) for t in running_tasks.values() if t.get("worker_id") == wid)
    if wid in workers:
        workers[wid].update({"sid": sid, "max_capacity": max_cap, "last_seen": now})
        if reserved:
            workers[wid]["free_capacity"] = max(0, max_cap - reserved)
    else:
        free = saved_cap.get("free_capacity")
        if free is None:
            free = max(0, max_cap - reserved)
        workers[wid] = {
            "sid": sid,
            "max_capacity": max_cap,
            "free_capacity": max(0, min(max_cap, int(free))),
            "last_seen": now,
        }
    add_log("-", f"Worker {wid} registered | max={max_cap} free={workers[wid]['free_capacity']} reserved={reserved}", "ok")
    save_state()
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
            meeting_used_firsts.pop(m, None)
    del running_tasks[tid]
    add_log(m or "-", f"Task {tid} completed | +{bc} capacity")
    save_state()

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
    add_log("-", "✅ Session JSON updated", "ok")
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("session_updated", {"message": "new session"}, to=info["sid"])
    return {"success": True, "message": "Session saved"}

@app.get("/api/logs")
async def get_logs(meeting: str = None, limit: int = 200):
    logs = list(meeting_logs.get(meeting, []))[-limit:] if meeting else list(global_logs)[-limit:]
    return {"logs": logs, "meeting": meeting}

@app.get("/status")
@app.get("/api/status")
async def status():
    connected = {w: i for w, i in workers.items() if i.get("sid")}
    meetings = {}
    for m, g in meeting_groups.items():
        active = sum(running_tasks[tid].get("bot_count", 0) for tid in g.get("task_ids", []) if tid in running_tasks)
        meetings[m] = {
            "meeting_code": m,
            "total_bots": g.get("total_bots", 0),
            "completed_bots": g.get("completed_bots", 0),
            "active_bots": active,
            "name_type": g.get("name_type", "indian"),
            "started_at": g.get("started_at"),
            "join_mode": g.get("join_mode", "individual"),
            "status": g.get("status", "running"),
        }
    reserved = sum(t.get("bot_count", 0) for t in running_tasks.values())
    total_cap = sum(x.get("max_capacity", 0) for x in workers.values())
    return {
        "workers": connected,
        "total_capacity": total_cap,
        "total_free_capacity": max(0, total_cap - reserved),
        "reserved_bots": reserved,
        "meetings": meetings,
        "schedules": scheduled_tasks,
        "session": session_status,
        "connected_workers_count": len(connected),
        "recent_logs": list(global_logs)[-40:],
        "pause_state": pause_state,
        "hf_token_set": bool(os.environ.get("HF_TOKEN")),
    }

@app.post("/api/start-bots")
async def start_bots(req: StartBotRequest):
    if pause_state:
        raise HTTPException(503, "System is globally paused. Resume first.")
    if not os.path.exists("zoom_session.json"):
        raise HTTPException(400, "No session file")
    if req.bot_count < 1:
        raise HTTPException(400, "bot_count >= 1")
    meeting = req.meeting_code.strip().replace(" ", "")
    if not meeting:
        raise HTTPException(400, "meeting required")
    passcode = "" if req.passcode is None else str(req.passcode)
    remaining, assigned = req.bot_count, []
    name_type = req.name_type or "indian"
    if name_type == "custom" and req.custom_names:
        firsts, used_local = [], meeting_used_firsts.setdefault(meeting, set())
        for raw in req.custom_names:
            if len(firsts) >= req.bot_count:
                break
            token = (str(raw).strip().split() or ["Aarav"])[0]
            key = token.lower()
            if key in used_local or key in BANNED_FIRSTS or "katappa" in key or key.startswith("user"):
                continue
            used_local.add(key)
            firsts.append(str(raw).strip())
        if len(firsts) < req.bot_count:
            firsts.extend(allocate_unique_firsts(meeting, req.bot_count - len(firsts), "indian"))
        all_firsts = firsts[:req.bot_count]
    else:
        all_firsts = allocate_unique_firsts(meeting, req.bot_count, name_type)
    offset = 0
    connected = {w: i for w, i in workers.items() if i.get("sid")}
    for wid, info in sorted(connected.items(), key=lambda x: x[1].get("free_capacity", 0), reverse=True):
        if remaining <= 0:
            break
        free = int(info.get("free_capacity", 0))
        if free <= 0:
            continue
        give = min(free, remaining)
        task_id = str(uuid.uuid4())[:8]
        slice_firsts = all_firsts[offset:offset + give]
        offset += give
        custom_slice = slice_firsts if name_type == "custom" else None
        payload = {
            "task_id": task_id,
            "meeting_code": meeting,
            "passcode": passcode,
            "bot_count": give,
            "duration_minutes": req.duration_minutes,
            "name_type": name_type,
            "custom_names": custom_slice,
            "assigned_first_names": slice_firsts,
            "join_mode": req.join_mode or "individual",
        }
        await sio.emit("new_task", payload, to=info["sid"])
        running_tasks[task_id] = {
            "task_id": task_id, "meeting_code": meeting, "bot_count": give, "worker_id": wid,
            "name_type": name_type, "duration_minutes": req.duration_minutes,
            "started_at": now_ist().isoformat(), "join_mode": req.join_mode or "individual",
        }
        if meeting not in meeting_groups:
            meeting_groups[meeting] = {
                "task_ids": [], "total_bots": 0, "completed_bots": 0,
                "name_type": name_type, "join_mode": req.join_mode or "individual",
                "started_at": now_ist().isoformat(), "status": "running",
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
    add_log(meeting, f"🚀 Started {started} bots | {name_type} | mode={req.join_mode}", "ok")
    save_state()
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
        "schedule_id": sid,
        "meeting_code": req.meeting_code.strip().replace(" ", ""),
        "passcode": "" if req.passcode is None else str(req.passcode),
        "bot_count": req.bot_count,
        "duration_minutes": req.duration_minutes,
        "name_type": req.name_type or "indian",
        "custom_names": req.custom_names,
        "join_mode": req.join_mode or "individual",
        "schedule_at": st.isoformat(),
        "created_at": now_ist().isoformat(),
    }
    add_log(req.meeting_code, f"📅 Scheduled {req.bot_count} bots", "info")
    save_state()
    return {"success": True, "schedule_id": sid, "message": "Scheduled successfully"}

@app.delete("/api/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    if schedule_id in scheduled_tasks:
        del scheduled_tasks[schedule_id]
        save_state()
        return {"success": True}
    raise HTTPException(404)

@app.post("/api/terminate")
async def terminate(req: Optional[TerminateRequest] = None):
    if req and req.meeting_code:
        meeting = req.meeting_code.strip().replace(" ", "")
        for wid, info in list(workers.items()):
            if info.get("sid"):
                await sio.emit("terminate_meeting", {"meeting_code": meeting}, to=info["sid"])
        for tid in [t for t, x in list(running_tasks.items()) if x.get("meeting_code") == meeting]:
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid, "meeting_code": meeting}, to=workers[wid]["sid"])
            if wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0),
                )
            del running_tasks[tid]
        meeting_groups.pop(meeting, None)
        meeting_used_firsts.pop(meeting, None)
        add_log(meeting, "🛑 HARD KILL all workers", "err")
        save_state()
        return {"success": True, "message": f"Meeting {meeting} terminated"}
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
                workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0),
            )
    running_tasks.clear()
    meeting_groups.clear()
    meeting_used_firsts.clear()
    add_log("-", "🛑 ALL hard-killed", "err")
    save_state()
    return {"success": True, "message": "All terminated"}

@app.post("/api/shutdown")
async def shutdown_server():
    add_log("-", "🛑 SHUTDOWN", "err")
    save_state()
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("shutdown", {}, to=info["sid"])
    await asyncio.sleep(1.5)
    os.kill(os.getpid(), signal.SIGTERM)
    return {"success": True}

@app.post("/api/pause")
async def pause_all():
    global pause_state
    pause_state = True
    add_log("-", "⏸️ GLOBAL PAUSE activated", "warn")
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("global_pause", {})
    save_state()
    return {"success": True, "paused": True}

@app.post("/api/resume")
async def resume_all():
    global pause_state
    pause_state = False
    add_log("-", "▶️ GLOBAL RESUME activated", "ok")
    for wid, info in workers.items():
        if info.get("sid"):
            await sio.emit("global_resume", {})
    save_state()
    return {"success": True, "paused": False}

@app.get("/api/pause-status")
async def pause_status():
    return {"paused": pause_state}

# ---------- HF helpers ----------
def _hf_token():
    return (os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_TOKEN") or "").strip()

def _runtime_stage(runtime) -> str:
    if runtime is None:
        return "unknown"
    if isinstance(runtime, dict):
        return str(runtime.get("stage") or runtime.get("status") or "unknown").lower()
    stage = getattr(runtime, "stage", None)
    if stage is not None:
        return str(getattr(stage, "value", stage)).lower()
    raw = getattr(runtime, "raw", None) or {}
    if isinstance(raw, dict):
        return str(raw.get("stage") or raw.get("status") or "unknown").lower()
    return "unknown"

def _normalize_status(stage: str) -> str:
    s = (stage or "unknown").lower()
    if s in ("running", "running_building"):
        return "running"
    if s in ("paused",):
        return "paused"
    if s in ("stopped", "sleeping", "sleeping_building"):
        return "stopped"
    if "error" in s:
        return "stopped"
    if s in ("building",):
        return "running"
    return s

def _extract_balance(payload):
    if not isinstance(payload, dict):
        return None
    for key in ("balance", "credits", "credit", "amount", "available"):
        if payload.get(key) is not None:
            try:
                return float(payload[key])
            except Exception:
                pass
    wallet = payload.get("wallet") or payload.get("billing") or payload.get("computeCredits") or {}
    if isinstance(wallet, dict):
        for key in ("balance", "credits", "amount", "available", "total"):
            if wallet.get(key) is not None:
                try:
                    return float(wallet[key])
                except Exception:
                    pass
    return None

@app.get("/api/wallet")
async def get_wallet_balance():
    token = _hf_token()
    if not token:
        return JSONResponse(status_code=400, content={"success": False, "error": "HF_TOKEN not set on server"})
    username = None
    credits = None
    source = None
    try:
        if HfApi is not None:
            api = HfApi(token=token)
            user_info = api.whoami()
            username = user_info.get("name") or user_info.get("fullname")
            credits = _extract_balance(user_info)
            if credits is not None:
                source = "whoami"
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=15) as client:
            if not username:
                r = await client.get("https://huggingface.co/api/whoami-v2", headers=headers)
                if r.status_code == 200:
                    info = r.json()
                    username = info.get("name")
                    if credits is None:
                        credits = _extract_balance(info)
            for url in (
                "https://huggingface.co/api/wallet",
                "https://huggingface.co/api/settings/billing",
                "https://huggingface.co/api/billing",
            ):
                if credits is not None:
                    break
                try:
                    resp = await client.get(url, headers=headers)
                    if resp.status_code == 200:
                        credits = _extract_balance(resp.json())
                        if credits is not None:
                            source = url
                except Exception:
                    continue
        if credits is None:
            credits = 0
        return {
            "success": True,
            "username": username,
            "credits": credits,
            "currency": "USD",
            "source": source,
            "token_ok": True,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/hf/spaces")
async def get_my_spaces(force_refresh: bool = False):
    global _hf_cache
    token = _hf_token()
    if not token:
        raise HTTPException(400, "HF_TOKEN not set on server (Railway Variables)")
    if HfApi is None or list_spaces is None:
        raise HTTPException(500, "huggingface_hub not installed")
    now = time.time()
    if not force_refresh and _hf_cache["spaces"] and (now - _hf_cache["timestamp"] < _hf_cache["ttl"]):
        return {"success": True, "spaces": _hf_cache["spaces"], "cached": True}
    try:
        api = HfApi(token=token)
        user = api.whoami()["name"]
        spaces = list(list_spaces(author=user, token=token))

        def fetch_one(space):
            sid = getattr(space, "id", None) or str(space)
            try:
                runtime = api.get_space_runtime(sid)
                stage = _runtime_stage(runtime)
            except Exception as e:
                stage = "unknown"
                print(f"runtime fail {sid}: {e}", flush=True)
            return {
                "id": sid,
                "name": sid.split("/")[-1],
                "status": _normalize_status(stage),
                "stage": stage,
                "sdk": getattr(space, "sdk", "unknown"),
                "likes": getattr(space, "likes", 0),
            }

        results = [fetch_one(s) for s in spaces]
        _hf_cache["spaces"] = results
        _hf_cache["timestamp"] = now
        return {"success": True, "spaces": results, "cached": False, "username": user}
    except Exception as e:
        if _hf_cache["spaces"]:
            return {"success": True, "spaces": _hf_cache["spaces"], "cached": True, "warning": str(e)}
        raise HTTPException(500, str(e))

@app.post("/api/hf/pause")
async def pause_space(body: HFSpaceAction):
    token = _hf_token()
    if not token:
        raise HTTPException(400, "HF_TOKEN not set")
    space_id = (body.space_id or "").strip()
    if not space_id:
        raise HTTPException(400, "space_id required")
    try:
        api = HfApi(token=token)
        runtime = api.get_space_runtime(space_id)
        status = _normalize_status(_runtime_stage(runtime))
        if status == "running":
            api.pause_space(space_id)
            _hf_cache["spaces"] = None
            add_log("-", f"⏸️ Paused HF Space: {space_id}", "ok")
            return {"success": True, "message": f"Paused {space_id}", "status": "paused"}
        return {"success": False, "message": f"Space is not running (status: {status})", "status": status}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/hf/resume")
async def resume_space(body: HFSpaceAction):
    token = _hf_token()
    if not token:
        raise HTTPException(400, "HF_TOKEN not set")
    space_id = (body.space_id or "").strip()
    if not space_id:
        raise HTTPException(400, "space_id required")
    try:
        api = HfApi(token=token)
        api.restart_space(space_id)
        _hf_cache["spaces"] = None
        add_log("-", f"▶️ Resumed HF Space: {space_id}", "ok")
        return {"success": True, "message": f"Resumed {space_id}", "status": "running"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/hf/toggle")
async def toggle_space(body: HFSpaceAction):
    token = _hf_token()
    if not token:
        raise HTTPException(400, "HF_TOKEN not set")
    space_id = (body.space_id or "").strip()
    if not space_id:
        raise HTTPException(400, "space_id required")
    try:
        api = HfApi(token=token)
        runtime = api.get_space_runtime(space_id)
        status = _normalize_status(_runtime_stage(runtime))
        if status == "running":
            api.pause_space(space_id)
            new_status = "paused"
            add_log("-", f"⏸️ Paused HF Space: {space_id}", "ok")
        else:
            api.restart_space(space_id)
            new_status = "running"
            add_log("-", f"▶️ Resumed HF Space: {space_id}", "ok")
        _hf_cache["spaces"] = None
        return {"success": True, "status": new_status, "message": f"{space_id} -> {new_status}"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/hf/pause-all")
async def pause_all_spaces():
    token = _hf_token()
    if not token:
        raise HTTPException(400, "HF_TOKEN not set")
    try:
        api = HfApi(token=token)
        user = api.whoami()["name"]
        spaces = list(list_spaces(author=user, token=token))
        count = 0
        for space in spaces:
            try:
                runtime = api.get_space_runtime(space.id)
                status = _normalize_status(_runtime_stage(runtime))
                if status == "running":
                    api.pause_space(space.id)
                    count += 1
            except Exception:
                pass
        add_log("-", f"⏸️ Paused {count} HF Spaces", "ok")
        _hf_cache["spaces"] = None
        return {"success": True, "paused_count": count}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/hf/resume-all")
async def resume_all_spaces():
    token = _hf_token()
    if not token:
        raise HTTPException(400, "HF_TOKEN not set")
    try:
        api = HfApi(token=token)
        user = api.whoami()["name"]
        spaces = list(list_spaces(author=user, token=token))
        count = 0
        for space in spaces:
            try:
                runtime = api.get_space_runtime(space.id)
                status = _normalize_status(_runtime_stage(runtime))
                if status in ("paused", "stopped", "unknown"):
                    api.restart_space(space.id)
                    count += 1
            except Exception:
                pass
        add_log("-", f"▶️ Resumed {count} HF Spaces", "ok")
        _hf_cache["spaces"] = None
        return {"success": True, "resumed_count": count}
    except Exception as e:
        raise HTTPException(500, str(e))

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
                    join_mode=info["join_mode"],
                ))
            except Exception as e:
                add_log(info.get("meeting_code", "-"), f"Schedule fail: {e}", "err")
            save_state()

async def keep_session_alive():
    while True:
        await asyncio.sleep(10)

@app.on_event("startup")
async def startup_event():
    load_state()
    asyncio.create_task(schedule_checker())
    asyncio.create_task(keep_session_alive())
    if os.path.exists("zoom_session.json"):
        session_status.update({"logged_in": True, "message": "Session present", "last_checked": now_ist().isoformat()})
    tok = "SET" if _hf_token() else "MISSING"
    add_log("-", f"✅ Server started | HF_TOKEN={tok}", "ok")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if os.path.exists("dashboard.html"):
        with open("dashboard.html", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>Put dashboard.html next to server.py</h1>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(asgi_app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
