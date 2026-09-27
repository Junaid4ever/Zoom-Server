# ============================================
# ZOOM BOT CENTRAL – Railway
# (Hugging Face removed)
# ============================================
import os, uuid, asyncio, json, signal, random, httpx, time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
import socketio

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
session_locked = False
session_apply = {}
SESSION_FILE = "zoom_session.json"
screenshot_enabled = False
screenshot_store = deque(maxlen=40)  # {id,tag,step,ts,b64}


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
            "session_locked": session_locked,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"save_state err: {e}", flush=True)


def load_state():
    global running_tasks, meeting_groups, scheduled_tasks, meeting_used_firsts, pause_state, session_locked
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
        session_locked = data.get("session_locked", False)
        print(f"[STATE] restored meetings={len(meeting_groups)} tasks={len(running_tasks)} pause={pause_state} lock={session_locked}", flush=True)
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
    await sio.emit("registered", {"worker_id": wid, "max_capacity": max_cap, "screenshots": screenshot_enabled}, to=sid)
    await sio.emit("screenshot_toggle", {"enabled": screenshot_enabled}, to=sid)
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE) as f:
                sess = json.load(f)
            await sio.emit("session_payload", {"session": sess, "locked": session_locked}, to=sid)
        except Exception:
            pass


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


@sio.event
async def session_applied(sid, data):
    wid = data.get("worker_id") or next((w for w, i in workers.items() if i.get("sid") == sid), None)
    if not wid:
        return
    session_apply[wid] = {
        "ok": True,
        "ts": now_ist().isoformat(),
        "cookies": int(data.get("cookies") or 0),
    }
    add_log("-", f"Session applied on {wid} ({session_apply[wid]['cookies']} cookies)", "ok")


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/session")
async def get_session():
    if not os.path.exists(SESSION_FILE):
        raise HTTPException(404, "Session not found")
    return FileResponse(SESSION_FILE, media_type="application/json")


@app.get("/api/session-status")
async def api_session_status():
    session_status["logged_in"] = os.path.exists(SESSION_FILE)
    session_status["message"] = "Session file present" if session_status["logged_in"] else "No session file"
    session_status["last_checked"] = now_ist().isoformat()
    return session_status


async def push_session_github(data: dict):
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN not set on Railway"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    path = "zoom_session.json"
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"
    content = json.dumps(data, indent=2)
    import base64
    b64 = base64.b64encode(content.encode()).decode()
    sha = None
    async with httpx.AsyncClient(timeout=30) as client:
        cur = await client.get(url, headers=headers, params={"ref": GITHUB_BRANCH})
        if cur.status_code == 200:
            sha = cur.json().get("sha")
        body = {
            "message": f"session lock {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}",
            "content": b64,
            "branch": GITHUB_BRANCH,
        }
        if sha:
            body["sha"] = sha
        put = await client.put(url, headers=headers, json=body)
        if put.status_code in (200, 201):
            return True, put.json().get("content", {}).get("html_url") or "ok"
        return False, f"GitHub {put.status_code}: {put.text[:200]}"


@app.post("/api/update-session")
async def update_session(request: Request):
    global session_locked
    if session_locked:
        raise HTTPException(423, "Session locked. Unlock first to replace JSON.")
    data = await request.json()
    if not isinstance(data, dict) or "cookies" not in data:
        raise HTTPException(400, "Invalid JSON")
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f, indent=2)
    session_locked = True
    session_apply.clear()
    session_status.update({"logged_in": True, "message": "Session locked ✓", "last_checked": now_ist().isoformat()})
    add_log("-", f"✅ Session saved ({len(data.get('cookies') or [])} cookies) — locked", "ok")
    save_state()
    connected = 0
    for wid, info in workers.items():
        if info.get("sid"):
            connected += 1
            session_apply[wid] = {"ok": False, "ts": None, "cookies": 0}
            await sio.emit("session_payload", {"session": data, "locked": True}, to=info["sid"])
    return {
        "success": True,
        "locked": True,
        "workers_notified": connected,
        "message": "Session saved, locked, sent to workers",
    }


@app.post("/api/logs/clear")
async def clear_logs(meeting: str = None):
    if meeting:
        meeting_logs.pop(meeting, None)
    else:
        global_logs.clear()
        meeting_logs.clear()
    return {"success": True}


@app.get("/api/screenshots")
async def list_shots():
    return {
        "enabled": screenshot_enabled,
        "items": [
            {"id": x["id"], "tag": x["tag"], "step": x["step"], "ts": x["ts"], "data": x["b64"]}
            for x in list(screenshot_store)[-24:]
        ],
    }


@app.post("/api/screenshots/toggle")
async def toggle_shots(request: Request):
    global screenshot_enabled
    body = await request.json()
    screenshot_enabled = bool(body.get("enabled"))
    for info in workers.values():
        if info.get("sid"):
            await sio.emit("screenshot_toggle", {"enabled": screenshot_enabled}, to=info["sid"])
    add_log("-", f"Screenshots {'ON' if screenshot_enabled else 'OFF'}", "ok")
    return {"success": True, "enabled": screenshot_enabled}


@app.post("/api/screenshots/upload")
async def upload_shot(request: Request):
    if not screenshot_enabled:
        return {"ok": False, "skipped": True}
    body = await request.json()
    screenshot_store.append({
        "id": str(uuid.uuid4())[:8],
        "tag": body.get("tag") or "",
        "step": body.get("step") or "",
        "ts": now_ist().strftime("%H:%M:%S"),
        "b64": body.get("data") or "",
    })
    return {"ok": True}


@app.delete("/api/screenshots")
async def wipe_shots():
    screenshot_store.clear()
    return {"success": True}


@app.post("/api/session/unlock")
async def unlock_session():
    global session_locked
    session_locked = False
    save_state()
    add_log("-", "🔓 Session unlocked — new JSON allowed", "warn")
    return {"success": True, "locked": False}


@app.get("/api/session-apply-status")
async def session_apply_status():
    connected = [w for w, i in workers.items() if i.get("sid")]
    total = len(connected) or 0
    applied = sum(1 for w in connected if session_apply.get(w, {}).get("ok"))
    percent = int(round((applied / total) * 100)) if total else (100 if os.path.exists(SESSION_FILE) else 0)
    return {
        "locked": session_locked,
        "has_file": os.path.exists(SESSION_FILE),
        "total_workers": total,
        "applied": applied,
        "percent": percent,
        "github_token_set": bool(GITHUB_TOKEN),
        "workers": {
            w: session_apply.get(w) or {"ok": False, "ts": None, "cookies": 0}
            for w in connected
        },
    }


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
        "session_locked": session_locked,
        "screenshot_enabled": screenshot_enabled,
    }


@app.post("/api/start-bots")
async def start_bots(req: StartBotRequest):
    if pause_state:
        raise HTTPException(503, "System is globally paused. Resume first.")
    if not os.path.exists(SESSION_FILE):
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
                add_log(info.get("meeting_code", "-"), "📅 Schedule due — starting bots", "info")
                await start_bots(StartBotRequest(
                    meeting_code=info["meeting_code"], passcode=info["passcode"],
                    bot_count=info["bot_count"], duration_minutes=info["duration_minutes"],
                    name_type=info["name_type"], custom_names=info["custom_names"],
                    join_mode=info["join_mode"],
                ))
            except Exception as e:
                add_log(info.get("meeting_code", "-"), f"Schedule fail: {e}", "err")
            save_state()


def _zoom_cookie_jar(data):
    jar = httpx.Cookies()
    for c in data.get("cookies") or []:
        name, val = c.get("name"), c.get("value")
        if not name or val is None:
            continue
        domain = (c.get("domain") or ".zoom.us").lstrip(".")
        path = c.get("path") or "/"
        try:
            jar.set(name, val, domain=domain, path=path)
        except Exception:
            try:
                jar.set(name, val)
            except Exception:
                pass
    return jar


async def keep_session_alive():
    urls = [
        "https://zoom.us/",
        "https://www.zoom.us/",
        "https://zoom.us/profile",
        "https://www.zoom.us/account",
    ]
    i = 0
    while True:
        await asyncio.sleep(15)
        try:
            if not os.path.exists(SESSION_FILE):
                session_status.update({
                    "logged_in": False,
                    "message": "No session file",
                    "last_checked": now_ist().isoformat(),
                })
                continue
            with open(SESSION_FILE) as f:
                data = json.load(f)
            cookies = data.get("cookies") or []
            if not cookies:
                session_status.update({
                    "logged_in": False,
                    "message": "Session has no cookies",
                    "last_checked": now_ist().isoformat(),
                })
                continue
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            url = urls[i % len(urls)]
            i += 1
            async with httpx.AsyncClient(
                timeout=20, follow_redirects=True, cookies=_zoom_cookie_jar(data), headers=headers
            ) as client:
                r = await client.get(url)
            updated = False
            now_ts = int(time.time())
            for sc in r.cookies.jar:
                hit = None
                for c in cookies:
                    if c.get("name") == sc.name:
                        hit = c
                        break
                if hit:
                    if hit.get("value") != sc.value:
                        hit["value"] = sc.value
                        updated = True
                    hit["expires"] = now_ts + 14 * 86400
                    updated = True
                else:
                    cookies.append({
                        "name": sc.name,
                        "value": sc.value,
                        "domain": sc.domain or ".zoom.us",
                        "path": sc.path or "/",
                        "expires": now_ts + 14 * 86400,
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    })
                    updated = True
            for c in cookies:
                exp = c.get("expires") or 0
                try:
                    exp = int(exp)
                except Exception:
                    exp = 0
                if exp < now_ts + 3 * 86400:
                    c["expires"] = now_ts + 14 * 86400
                    updated = True
            if updated:
                data["cookies"] = cookies
                with open(SESSION_FILE, "w") as f:
                    json.dump(data, f)
            ok = r.status_code < 400
            session_status.update({
                "logged_in": True if cookies else False,
                "message": f"Session ping {r.status_code}" + (" + cookies saved" if updated else ""),
                "last_checked": now_ist().isoformat(),
            })
            if i % 8 == 0:
                add_log("-", f"🔄 Zoom session ping {r.status_code} ({url})", "ok" if ok else "err")
        except Exception as e:
            session_status.update({
                "message": f"Session ping err: {str(e)[:80]}",
                "last_checked": now_ist().isoformat(),
            })


@app.on_event("startup")
async def startup_event():
    load_state()
    asyncio.create_task(schedule_checker())
    asyncio.create_task(keep_session_alive())
    if os.path.exists(SESSION_FILE):
        session_status.update({"logged_in": True, "message": "Session present", "last_checked": now_ist().isoformat()})
    add_log("-", "✅ Server started", "ok")


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if os.path.exists("dashboard.html"):
        with open("dashboard.html", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>Put dashboard.html next to main.py</h1>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(asgi_app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
