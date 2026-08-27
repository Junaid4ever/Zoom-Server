# ============================================
# ZOOM BOT CENTRAL – FINAL (Railway)
# persist state | unique names | hard kill | schedule
# ============================================
import os, uuid, asyncio, json, signal, random
from collections import deque
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

workers, running_tasks, meeting_groups, scheduled_tasks = {}, {}, {}, {}
session_status = {"logged_in": False, "last_checked": None, "message": "No session file", "login_in_progress": False}
meeting_logs, global_logs = {}, deque(maxlen=400)
meeting_used_firsts = {}
STATE_FILE = "bot_state.json"
BANNED_FIRSTS = {"katappa", "mj", "m j", "m.j"}

INDIAN_FIRST_POOL = [
    "Aarav","Vivaan","Aditya","Vihaan","Arjun","Reyansh","Ayaan","Krishna","Ishaan","Shaurya",
    "Rahul","Rohan","Priya","Ananya","Diya","Saanvi","Aadhya","Kavya","Riya","Anika",
    "Amit","Rajesh","Sneha","Pooja","Neha","Vikram","Karan","Manish","Suresh","Deepak",
    "Ankit","Siddharth","Kunal","Nikhil","Harsh","Yash","Aryan","Kabir","Dev","Rudra",
    "Atharv","Darsh","Ira","Myra","Anvi","Kiara","Pari","Navya","Shanaya","Veer",
    "Om","Yuvraj","Pranav","Dhruv","Kartik","Laksh","Rian","Samar","Advait","Arnav",
    "Raghav","Tanvi","Isha","Meera","Sara","Aisha","Zara","Inaaya","Anaya","Kyra",
    "Rishi","Varun","Nitin","Pankaj","Gaurav","Sachin","Rohit","Vivek","Ashish","Mohit",
    "Sunita","Kavita","Jyoti","Rekha","Geeta","Seema","Nisha","Ritu","Swati","Preeti",
    "Farhan","Imran","Zoya","Rehan","Samir","Aamir","Naina","Alisha","Nirvaan","Aarush",
    "Viaan","Shivansh","Parth","Yug","Agastya","Viraj","Shaunak","Kiaan","Ahaan","Neel",
    "Avi","Daksh","Shiven","Aariv","Kian","Arhaan","Zayan","Ivaan","Ansh","Riyan",
    "Jai","Veeraj","Saanvika","Aarohi","Amaira","Siya","Avni","Meher","Noor","Sana",
    "Alia","Kriti","Shreya","Aditi","Ishita","Tanya","Ritika","Pallavi","Shruti","Nikita",
    "Komal","Payal","Ruchi","Manav","Aman","Sahil","Akash","Naveen","Sanjay","Ramesh",
    "Mahesh","Dinesh","Rakesh","Naresh","Mukesh","Paresh","Abhinav","Abhishek","Aakash","Anuj",
    "Ashutosh","Bhavesh","Chirag","Darshan","Eshan","Faisal","Gagan","Hemant","Inder","Jatin",
    "Kaushal","Lalit","Mayank","Naman","Onkar","Pratik","Rajat","Sagar","Tarun","Uday",
    "Vinay","Wasim","Yatin","Zeeshan","Arpit","Bhavya","Charu","Divya","Ekta","Garima",
    "Heena","Ipsita","Jaya","Kirti","Lavanya","Madhavi","Nandini","Pragya","Radhika","Sakshi",
    "Trisha","Urvashi","Vaishnavi","Yamini","Ayesha","Bhavika","Chhavi","Drishti","Eshita","Fiza",
    "Gunjan","Hrithik","Ishan","Jitesh","Kshitij","Lokesh","Mihir","Niraj","Omkar","Pranay",
    "Rishabh","Tushar","Utkarsh","Vishal","Anshul","Brijesh","Chetan","Devansh","Eshaan","Faraz",
    "Girish","Hitesh","Iqbal","Jaspreet","Luv","Manjot","Navjot","Puneet","Simran","Ujjwal",
    "Harpreet","Gurpreet","Jasleen","Manpreet","Navdeep","Amrit","Aaradhya","Bhavna","Chandni","Deepika",
    "Gayatri","Hema","Indira","Janhvi","Kajal","Lata","Mamta","Namrata","Prerna","Rashmi",
    "Sonal","Tanisha","Vandana","Yashika","Chitra","Damini","Fatima","Oviya",
]
EN_FIRST_POOL = [
    "James","John","Michael","David","Emily","Emma","Olivia","Daniel","Matthew","Sarah",
    "Robert","William","Joseph","Thomas","Charles","Christopher","Andrew","Joshua","Ryan","Justin",
    "Ethan","Noah","Liam","Mason","Logan","Lucas","Jackson","Aiden","Sebastian","Jack",
    "Sophia","Isabella","Mia","Charlotte","Amelia","Harper","Evelyn","Abigail","Ella","Scarlett",
    "Henry","Alexander","Samuel","Benjamin","Gabriel","Carter","Wyatt","Julian","Grayson","Leo",
    "Grace","Chloe","Victoria","Riley","Aria","Lily","Zoey","Nora","Hazel","Aurora",
    "Owen","Caleb","Isaac","Luke","Nathan","Aaron","Adam","Adrian","Alan","Albert",
    "Alice","Amy","Andrea","Angela","Anna","Anne","Ashley","Barbara","Betty","Brenda",
    "Brian","Bruce","Bryan","Carl","Carol","Catherine","Christine","Cynthia","Deborah","Denise",
    "Dennis","Diana","Diane","Donald","Donna","Doris","Dorothy","Douglas","Edward","Elizabeth",
    "Eric","Frances","Frank","Fred","Gary","George","Gerald","Gloria","Gregory","Harold",
    "Helen","Irene","Janet","Janice","Jean","Jeffrey","Jennifer","Jeremy","Jerry","Jesse",
    "Joan","Joe","Johnny","Jonathan","Jordan","Jose","Joyce","Juan","Judith","Judy",
    "Julia","Julie","Karen","Kathleen","Kathryn","Keith","Kelly","Kenneth","Kevin","Kimberly",
    "Larry","Laura","Lawrence","Linda","Lisa","Lois","Louis","Louise","Margaret","Maria",
    "Marie","Marilyn","Mark","Martha","Martin","Mary","Melissa","Michelle","Mildred","Nancy",
    "Natalie","Nicholas","Nicole","Norman","Pamela","Patricia","Patrick","Paul","Paula","Peter",
    "Philip","Rachel","Ralph","Raymond","Rebecca","Richard","Roger","Ronald","Rose","Roy",
    "Russell","Ruth","Samantha","Sandra","Sara","Scott","Sean","Sharon","Shirley","Stephanie",
    "Stephen","Steven","Susan","Teresa","Terry","Theresa","Timothy","Tina","Todd","Troy",
    "Victor","Virginia","Walter","Wayne","Wendy","Elliot","Silas","Clara","Mila","Lila","Ezra",
]
_INDIAN_PREFIX = ["Aa","Vi","Ad","Ar","Re","Kr","Is","Sh","Ra","Ro","An","Di","Sa","Ka","Ri","Am","Ne","Su","De","Si","Om","Yu","Pr","Dh","La","Ha","Na","Ja","Ma","Pa","Ta","Ga","Ve","Ch","Bh","Tr"]
_INDIAN_SUFFIX = ["rav","haan","itya","jun","ansh","ish","aurya","hul","han","anya","ya","anvi","vya","it","esh","epak","vik","yan","eet","isha","ika","ita","ani","eep","adev"]

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
        }
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"save_state err: {e}", flush=True)

def load_state():
    global running_tasks, meeting_groups, scheduled_tasks, meeting_used_firsts
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        running_tasks.update(data.get("running_tasks") or {})
        meeting_groups.update(data.get("meeting_groups") or {})
        scheduled_tasks.update(data.get("scheduled_tasks") or {})
        meeting_used_firsts.update({k: set(v) for k, v in (data.get("meeting_used_firsts") or {}).items()})
        print(f"[STATE] restored meetings={len(meeting_groups)} tasks={len(running_tasks)}", flush=True)
    except Exception as e:
        print(f"load_state err: {e}", flush=True)

def _synthetic_indian_first(used: set) -> str:
    for _ in range(300):
        name = random.choice(_INDIAN_PREFIX) + random.choice(_INDIAN_SUFFIX)
        name = name[0].upper() + name[1:].lower()
        key = name.lower()
        if key not in used and key not in BANNED_FIRSTS and "katappa" not in key and not key.startswith("user") and len(name) >= 4:
            return name
    return "Aarav" + random.choice(["esh", "ansh", "yan", "ika"])

def allocate_unique_firsts(meeting: str, count: int, name_type: str) -> List[str]:
    used = meeting_used_firsts.setdefault(meeting, set())
    pool = list(EN_FIRST_POOL if name_type == "english" else INDIAN_FIRST_POOL)
    random.shuffle(pool)
    out = []
    for f in pool:
        if len(out) >= count:
            break
        key = f.lower()
        if key not in used and key not in BANNED_FIRSTS and "katappa" not in key and not key.startswith("user"):
            used.add(key)
            out.append(f)
    while len(out) < count:
        if name_type == "english":
            name = random.choice(["Elliot", "Owen", "Silas", "Clara", "Mila", "Nora", "Lila", "Ezra", "Miles", "Chloe"])
            if name.lower() in used:
                name = random.choice(["Alex", "Sam", "Jordan", "Casey", "Riley", "Quinn"]) + random.choice(["a", "e", "y", ""])
        else:
            name = _synthetic_indian_first(used)
        key = name.lower()
        if key not in used and key not in BANNED_FIRSTS and "katappa" not in key and not key.startswith("user"):
            used.add(key)
            out.append(name)
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
            add_log("-", f"Worker {wid} disconnected | {len(orphan)} task(s) reserved — Kill to free", "err")
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
    remaining, assigned = req.bot_count, []
    name_type = req.name_type or "indian"

    if name_type == "custom" and req.custom_names:
        firsts, used_local = [], meeting_used_firsts.setdefault(meeting, set())
        for raw in req.custom_names:
            if len(firsts) >= req.bot_count:
                break
            token = (raw.strip().split() or ["Aarav"])[0]
            key = token.lower()
            if key in used_local or key in BANNED_FIRSTS or "katappa" in key or key.startswith("user"):
                continue
            used_local.add(key)
            firsts.append(raw.strip())
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
            "task_id": task_id, "meeting_code": meeting, "passcode": passcode, "bot_count": give,
            "duration_minutes": req.duration_minutes, "name_type": name_type,
            "custom_names": custom_slice, "assigned_first_names": slice_firsts,
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
    add_log(meeting, f"🚀 Started {started} bots | unique names | mode={req.join_mode}", "ok")
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
        "schedule_id": sid, "meeting_code": req.meeting_code.strip().replace(" ", ""),
        "passcode": "" if req.passcode is None else str(req.passcode), "bot_count": req.bot_count,
        "duration_minutes": req.duration_minutes, "name_type": req.name_type or "indian",
        "custom_names": req.custom_names, "join_mode": req.join_mode or "individual",
        "schedule_at": st.isoformat(), "created_at": now_ist().isoformat(),
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

@app.on_event("startup")
async def startup_event():
    load_state()
    asyncio.create_task(schedule_checker())
    if os.path.exists("zoom_session.json"):
        session_status.update({"logged_in": True, "message": "Session present", "last_checked": now_ist().isoformat()})
    add_log("-", "✅ Server started (state restored)", "ok")

DASHBOARD_HTML = open("dashboard.html", encoding="utf-8").read() if os.path.exists("dashboard.html") else """<!DOCTYPE html>
<html><body style="font-family:sans-serif;background:#0b1220;color:#eef5ff;padding:24px">
<h1>Zoom Command Center</h1>
<p>Put Control Deck HTML in <b>dashboard.html</b> next to main.py</p>
<p>Bots column: <code>active_bots / total_bots</code></p>
</body></html>"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if os.path.exists("dashboard.html"):
        with open("dashboard.html", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse(DASHBOARD_HTML)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(asgi_app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
