# ============================================
# ZOOM BOT CENTRAL – FULL + AUTO LOGIN + SESSION STATUS
# ============================================
import os
import uuid
import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel
import socketio
from playwright.async_api import async_playwright

# ========== IST ==========
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

# ========== SOCKET.IO ==========
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
meeting_groups = {}
scheduled_tasks = {}

# Session status
session_status = {
    "logged_in": False,
    "last_checked": None,
    "message": "Not checked yet",
    "login_in_progress": False
}

# Zoom credentials (Railway env se bhi le sakte ho)
ZOOM_EMAIL = os.environ.get("ZOOM_EMAIL", "mohdjunaidq@clickorbit.in")
ZOOM_PASSWORD = os.environ.get("ZOOM_PASSWORD", "Zoom@126")

# ========== MODELS ==========
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

# ========== LOGIN FUNCTION ==========
async def perform_zoom_login():
    global session_status
    session_status["login_in_progress"] = True
    session_status["message"] = "Login in progress..."

    try:
        if os.path.exists("zoom_session.json"):
            os.remove("zoom_session.json")
            print("🗑️ Old session deleted")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                print(f"Login attempt {attempt}/{max_attempts}")
                page = await context.new_page()
                try:
                    await page.goto("https://zoom.us/signin#/login", timeout=30000)
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(1)

                    if "signin" not in page.url:
                        storage = await context.storage_state()
                        with open("zoom_session.json", "w") as f:
                            json.dump(storage, f, indent=2)
                        await page.close()
                        await browser.close()
                        session_status.update({
                            "logged_in": True,
                            "last_checked": now_ist().isoformat(),
                            "message": "Logged in successfully",
                            "login_in_progress": False
                        })
                        print("✅ Login successful")
                        return True

                    email_field = await page.wait_for_selector('//*[@id="email"]', state="attached", timeout=10000)
                    await email_field.fill(ZOOM_EMAIL)
                    await page.click('//*[@id="signin_btn_next"]')
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_selector('//*[@id="password"]', state="attached", timeout=15000)
                    await asyncio.sleep(0.5)

                    password_field = page.locator('//*[@id="password"]')
                    await password_field.fill(ZOOM_PASSWORD)

                    try:
                        await page.click('//*[@id="js_btn_login"]/span', force=True)
                    except:
                        pass
                    await page.keyboard.press('Enter')
                    await page.evaluate("document.getElementById('js_btn_login')?.click()")
                    await asyncio.sleep(4)

                    if "signin" not in page.url:
                        storage = await context.storage_state()
                        with open("zoom_session.json", "w") as f:
                            json.dump(storage, f, indent=2)
                        await page.close()
                        await browser.close()
                        session_status.update({
                            "logged_in": True,
                            "last_checked": now_ist().isoformat(),
                            "message": "Logged in successfully",
                            "login_in_progress": False
                        })
                        print("✅ Login successful")
                        return True
                    else:
                        if await page.locator("text=An error with reCAPTCHA occurred").count() > 0:
                            print("⚠️ reCAPTCHA detected")
                        print(f"Attempt {attempt} failed")
                except Exception as e:
                    print(f"Login error: {e}")
                finally:
                    await page.close()

                if attempt < max_attempts:
                    await context.close()
                    context = await browser.new_context(
                        viewport={"width": 1280, "height": 800},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    await asyncio.sleep(2)

            await browser.close()
            session_status.update({
                "logged_in": False,
                "last_checked": now_ist().isoformat(),
                "message": "Login failed (reCAPTCHA or wrong credentials)",
                "login_in_progress": False
            })
            return False

    except Exception as e:
        session_status.update({
            "logged_in": False,
            "last_checked": now_ist().isoformat(),
            "message": f"Login error: {str(e)[:80]}",
            "login_in_progress": False
        })
        return False

# ========== SESSION CHECK ==========
async def check_session_status():
    global session_status
    if session_status.get("login_in_progress"):
        return

    if not os.path.exists("zoom_session.json"):
        session_status.update({
            "logged_in": False,
            "last_checked": now_ist().isoformat(),
            "message": "No session file found"
        })
        return

    try:
        with open("zoom_session.json", "r") as f:
            storage = json.load(f)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(storage_state=storage)
            page = await context.new_page()
            await page.goto("https://zoom.us/profile", timeout=20000)
            await asyncio.sleep(2)
            url = page.url
            await browser.close()

            if "signin" in url or "login" in url:
                session_status.update({
                    "logged_in": False,
                    "last_checked": now_ist().isoformat(),
                    "message": "Session expired – Logged Out"
                })
            else:
                session_status.update({
                    "logged_in": True,
                    "last_checked": now_ist().isoformat(),
                    "message": "Logged In"
                })
    except Exception as e:
        session_status.update({
            "logged_in": False,
            "last_checked": now_ist().isoformat(),
            "message": f"Check failed: {str(e)[:60]}"
        })

# ========== SOCKET EVENTS ==========
@sio.event
async def connect(sid, environ):
    print(f"[SIO] Connected: {sid}")

@sio.event
async def disconnect(sid):
    for wid, info in list(workers.items()):
        if info.get("sid") == sid:
            tasks_to_remove = [tid for tid, t in running_tasks.items() if t.get("worker_id") == wid]
            for tid in tasks_to_remove:
                meeting = running_tasks[tid].get("meeting_code")
                if meeting and meeting in meeting_groups and tid in meeting_groups[meeting]:
                    meeting_groups[meeting].remove(tid)
                    if not meeting_groups[meeting]:
                        del meeting_groups[meeting]
                del running_tasks[tid]
            workers[wid]["free_capacity"] = workers[wid]["max_capacity"]
            workers[wid]["sid"] = None
            workers[wid]["last_seen"] = now_ist().isoformat()
            print(f"[SIO] Worker {wid} disconnected → Capacity restored")
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
    else:
        workers[wid] = {
            "sid": sid,
            "max_capacity": max_cap,
            "free_capacity": max_cap,
            "last_seen": now
        }
    print(f"[SIO] Worker {wid} registered | capacity={max_cap}")
    await sio.emit("registered", {"worker_id": wid, "max_capacity": max_cap}, to=sid)

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

# ========== API ==========
@app.get("/health")
async def health():
    return {"ok": True, "time": now_ist().isoformat()}

@app.get("/session")
async def get_session():
    if not os.path.exists("zoom_session.json"):
        raise HTTPException(status_code=404, detail="Session not found")
    return FileResponse("zoom_session.json", media_type="application/json")

@app.get("/api/session-status")
async def api_session_status():
    return session_status

@app.post("/api/login")
async def api_login():
    if session_status.get("login_in_progress"):
        return {"success": False, "message": "Login already in progress"}
    success = await perform_zoom_login()
    return {
        "success": success,
        "message": session_status["message"],
        "logged_in": session_status["logged_in"]
    }

@app.get("/status")
@app.get("/api/status")
async def status():
    # Only connected workers
    connected_workers = {
        wid: info for wid, info in workers.items()
        if info.get("sid") is not None
    }
    total_free = sum(w.get("free_capacity", 0) for w in connected_workers.values())
    total_capacity = sum(w.get("max_capacity", 0) for w in connected_workers.values())

    meetings = {}
    for tid, task in list(running_tasks.items()):
        meeting = task.get("meeting_code", "unknown")
        if meeting not in meetings:
            meetings[meeting] = {
                "meeting_code": meeting,
                "total_bots": 0,
                "name_type": task.get("name_type", "indian"),
                "started_at": task.get("started_at"),
                "duration_minutes": task.get("duration_minutes", 120),
                "join_mode": task.get("join_mode", "individual")
            }
        meetings[meeting]["total_bots"] += task.get("bot_count", 0)
        if task.get("started_at") and (meetings[meeting]["started_at"] is None or task["started_at"] > meetings[meeting]["started_at"]):
            meetings[meeting]["started_at"] = task["started_at"]

    return {
        "workers": connected_workers,
        "total_capacity": total_capacity,
        "total_free_capacity": total_free,
        "meetings": meetings,
        "schedules": scheduled_tasks,
        "session": session_status,
        "timestamp": now_ist().isoformat()
    }

@app.post("/api/start-bots")
async def start_bots(req: StartBotRequest):
    if not session_status.get("logged_in"):
        raise HTTPException(400, "Zoom session not logged in. Please login first.")

    if req.bot_count < 1:
        raise HTTPException(400, "bot_count must be >= 1")

    meeting = req.meeting_code.strip().replace(" ", "")
    if not meeting:
        raise HTTPException(400, "meeting_code required")

    remaining = req.bot_count
    assigned = []
    connected = {wid: info for wid, info in workers.items() if info.get("sid")}

    sorted_workers = sorted(connected.items(), key=lambda x: x[1].get("free_capacity", 0), reverse=True)

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
            "join_mode": req.join_mode or "individual"
        }

        if meeting not in meeting_groups:
            meeting_groups[meeting] = []
        meeting_groups[meeting].append(task_id)

        workers[wid]["free_capacity"] = max(0, free - give)
        assigned.append({"worker": wid, "bots": give, "task_id": task_id})
        remaining -= give

    if not assigned:
        raise HTTPException(503, "No free capacity or no connected workers.")

    return {
        "success": True,
        "message": f"Started {req.bot_count - remaining} bots for {meeting}",
        "assigned": assigned,
        "remaining_unassigned": remaining
    }

@app.post("/api/schedule")
async def create_schedule(req: ScheduleRequest):
    try:
        schedule_time = datetime.fromisoformat(req.schedule_at.replace("Z", "+00:00"))
        if schedule_time.tzinfo is None:
            schedule_time = schedule_time.replace(tzinfo=IST)
        else:
            schedule_time = schedule_time.astimezone(IST)
    except Exception as e:
        raise HTTPException(400, f"Invalid schedule_at: {e}")

    if schedule_time <= now_ist():
        raise HTTPException(400, "Schedule time must be in future")

    sid = str(uuid.uuid4())[:8]
    scheduled_tasks[sid] = {
        "schedule_id": sid,
        "meeting_code": req.meeting_code.strip().replace(" ", ""),
        "passcode": req.passcode or "",
        "bot_count": req.bot_count,
        "duration_minutes": req.duration_minutes,
        "name_type": req.name_type or "indian",
        "custom_names": req.custom_names,
        "join_mode": req.join_mode or "individual",
        "schedule_at": schedule_time.isoformat(),
        "created_at": now_ist().isoformat()
    }
    return {"success": True, "schedule_id": sid, "message": "Scheduled successfully"}

@app.delete("/api/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    if schedule_id in scheduled_tasks:
        del scheduled_tasks[schedule_id]
        return {"success": True}
    raise HTTPException(404, "Schedule not found")

@app.post("/api/terminate")
async def terminate(req: Optional[TerminateRequest] = None):
    if req and req.meeting_code:
        meeting = req.meeting_code.strip().replace(" ", "")
        to_kill = [tid for tid, t in running_tasks.items() if t.get("meeting_code") == meeting]
        if not to_kill:
            raise HTTPException(404, f"No active tasks for {meeting}")
        for tid in to_kill:
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
            del running_tasks[tid]
        if meeting in meeting_groups:
            del meeting_groups[meeting]
        return {"success": True, "message": f"Meeting {meeting} terminated"}
    else:
        for tid in list(running_tasks.keys()):
            wid = running_tasks[tid].get("worker_id")
            if wid in workers and workers[wid].get("sid"):
                await sio.emit("terminate", {"task_id": tid}, to=workers[wid]["sid"])
            if wid and wid in workers:
                workers[wid]["free_capacity"] = min(
                    workers[wid]["max_capacity"],
                    workers[wid].get("free_capacity", 0) + running_tasks[tid].get("bot_count", 0)
                )
        running_tasks.clear()
        meeting_groups.clear()
        return {"success": True, "message": "All tasks terminated"}

# ========== BACKGROUND TASKS ==========
async def session_checker():
    while True:
        await check_session_status()
        await asyncio.sleep(30)   # har 30 second mein check

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
            except:
                continue
        for sid in to_run:
            info = scheduled_tasks.pop(sid)
            req = StartBotRequest(
                meeting_code=info["meeting_code"],
                passcode=info["passcode"],
                bot_count=info["bot_count"],
                duration_minutes=info["duration_minutes"],
                name_type=info["name_type"],
                custom_names=info["custom_names"],
                join_mode=info["join_mode"]
            )
            try:
                await start_bots(req)
            except Exception as e:
                print(f"[SCHEDULE] Failed: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(session_checker())
    asyncio.create_task(schedule_checker())
    # Initial check
    asyncio.create_task(check_session_status())
    print("✅ Background tasks started")

# ========== DASHBOARD HTML ==========
# (HTML next message mein dunga kyunki bohot lamba hai)
