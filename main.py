<!DOCTYPE html>
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
        grid-template-columns: 360px 1fr;
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

    /* Toggle Switch */
    .mode-toggle {
        display: flex;
        background: #0a1525;
        border-radius: 8px;
        padding: 4px;
        border: 1px solid #1e3a5f;
        margin-bottom: 14px;
    }
    .mode-btn {
        flex: 1;
        padding: 8px 0;
        text-align: center;
        border-radius: 6px;
        font-size: 13px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.2s;
        color: #89a9c9;
        user-select: none;
    }
    .mode-btn.active {
        background: #2a7acc;
        color: white;
    }
    .mode-btn:hover:not(.active) {
        background: #1a2a3a;
    }

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
        width: 170px;
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

    /* Highlight searched meeting */
    tr.highlight td {
        background: #1a3a5a !important;
        border-left: 4px solid #4a9eff;
    }
    tr.highlight {
        box-shadow: 0 0 0 1px #4a9eff;
    }

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
    .badge-mode {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 10px;
        font-weight: 500;
        border: 1px solid;
    }
    .badge-slow { border-color: #c29a4a; color: #c29a4a; }
    .badge-together { border-color: #6aaa6a; color: #6aaa6a; }

    .log {
        margin-top: 12px;
        padding: 8px 12px;
        background: #0a1525;
        border: 1px solid #1e3a5f;
        border-radius: 6px;
        font-size: 12px;
        color: #89a9c9;
        font-family: monospace;
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

            <!-- Slow / Together Toggle -->
            <div class="mode-toggle">
                <div class="mode-btn active" id="modeSlow" onclick="setMode('individual')">🐢 Slow</div>
                <div class="mode-btn" id="modeTogether" onclick="setMode('together')">⚡ Together</div>
            </div>

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
                    <input id="searchMeeting" placeholder="Search Meeting ID" oninput="filterMeetings()" />
                    <button class="btn btn-danger btn-sm" onclick="killBySearch()">Kill</button>
                </div>
                <span id="taskCount" style="background:#0a1525;padding:4px 12px;border-radius:20px;font-size:12px;color:#89a9c9;">0 meetings</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Meeting ID</th>
                        <th>Total Bots</th>
                        <th>Started (IST)</th>
                        <th>Mode</th>
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

let currentMode = 'individual';   // default Slow
let allMeetings = {};             // store for filtering

function setMode(mode) {
    currentMode = mode;
    $('modeSlow').classList.toggle('active', mode === 'individual');
    $('modeTogether').classList.toggle('active', mode === 'together');
}

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

function renderMeetings(meetings) {
    allMeetings = meetings;
    const search = ($('searchMeeting').value || '').trim().toLowerCase();

    let filtered = Object.entries(meetings);
    if (search) {
        filtered = filtered.filter(([m]) => m.toLowerCase().includes(search));
    }

    taskCount.textContent = Object.keys(meetings).length + ' meetings';

    if (!filtered.length) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#89a9c9;padding:20px;">No active meetings</td></tr>';
        return;
    }

    let idx = 0;
    tbody.innerHTML = filtered.map(([meeting, m]) => {
        idx++;
        const bots = m.total_bots || 0;
        const type = m.name_type || 'indian';
        const mode = m.join_mode || 'individual';
        const startTime = m.started_at ? new Date(m.started_at).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}) : '-';
        const typeBadge = type === 'indian' ? 'indian' : type === 'english' ? 'english' : 'custom';
        const modeBadge = mode === 'together' ? 'together' : 'slow';
        const modeText = mode === 'together' ? 'Together' : 'Slow';
        const isHighlight = search && meeting.toLowerCase().includes(search);

        return `<tr class="${isHighlight ? 'highlight' : ''}">
            <td>${idx}</td>
            <td class="meeting-id">${meeting}</td>
            <td><strong style="color:#ffd166">${bots}</strong></td>
            <td>${startTime}</td>
            <td><span class="badge-mode badge-${modeBadge}">${modeText}</span></td>
            <td><span class="badge-type badge-${typeBadge}">${type}</span></td>
            <td>
                <button class="btn btn-danger btn-sm" onclick="killMeeting('${meeting}')">Kill</button>
            </td>
        </tr>`;
    }).join('');
}

function filterMeetings() {
    renderMeetings(allMeetings);
}

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

        renderMeetings(meetings);
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
        show(`Starting ${bots} bots in ${currentMode === 'together' ? 'Together' : 'Slow'} mode...`, 'info');
        const r = await fetch(API+'/api/start-bots', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({
                meeting_code: meeting,
                passcode: pass,
                bot_count: bots,
                duration_minutes: dur,
                name_type: type,
                custom_names: custom,
                join_mode: currentMode          // ← Slow or Together
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
