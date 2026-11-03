import os, sqlite3, time, uuid, json, hashlib
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

DB = os.getenv("DATABASE_PATH", "/data/opencompute.db")
KEY = os.getenv("ADMIN_KEY", "dev-only-change-me")
LEASE_SECONDS = 30
app = FastAPI(title="OpenCompute AI", version="0.1.0")

def db():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB, timeout=15, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, name TEXT, capabilities TEXT, last_seen REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, kind TEXT, payload TEXT, status TEXT, node_id TEXT, lease_token TEXT, lease_until REAL, attempts INTEGER DEFAULT 0, result TEXT, created REAL)")
    return c

def auth(x_api_key: str = Header(default="")):
    if x_api_key != KEY:
        raise HTTPException(401, "Invalid API key")

class Node(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    name: str = Field(min_length=1, max_length=100)
    capabilities: list[str] = Field(default_factory=lambda: ["sum", "sha256"])

class Task(BaseModel):
    kind: str
    payload: dict

class Complete(BaseModel):
    node_id: str
    lease_token: str
    result: dict

@app.on_event("startup")
def startup():
    db().close()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/nodes/register", dependencies=[Depends(auth)])
def register(n: Node):
    with db() as c:
        c.execute("INSERT INTO nodes VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,capabilities=excluded.capabilities,last_seen=excluded.last_seen",
                  (n.id, n.name, json.dumps(n.capabilities), time.time()))
    return {"ok": True}

@app.post("/nodes/{node_id}/heartbeat", dependencies=[Depends(auth)])
def heartbeat(node_id: str):
    with db() as c:
        r = c.execute("UPDATE nodes SET last_seen=? WHERE id=?", (time.time(), node_id))
        if not r.rowcount:
            raise HTTPException(404, "Node missing")
    return {"ok": True}

@app.post("/tasks", dependencies=[Depends(auth)])
def create_task(t: Task):
    if t.kind == "sum":
        nums = t.payload.get("numbers")
        if not isinstance(nums, list) or len(nums) > 10000 or any(type(x) not in (int, float) for x in nums):
            raise HTTPException(400, "Invalid numbers")
    elif t.kind == "sha256":
        s = t.payload.get("text")
        if not isinstance(s, str) or len(s) > 100000:
            raise HTTPException(400, "Invalid text")
    else:
        raise HTTPException(400, "Unsupported task")
    tid = str(uuid.uuid4())
    with db() as c:
        c.execute("INSERT INTO tasks(id,kind,payload,status,created) VALUES(?,?,?,?,?)",
                  (tid, t.kind, json.dumps(t.payload), "queued", time.time()))
    return {"id": tid, "status": "queued"}

@app.post("/tasks/claim", dependencies=[Depends(auth)])
def claim(node_id: str):
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        node = c.execute("SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
        now = time.time()
        if not node or now - node["last_seen"] > 60:
            raise HTTPException(403, "Node offline or unknown")
        caps = json.loads(node["capabilities"])
        rows = c.execute("SELECT * FROM tasks WHERE status='queued' OR (status='leased' AND lease_until<?) ORDER BY created LIMIT 100", (now,)).fetchall()
        task = next((t for t in rows if t["kind"] in caps and t["attempts"] < 3), None)
        if task is None:
            c.execute("COMMIT")
            return {"task": None}
        token = str(uuid.uuid4())
        c.execute("UPDATE tasks SET status='leased',node_id=?,lease_token=?,lease_until=?,attempts=attempts+1 WHERE id=?",
                  (node_id, token, now + LEASE_SECONDS, task["id"]))
        c.execute("COMMIT")
        return {"task": {"id": task["id"], "kind": task["kind"], "payload": json.loads(task["payload"]), "lease_token": token}}
    except Exception:
        if c.in_transaction:
            c.execute("ROLLBACK")
        raise
    finally:
        c.close()

@app.post("/tasks/{task_id}/complete", dependencies=[Depends(auth)])
def complete(task_id: str, body: Complete):
    with db() as c:
        r = c.execute("UPDATE tasks SET status='completed',result=?,lease_until=NULL WHERE id=? AND status='leased' AND node_id=? AND lease_token=? AND lease_until>=?",
                      (json.dumps(body.result), task_id, body.node_id, body.lease_token, time.time()))
        if not r.rowcount:
            raise HTTPException(409, "Lease expired, duplicate, or invalid")
    return {"ok": True}

@app.get("/cluster/status", dependencies=[Depends(auth)])
def status():
    with db() as c:
        nodes = [dict(x) for x in c.execute("SELECT * FROM nodes ORDER BY name")]
        tasks = [dict(x) for x in c.execute("SELECT id,kind,status,node_id,attempts,result,created FROM tasks ORDER BY created DESC LIMIT 100")]
    for n in nodes:
        n["online"] = time.time() - n["last_seen"] < 60
        n["capabilities"] = json.loads(n["capabilities"])
    return {"nodes": nodes, "tasks": tasks, "counts": {s: sum(t["status"] == s for t in tasks) for s in ("queued", "leased", "completed")}}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """<!doctype html><html lang="zh"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OpenCompute AI</title>
<style>body{font:15px system-ui;background:#0b1220;color:#e8efff;max-width:960px;margin:40px auto;padding:20px}section{background:#18243a;padding:20px;border-radius:14px;margin:16px 0}input,button{padding:11px;border-radius:8px;border:0;margin:5px}button{background:#5c9aff;color:white;cursor:pointer}pre{white-space:pre-wrap;overflow-wrap:anywhere}small{color:#9eb0cb}</style>
<h1>◈ OpenCompute AI</h1><small>Phase 1 · Task dashboard</small><section><input id="key" type="password" placeholder="开发 API Key"><button onclick="load()">刷新状态</button><p id="stats">等待连接</p></section><section><h2>提交测试任务</h2><button onclick="submitTask()">计算 1 + 2 + 3</button><p id="msg"></p></section><section><h2>节点与任务</h2><pre id="data">尚无数据</pre></section>
<script>async function req(path,opts={}){let r=await fetch(path,{...opts,headers:{'Content-Type':'application/json','X-API-Key':document.getElementById('key').value}});if(!r.ok)throw Error(await r.text());return r.json()}async function load(){try{let d=await req('/cluster/status');document.getElementById('stats').textContent='在线节点 '+d.nodes.filter(x=>x.online).length+' · 已完成 '+d.counts.completed+' · 等待 '+d.counts.queued;document.getElementById('data').textContent=JSON.stringify(d,null,2)}catch(e){document.getElementById('stats').textContent=e.message}}async function submitTask(){try{let d=await req('/tasks',{method:'POST',body:JSON.stringify({kind:'sum',payload:{numbers:[1,2,3]}})});document.getElementById('msg').textContent='任务已提交 '+d.id;load()}catch(e){document.getElementById('msg').textContent=e.message}}</script></html>"""
