#!/usr/bin/env python3
# queue_client_front.py — Backend WS bridge + REST, com status e logs em tempo real

import os
import time
import uuid
import json
import pickle
import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple, Set

import requests
from fastapi import (
    FastAPI,
    HTTPException,
    UploadFile,
    File,
    Request,
    WebSocket,
    WebSocketDisconnect,
    Form,
)
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# ===================== DEBUG HELPERS =====================

DEBUG = os.getenv("DEBUG_QUEUE_FRONT", "1").lower() in ("1", "true", "yes")

def dbg(*args):
    if DEBUG:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"[dbg {ts}]", *args)

# ===================== CLIENTE WS RESP (inline) =====================

import websockets

class RedisRESPError(Exception):
    pass

class AsyncRedisWS:
    """
    Cliente mínimo RESP sobre WebSocket.
    Suporta: + - : $ * (simple string, error, integer, bulk, array)
    """
    def __init__(self, url: str, token: str = "", connect_timeout: float = 10.0, name: str = "cmd"):
        self.url = f"{url}?token={token}" if token else url
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._lock = asyncio.Lock()
        self._connect_timeout = connect_timeout
        self._name = name

    async def connect(self):
        if self._ws is not None and not getattr(self._ws, "closed", False):
            return
        dbg(f"[redis-{self._name}] connecting -> {self.url}")
        self._ws = await websockets.connect(self.url, max_size=None)
        dbg(f"[redis-{self._name}] connected")

    async def aclose(self):
        if self._ws is not None:
            try:
                await self._ws.close()
                dbg(f"[redis-{self._name}] closed")
            except Exception as e:
                dbg(f"[redis-{self._name}] close error:", repr(e))
            self._ws = None

    def _encode_cmd(self, *parts: Union[str, bytes, int, float]) -> bytes:
        out = bytearray()
        out.extend(f"*{len(parts)}\r\n".encode())
        for p in parts:
            if isinstance(p, (bytes, bytearray)):
                s = bytes(p)
            else:
                s = str(p).encode()
            out.extend(f"${len(s)}\r\n".encode())
            out.extend(s)
            out.extend(b"\r\n")
        return bytes(out)

    def _read_line(self, buf: bytearray, start: int = 0) -> Tuple[bytes, int]:
        i = buf.find(b"\r\n", start)
        if i == -1:
            raise RedisRESPError("incomplete")
        return bytes(buf[start:i]), i + 2

    def _parse_resp(self, data: bytes) -> Any:
        buf = bytearray(data)
        pos = 0

        def parse_at():
            nonlocal pos
            if pos >= len(buf):
                raise RedisRESPError("empty")
            prefix = chr(buf[pos]); pos += 1

            if prefix == "+":
                line, pos2 = self._read_line(buf, pos); pos = pos2
                return line.decode()

            if prefix == "-":
                line, pos2 = self._read_line(buf, pos); pos = pos2
                raise RedisRESPError(line.decode())

            if prefix == ":":
                line, pos2 = self._read_line(buf, pos); pos = pos2
                return int(line.decode())

            if prefix == "$":
                line, pos2 = self._read_line(buf, pos); pos = pos2
                n = int(line.decode())
                if n == -1:
                    return None
                res = bytes(buf[pos : pos + n])
                pos += n + 2
                return res.decode(errors="replace")

            if prefix == "*":
                line, pos2 = self._read_line(buf, pos); pos = pos2
                n = int(line.decode())
                if n == -1:
                    return None
                arr = []
                for _ in range(n):
                    arr.append(parse_at())
                return arr

            raise RedisRESPError("unknown prefix")
        return parse_at()

    async def _send_raw(self, data: bytes):
        if self._ws is None or getattr(self._ws, "closed", False):
            await self.connect()
        await self._ws.send(data)

    async def _recv_raw(self) -> bytes:
        if self._ws is None or getattr(self._ws, "closed", False):
            await self.connect()
        msg = await self._ws.recv()
        return msg if isinstance(msg, (bytes, bytearray)) else msg.encode()

    async def execute(self, *parts: Union[str, int, float, bytes]) -> Any:
        async with self._lock:
            payload = self._encode_cmd(*parts)
            try:
                await self._send_raw(payload)
                data = await self._recv_raw()
            except Exception:
                await self.aclose()
                raise

            if not data or data[:1] not in b"+-:$*":
                await self.aclose()
                return None

            try:
                return self._parse_resp(data)
            except RedisRESPError:
                await self.aclose()
                return None

    # ===== Comandos =====
    async def ping(self) -> bool:
        r = await self.execute("PING")
        return (r == "PONG") or (r is not None)

    async def get(self, key: str) -> Optional[str]:
        return await self.execute("GET", key)

    async def set(self, key: str, value: Union[str, bytes], ex: Optional[int] = None):
        if ex is None:
            return await self.execute("SET", key, value)
        return await self.execute("SET", key, value, "EX", int(ex))

    async def delete(self, *keys: str):
        return await self.execute("DEL", *keys)

    async def sadd(self, key: str, *members: str):
        return await self.execute("SADD", key, *members)

    async def srem(self, key: str, *members: str):
        return await self.execute("SREM", key, *members)

    async def smembers(self, key: str) -> List[str]:
        r = await self.execute("SMEMBERS", key)
        return [] if not r else [str(x) for x in r]

    async def rpush(self, key: str, *values: str):
        return await self.execute("RPUSH", key, *values)

    async def lpush(self, key: str, *values: str):
        return await self.execute("LPUSH", key, *values)

    async def llen(self, key: str) -> int:
        r = await self.execute("LLEN", key)
        return int(r or 0)

    async def lrange(self, key: str, start: int, stop: int) -> List[str]:
        r = await self.execute("LRANGE", key, start, stop)
        return [] if not r else [str(x) for x in r]

    async def lpop(self, key: str) -> Optional[str]:
        r = await self.execute("LPOP", key)
        return None if r is None else str(r)

    async def lindex(self, key: str, index: int) -> Optional[str]:
        r = await self.execute("LINDEX", key, index)
        return None if r is None else str(r)

    async def scan(self, cursor: str = "0", match: Optional[str] = None, count: int = 10):
        cmd = ["SCAN", cursor]
        if match:
            cmd += ["MATCH", match]
        if count:
            cmd += ["COUNT", int(count)]
        r = await self.execute(*cmd)
        if not r:
            return "0", []
        cur = str(r[0])
        keys = [str(x) for x in r[1]] if len(r) > 1 and isinstance(r[1], list) else []
        return cur, keys

    async def ttl(self, key: str) -> Optional[int]:
        r = await self.execute("TTL", key)
        try:
            return int(r)
        except Exception:
            return None

    async def hmset(self, key: str, mapping: dict):
        parts = ["HMSET", key]
        for f, v in mapping.items():
            parts.extend([str(f), str(v)])
        return await self.execute(*parts)

    async def hget(self, key: str, field: str):
        return await self.execute("HGET", key, field)

    async def hgetall(self, key: str):
        r = await self.execute("HGETALL", key)
        if not r:
            return {}
        it = iter(r)
        out = {}
        for f, v in zip(it, it):
            out[str(f)] = str(v)
        return out

    async def hdel(self, key: str, *fields: str):
        return await self.execute("HDEL", key, *fields)

    async def zadd(self, key: str, score: float, member: str):
        return await self.execute("ZADD", key, score, member)

    async def zrem(self, key: str, *members: str):
        return await self.execute("ZREM", key, *members)

    async def zrange(self, key: str, start: int, stop: int, withscores=False):
        if withscores:
            r = await self.execute("ZRANGE", key, start, stop, "WITHSCORES")
            out = []
            it = iter(r)
            for m, s in zip(it, it):
                out.append((str(m), float(s)))
            return out
        else:
            r = await self.execute("ZRANGE", key, start, stop)
            return [] if not r else [str(x) for x in r]

    async def zrangebyscore(self, key: str, min_v: str, max_v: str, withscores=False):
        if withscores:
            r = await self.execute("ZRANGEBYSCORE", key, min_v, max_v, "WITHSCORES")
            out = []
            it = iter(r)
            for m, s in zip(it, it):
                out.append((str(m), float(s)))
            return out
        else:
            r = await self.execute("ZRANGEBYSCORE", key, min_v, max_v)
            return [] if not r else [str(x) for x in r]

    async def publish(self, channel: str, message: str):
        return await self.execute("PUBLISH", channel, message)

class AsyncRedisPubSubWS:
    """
    Conexão dedicada a Pub/Sub via RESP-over-WS.
    """
    def __init__(self, url: str, token: str = ""):
        self.url = f"{url}?token={token}" if token else url
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def connect(self):
        if self._ws and not getattr(self._ws, "closed", False):
            return
        dbg("[redis-pubsub] connecting ->", self.url)
        self._ws = await websockets.connect(self.url, max_size=None)
        dbg("[redis-pubsub] connected")

    async def aclose(self):
        if self._ws is not None:
            try:
                await self._ws.close()
                dbg("[redis-pubsub] closed")
            except Exception as e:
                dbg("[redis-pubsub] close error:", repr(e))
            self._ws = None

    def _enc(self, *parts: Union[str, bytes, int, float]) -> bytes:
        out = bytearray()
        out.extend(f"*{len(parts)}\r\n".encode())
        for p in parts:
            s = p if isinstance(p, (bytes, bytearray)) else str(p).encode()
            out.extend(f"${len(s)}\r\n".encode()); out.extend(s); out.extend(b"\r\n")
        return bytes(out)

    async def _send(self, *parts):
        if not self._ws or getattr(self._ws, "closed", False):
            await self.connect()
        await self._ws.send(self._enc(*parts))

    async def subscribe(self, *channels: str):
        if not channels:
            return
        dbg("[redis-pubsub] SUBSCRIBE", channels)
        await self._send("SUBSCRIBE", *channels)

    async def psubscribe(self, *patterns: str):
        if not patterns:
            return
        dbg("[redis-pubsub] PSUBSCRIBE", patterns)
        await self._send("PSUBSCRIBE", *patterns)

    async def read_message(self) -> Optional[Dict[str, Any]]:
        """
        Retorna:
          {"type":"message","channel": "...", "payload": "..."}
          {"type":"pmessage","pattern":"...", "channel": "...", "payload": "..."}
          {"type":"subscribe"|...}
        """
        if not self._ws or getattr(self._ws, "closed", False):
            await self.connect()
        data = await self._ws.recv()
        if not isinstance(data, (bytes, bytearray)):
            data = str(data).encode()
        text = data.decode(errors="replace")
        parts = text.split("\r\n")
        if not parts or not parts[0].startswith("*"):
            return None
        try:
            elems = []
            i = 1
            while i < len(parts):
                line = parts[i]
                if line.startswith("$"):
                    i += 1
                    if i < len(parts):
                        elems.append(parts[i])
                i += 1
            if not elems:
                return None
            kind = elems[0]
            if kind == "message" and len(elems) >= 3:
                return {"type": "message", "channel": elems[1], "payload": elems[2]}
            if kind == "pmessage" and len(elems) >= 4:
                return {"type": "pmessage","pattern": elems[1],"channel": elems[2],"payload": elems[3]}
            return {"type": kind, "elems": elems}
        except Exception:
            return None

# ===================== CONFIG =====================

REDIS_WS_URL = os.getenv("REDIS_WS_URL", "wss://redisrender.onrender.com")
REDIS_TOKEN  = os.getenv("REDIS_TOKEN", "")

REDIS_LOG_NS = os.getenv("REDIS_LOG_NS", "iss")
PREQUEUE_NS  = os.getenv("PREQUEUE_NS", f"{REDIS_LOG_NS}:y")
PREQUEUE_COLABS_SET = f"{PREQUEUE_NS}:colabs"

FRONT_LAST_RUN_KEY = f"{REDIS_LOG_NS}:front:last_run_ts"
ARQ_ENABLED = False  # informativo

SESSION_COOKIE_NAME = os.getenv("FRONT_SESSION_COOKIE", "iss_front_session")
SESSION_TTL = int(os.getenv("FRONT_SESSION_TTL", "86400"))  # 1 dia

# ===================== HELPERS =====================

def runs_key_for(tenant: str) -> str:
    return f"{REDIS_LOG_NS}:front:{tenant}:runs"

def session_key(token: str) -> str:
    return f"{REDIS_LOG_NS}:front:sess:{token}"

def cnpj_digits(cnpj: str) -> str:
    d = "".join(filter(str.isdigit, cnpj))[-14:]
    return d.zfill(14) if d else "0"*14

def mask_cnpj(cnpj: str) -> str:
    d = cnpj_digits(cnpj)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def slugify(value: str) -> str:
    import re, unicodedata
    value = (value or "").strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii","ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "colab"

def task_logs_key(job_id: str) -> str:
    return f"{REDIS_LOG_NS}:task:{job_id}:logs"

def extract_meta(function: str, args: List[Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    meta = {"cnpj":"", "colaborador":"", "colaborador_norm":"", "mes":"", "etapa": function or ""}
    try:
        if function == "job_notas" and len(args) >= 4:
            colab_norm, cnpj, mes = str(args[0]), str(args[1]), str(args[2])
            meta.update({"colaborador_norm": colab_norm, "colaborador": colab_norm, "cnpj": cnpj, "mes": mes, "etapa": "notas"})
        elif function == "job_escrituracao" and len(args) >= 4:
            colab_norm, cnpj, mes = str(args[0]), str(args[1]), str(args[2])
            meta.update({"colaborador_norm": colab_norm, "colaborador": colab_norm, "cnpj": cnpj, "mes": mes, "etapa": "escrituracao"})
        elif function == "job_certidao" and len(args) >= 4:
            colab_norm, cnpj, mes = str(args[0]), str(args[1]), str(args[2])
            meta.update({"colaborador_norm": colab_norm, "colaborador": colab_norm, "cnpj": cnpj, "mes": mes, "etapa": "certidao"})
        elif function == "job_dam" and len(args) >= 3:
            colab_norm, cnpj = str(args[0]), str(args[1])
            meta.update({"colaborador_norm": colab_norm, "colaborador": colab_norm, "cnpj": cnpj, "etapa": "dam"})
    except Exception:
        pass
    return meta

def detect_etapa_from_logs(lines: List[str]) -> str:
    for line in lines:
        s = (line or "").lower()
        if "job_escrituracao" in s: return "escrituracao"
        if "job_notas" in s:        return "notas"
        if "job_certidao" in s:     return "certidao"
        if "job_dam" in s:          return "dam"
    return ""

def detect_job_status_from_logs(lines: List[str]) -> str:
    if not lines:
        return "pending"
    lower_lines = [(l or "").lower() for l in lines]
    joined = "\n".join(lower_lines)
    if any("=== fim" in l and "ok" in l for l in lower_lines): return "success"
    if "'status': 'ok'" in joined or '"status": "ok"' in joined: return "success"
    if "finalizado" in joined or "concluído" in joined or "concluido" in joined: return "success"
    ERROR_TOKENS = [" erro ", "error", "exception", "traceback", "loginerror", "cnpjinexistenteerror", "cnpjmismatcherror"]
    if any(tok in joined for tok in ERROR_TOKENS): return "error"
    return "running"

STATUS_PRIORITY = {"pending":0,"waiting":1,"queued":2,"running":3,"success":4,"error":4}
def merge_status(current: str, new: str) -> str:
    return new if STATUS_PRIORITY.get(new,0) >= STATUS_PRIORITY.get(current,0) else current

# ===================== APP =====================

app = FastAPI(title="ISS Queue Front (Cliente) — WS-only (Render)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def rds() -> AsyncRedisWS:
    # cliente de comandos (REST, leituras, gravações)
    return getattr(app.state, "redis_cmd", None)

async def get_session_from_request(request: Request) -> Optional[Dict[str, Any]]:
    rd: Optional[AsyncRedisWS] = getattr(app.state, "redis_cmd", None)
    if rd is None:
        dbg("get_session_from_request: redis_cmd not ready")
        return None
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    raw = await rd.get(session_key(token))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

# ===================== AUTH MIDDLEWARE =====================

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in ("/login", "/logout", "/") or path.startswith("/openapi") or path.startswith("/docs") or path.startswith("/redoc"):
        return await call_next(request)

    if path == "/" or path.startswith("/api") or path.startswith("/ws"):
        sess = await get_session_from_request(request)
        if not sess:
            if path.startswith("/api"):
                dbg("auth_middleware: 401 on", path)
                return JSONResponse({"detail": "Não autenticado"}, status_code=401)
        else:
            request.state.session = sess
    return await call_next(request)

# ===================== STARTUP / SHUTDOWN =====================

@app.on_event("startup")
async def startup():
    # 1) comandos
    app.state.redis_cmd = AsyncRedisWS(REDIS_WS_URL, token=REDIS_TOKEN, name="cmd")
    await app.state.redis_cmd.connect()


    # 3) scheduler (isolado)
    app.state.redis_scheduler = AsyncRedisWS(REDIS_WS_URL, token=REDIS_TOKEN, name="sched")
    await app.state.redis_scheduler.connect()

    dbg("[startup] Redis WS OK (cmd/pubsub/sched)")
    asyncio.create_task(scheduler_y_to_arq())

@app.on_event("shutdown")
async def shutdown():
    try:
        await app.state.redis_cmd.aclose()
    except Exception:
        pass
    try:
        await app.state.redis_pubsub.aclose()
    except Exception:
        pass
    try:
        await app.state.redis_scheduler.aclose()
    except Exception:
        pass
    print("Shutdown concluído (WS fechados).")

# ===================== RUNS PERSISTÊNCIA =====================

async def load_runs(rd: AsyncRedisWS, tenant: str) -> Dict[str, Dict[str, Any]]:
    if not tenant:
        return {}
    raw = await rd.get(runs_key_for(tenant))
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}

async def save_runs(rd: AsyncRedisWS, tenant: str, data: Dict[str, Dict[str, Any]]):
    if not tenant:
        return
    await rd.set(runs_key_for(tenant), json.dumps(data))

# ===================== HTML LOGIN =====================

PAGE_LOGIN = """<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Prumo Sistemas — Login ISS Front (Cliente)</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet"/>
  <style>body{background:#f8f9fa;min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}.login-card{max-width:420px;width:100%;border-radius:.75rem;box-shadow:0 .5rem 1rem rgba(0,0,0,.08)}</style>
</head>
<body>
  <div class="card login-card">
    <div class="card-body p-4">
      <h1 class="h5 mb-3 text-center">Prumo Sistemas — ISS Front</h1>
      <p class="text-muted small text-center mb-4">Informe o nome do colaborador e a senha padrão para acessar o painel.</p>
      <form method="post" action="/login" autocomplete="off">
        <div class="mb-3">
          <label class="form-label small text-muted">Colaborador</label>
          <input class="form-control" name="colaborador" placeholder="Ex.: João da Silva" required>
        </div>
        <div class="mb-3">
          <label class="form-label small text-muted">Senha</label>
          <input type="password" class="form-control" name="senha" placeholder="123456" required>
        </div>
        <div class="d-grid"><button class="btn btn-primary" type="submit">Entrar</button></div>
        <div id="erroLogin" class="text-danger small mt-3 text-center" style="display:none;">Usuário ou senha incorretos.</div>
      </form>
    </div>
  </div>
<script>
  document.querySelector("form").addEventListener("submit", async function (e) {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    const erroEl = document.getElementById("erroLogin");
    erroEl.style.display = "none";
    const resp = await fetch("/login", { method: "POST", body: formData });
    if (resp.status === 200 || resp.status === 303) { window.location.href = "/"; return; }
    if (resp.status === 401) { erroEl.style.display = "block"; }
  });
</script>
</body>
</html>
"""


# ===================== HTML LOGIN =====================

PAGE_LOGIN = """<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Prumo Sistemas — Login ISS Front (Cliente)</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet"/>
  <style>
    body { background-color: #f8f9fa; min-height: 100vh; display:flex; align-items:center; justify-content:center; font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
    .login-card { max-width: 420px; width: 100%; border-radius: 0.75rem; box-shadow: 0 0.5rem 1rem rgba(0,0,0,0.08); }
  </style>
</head>
<body>
  <div class="card login-card">
    <div class="card-body p-4">
      <h1 class="h5 mb-3 text-center">Prumo Sistemas — ISS Front</h1>
      <p class="text-muted small text-center mb-4">Informe o nome do colaborador e a senha padrão para acessar o painel.</p>
      <form method="post" action="/login" autocomplete="off">
        <div class="mb-3">
          <label class="form-label small text-muted">Colaborador</label>
          <input class="form-control" name="colaborador" placeholder="Ex.: João da Silva" required>
        </div>
        <div class="mb-3">
          <label class="form-label small text-muted">Senha</label>
          <input type="password" class="form-control" name="senha" placeholder="123456" required>
        </div>
        <div class="d-grid"><button class="btn btn-primary" type="submit">Entrar</button></div>
        <div id="erroLogin" class="text-danger small mt-3 text-center" style="display:none;">Usuário ou senha incorretos.</div>
      </form>
    </div>
  </div>
<script>
  document.querySelector("form").addEventListener("submit", async function (e) {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    const erroEl = document.getElementById("erroLogin");
    erroEl.style.display = "none";
    const resp = await fetch("/login", { method: "POST", body: formData });
    if (resp.status === 200 || resp.status === 303) { window.location.href = "/"; return; }
    if (resp.status === 401) { erroEl.style.display = "block"; }
  });
</script>
</body>
</html>
"""

# ===================== HTML CLIENT =====================

PAGE_CLIENT = """<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Prumo Sistemas — ISS Front (Cliente)</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet"/>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css" rel="stylesheet"/>
  <style>
    :root {
      --prumo-primary: #007bff;
      --prumo-primary-rgb: 0, 123, 255;
      --prumo-success: #28a745;
      --prumo-danger: #dc3545;
      --prumo-warning: #ffc107;
      --prumo-info: #17a2b8;
      --prumo-light-gray: #f8f9fa;
      --prumo-medium-gray: #e9ecef;
      --prumo-dark: #343a40;
    }
    body {
      background-color: var(--prumo-light-gray);
      min-height: 100vh;
      font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    }
    .card {
      border: 1px solid var(--prumo-medium-gray);
      border-radius: 0.75rem;
      box-shadow: 0 0.25rem 0.5rem rgba(0, 0, 0, 0.05);
      transition: all 0.3s ease;
    }
    .card-header {
      background-color: white;
      border-bottom: 1px solid var(--prumo-medium-gray);
      border-top-left-radius: 0.75rem;
      border-top-right-radius: 0.75rem;
      padding: 1rem 1.25rem;
    }
    .kpi-box-v3 {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 1rem;
    }
    .kpi-box-v3 .item {
      background: white;
      border-radius: 0.5rem;
      padding: 1rem;
      box-shadow: none;
      border: 1px solid var(--prumo-medium-gray);
      transition: border-color 0.2s;
    }
    .kpi-box-v3 .item:hover {
      border-color: var(--prumo-primary);
    }
    .kpi-box-v3 .item .value {
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--prumo-dark);
    }
    .kpi-box-v3 .item .label {
      font-size: 0.8rem;
      color: #6c757d;
      margin-top: 0.25rem;
    }
    .table-progress th, .table-progress td {
      vertical-align: middle;
      font-size: 0.9rem;
    }
    .table-progress thead th {
      font-weight: 600;
      color: #495057;
    }
    .table-progress .cnpj-info {
      line-height: 1.2;
    }
    .table-progress .cnpj-info .cnpj {
      font-weight: 600;
      color: var(--prumo-dark);
    }
    .table-progress .cnpj-info .razao {
      font-size: 0.8rem;
      color: #6c757d;
    }
    .status-etapa {
      display: inline-block;
      margin-right: 0.5rem;
      font-size: 0.75rem;
      padding: 0.2rem 0.4rem;
      border-radius: 0.25rem;
      font-weight: 600;
      line-height: 1;
    }
    .status-etapa.success { background-color: var(--prumo-success); color: white; }
    .status-etapa.running { background-color: var(--prumo-primary); color: white; }
    .status-etapa.waiting { background-color: var(--prumo-warning); color: var(--prumo-dark); }
    .status-etapa.error { background-color: var(--prumo-danger); color: white; }
    .status-etapa.pending { background-color: var(--prumo-medium-gray); color: var(--prumo-dark); }
    .log-item {
      padding: 0.5rem 0;
      border-bottom: 1px solid var(--prumo-medium-gray);
    }
    .log-item:last-child { border-bottom: none; }
    .log-item .status-badge {
      font-size: 0.8rem;
      padding: 0.3em 0.6em;
    }
    .log-content {
      max-height: 300px;
      overflow-y: auto;
    }
  </style>
</head>
<body>
<div id="app-page">
  <nav class="navbar navbar-expand-lg bg-white border-bottom shadow-sm sticky-top">
    <div class="container-fluid px-4">
      <span class="navbar-brand fw-bold text-primary">
        <i class="bi bi-building-gear me-2"></i>
        Prumo Sistemas — <small class="text-muted" id="nav-username">ISS Front</small>
      </span>
      <div class="d-flex align-items-center">
        <span class="badge bg-light text-secondary me-3">Usuário: Cliente</span>
        <button class="btn btn-outline-secondary btn-sm" id="btn-logout" type="button">
          <i class="bi bi-box-arrow-right"></i> Sair
        </button>
      </div>
    </div>
  </nav>

  <div class="container py-4">
    <h1 class="h4 mb-4 fw-bold text-dark">Dashboard de Automação</h1>

    <div class="kpi-box-v3 mb-4" id="kpi-box">
      <div class="item">
        <div class="label">Em Execução</div>
        <div class="value text-primary" id="kpi-running">—</div>
      </div>
      <div class="item">
        <div class="label">Aguardando Processamento</div>
        <div class="value text-warning" id="kpi-queue">0</div>
      </div>
      <div class="item">
        <div class="label">Total de CNPJs Cadastrados</div>
        <div class="value text-success" id="kpi-total-cnpjs">0</div>
      </div>
      <div class="item">
        <div class="label">Última Ação</div>
        <div class="value text-secondary" id="kpi-last-run">—</div>
      </div>
    </div>

    <ul class="nav nav-tabs mb-4" role="tablist">
      <li class="nav-item" role="presentation">
        <button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab-lancar" type="button" role="tab" aria-selected="true">
          <i class="bi bi-send-check me-1"></i> Lançar Execução
        </button>
      </li>
      <li class="nav-item" role="presentation">
        <button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-status" type="button" role="tab" aria-selected="false">
          <i class="bi bi-bar-chart-line me-1"></i> Status de Execução
        </button>
      </li>
      <li class="nav-item" role="presentation">
        <button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-contas" type="button" role="tab" aria-selected="false">
          <i class="bi bi-person-gear me-1"></i> Contas & CNPJs
        </button>
      </li>
    </ul>

    <div class="tab-content">
      <!-- TAB LANÇAR -->
      <div class="tab-pane fade show active" id="tab-lancar" role="tabpanel">
        <div class="row g-4">
          <div class="col-lg-4">
            <div class="card h-100">
              <div class="card-header fw-bold">
                <i class="bi bi-gear-wide-connected me-2"></i>Configuração da Execução
              </div>
              <div class="card-body">
                <div class="row g-3">
                  <div class="col-12">
                    <label class="form-label small text-muted">Provedor</label>
                    <select class="form-select form-select-sm" id="provider">
                      <option value="1" selected>ISS Fortaleza (CE)</option>
                      <option value="2" disabled>ISS Eusébio (CE) — em breve</option>
                    </select>
                  </div>
                  <div class="col-6">
                    <label class="form-label small text-muted">Mês</label>
                    <select class="form-select form-select-sm" id="mes"></select>
                  </div>
                  <div class="col-6">
                    <label class="form-label small text-muted">Ano</label>
                    <select class="form-select form-select-sm" id="ano"></select>
                  </div>
                  <div class="col-12">
                    <label class="form-label small text-muted">Etapas a Executar</label>
                    <div class="row">
                      <div class="col-6">
                        <div class="form-check form-check-sm">
                          <input class="form-check-input etapa-chk" type="checkbox" id="etapa_escrituracao" checked>
                          <label class="form-check-label small" for="etapa_escrituracao">Escrituração</label>
                        </div>
                        <div class="form-check form-check-sm">
                          <input class="form-check-input etapa-chk" type="checkbox" id="etapa_notas" checked>
                          <label class="form-check-label small" for="etapa_notas">Notas</label>
                        </div>
                      </div>
                      <div class="col-6">
                        <div class="form-check form-check-sm">
                          <input class="form-check-input etapa-chk" type="checkbox" id="etapa_dam">
                          <label class="form-check-label small" for="etapa_dam">DAM</label>
                        </div>
                        <div class="form-check form-check-sm">
                          <input class="form-check-input etapa-chk" type="checkbox" id="etapa_certidao" checked>
                          <label class="form-check-label small" for="etapa_certidao">Certidão</label>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div class="col-12">
                    <label class="form-label small text-muted">Opções</label>
                    <div class="form-check form-switch form-check-sm">
                      <input class="form-check-input" type="checkbox" id="formato_dominio">
                      <label class="form-check-label small" for="formato_dominio">Exportar para Domínio</label>
                    </div>
                  </div>
                </div>
                <hr class="my-4">
                <div class="d-grid gap-2">
                  <button class="btn btn-primary btn-lg" id="btn-enfileirar" type="button">
                    <i class="bi bi-send-check me-2"></i> Lançar Execução
                  </button>
                </div>
                <div class="mt-3 small text-center" id="enfileirar-result"></div>
              </div>
            </div>
          </div>

          <div class="col-lg-8">
            <div class="card h-100">
              <div class="card-header fw-bold">
                <i class="bi bi-list-check me-2"></i> Seleção de CNPJs para Execução
              </div>
              <div class="card-body">
                <div class="mb-3">
                  <div class="d-flex align-items-center gap-3 flex-wrap mb-3">
                    <div class="input-group input-group-sm flex-grow-1" style="max-width: 250px;">
                      <span class="input-group-text"><i class="bi bi-search"></i></span>
                      <input class="form-control" id="cnpj-search-lancar" placeholder="Buscar CNPJ/Razão...">
                    </div>
                    <select class="form-select form-select-sm" id="filter-account-lancar" style="max-width: 180px;">
                      <option value="">Filtrar por Conta</option>
                    </select>
                    <select class="form-select form-select-sm" id="filter-dominio-lancar" style="max-width: 150px;">
                      <option value="">Todos</option>
                      <option value="with">Com Domínio</option>
                      <option value="without">Sem Domínio</option>
                    </select>
                    <div class="ms-auto small text-muted">
                      <span class="fw-bold text-primary" id="sel-count-lancar">0</span> CNPJs Selecionados
                    </div>
                  </div>
                </div>
                <div class="table-responsive">
                  <table class="table table-sm table-hover align-middle" id="table-selector-lancar">
                    <thead class="table-light">
                      <tr>
                        <th style="width:30px"><input class="form-check-input" type="checkbox" id="chk-all-lancar"></th>
                        <th style="width:200px">CNPJ / Razão Social</th>
                        <th style="width:100px">Conta</th>
                        <th style="width:100px">Cód. Domínio</th>
                      </tr>
                    </thead>
                    <tbody id="tbody-selector-lancar">
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- TAB STATUS -->
      <div class="tab-pane fade" id="tab-status" role="tabpanel">
        <div class="card">
          <div class="card-header fw-bold">
            <i class="bi bi-list-check me-2"></i> Status de Execução por Execução (clique)
          </div>
          <div class="card-body">
            <div class="d-flex gap-2 mb-3">
              <button class="btn btn-outline-danger btn-sm" id="btn-parar-tudo" type="button">
                <i class="bi bi-stop-circle"></i> Parar Tudo
              </button>
              <button class="btn btn-success btn-sm" id="btn-download" type="button">
                <i class="bi bi-download"></i> Baixar ZIP
              </button>
            </div>
            <div class="table-responsive">
              <table class="table table-sm table-hover align-middle table-progress" id="table-selector-status">
                <thead class="table-light">
                  <tr>
                    <th style="width:220px">CNPJ / Razão Social</th>
                    <th style="width:120px">Conta</th>
                    <th style="width:100px" class="text-center">Mês/Ano</th>
                    <th style="width:220px" class="text-center">Etapas</th>
                    <th style="width:120px" class="text-center">Status Geral</th>
                    <th style="width:80px" class="text-center">Logs</th>
                  </tr>
                </thead>
                <tbody id="tbody-selector-status"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      <!-- TAB CONTAS & CNPJS -->
      <div class="tab-pane fade" id="tab-contas" role="tabpanel">
        <div class="row g-4">
          <div class="col-lg-5">
            <div class="card h-100">
              <div class="card-header fw-bold">
                <i class="bi bi-person-gear me-2"></i>Contas de Acesso
              </div>
              <div class="card-body">
                <form id="form-account" class="row g-3">
                  <input type="hidden" id="acc-id">
                  <div class="col-12">
                    <label class="form-label small text-muted">Provedor</label>
                    <select class="form-select form-select-sm" id="acc-provider">
                      <option value="1" selected>ISS Fortaleza (CE)</option>
                    </select>
                  </div>
                  <div class="col-6">
                    <label class="form-label small text-muted">Usuário</label>
                    <input class="form-control form-control-sm" id="acc-user" placeholder="usuário do ISS" required>
                  </div>
                  <div class="col-6">
                    <label class="form-label small text-muted">Senha</label>
                    <input type="password" class="form-control form-control-sm" id="acc-pass" placeholder="senha do ISS" required>
                  </div>
                  <div class="col-12">
                    <label class="form-label small text-muted">Alias (apelido da conta)</label>
                    <input class="form-control form-control-sm" id="acc-alias" placeholder="Ex.: Matriz / Filial 01">
                  </div>
                  <div class="col-12 d-flex gap-2 pt-2">
                    <button class="btn btn-primary btn-sm" type="submit">
                      <i class="bi bi-floppy me-1"></i> Salvar Conta
                    </button>
                    <button class="btn btn-outline-secondary btn-sm" type="button" id="acc-reset">
                      <i class="bi bi-arrow-counterclockwise me-1"></i> Limpar
                    </button>
                  </div>
                </form>
                <hr class="my-4">
                <h5 class="card-title small text-muted mb-3">Contas Cadastradas</h5>
                <div id="acc-list"></div>
              </div>
            </div>
          </div>

          <div class="col-lg-7">
            <div class="card h-100">
              <div class="card-header d-flex justify-content-between align-items-center">
                <span class="fw-bold">
                  <i class="bi bi-buildings me-2"></i>CNPJs Vinculados
                </span>
                <div class="d-flex gap-2">
                  <button class="btn btn-outline-secondary btn-sm" data-bs-toggle="modal" data-bs-target="#modalImport" type="button">
                    <i class="bi bi-file-earmark-spreadsheet me-1"></i> Importar XLSX
                  </button>
                  <button class="btn btn-primary btn-sm" data-bs-toggle="modal" data-bs-target="#modalCNPJ" type="button">
                    <i class="bi bi-plus-lg me-1"></i> Adicionar CNPJ
                  </button>
                </div>
              </div>
              <div class="card-body">
                <div id="cnpj-list">
                  <div class="table-responsive">
                    <table class="table table-sm table-hover align-middle">
                      <thead class="table-light">
                        <tr>
                          <th style="width:150px">CNPJ</th>
                          <th>Razão Social</th>
                          <th style="width:100px">Conta</th>
                          <th style="width:80px">Domínio</th>
                          <th style="width:60px" class="text-end">Ações</th>
                        </tr>
                      </thead>
                      <tbody id="tbody-cnpjs"></tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>

        </div>
      </div>

    </div>
  </div>
</div>

<!-- Modal Logs -->
<div class="modal fade" id="modalLogs" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title" id="modalLogsTitle">Logs de Execução</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <h6 class="fw-bold mb-3">Status Detalhado por Etapa</h6>
        <div id="logs-status-summary" class="row"></div>
        <hr>
        <h6 class="fw-bold mb-3">Log Completo</h6>
        <div class="mb-3 d-flex align-items-center">
          <label for="log-filter-etapa" class="form-label small text-muted me-2 mb-0">Filtrar por Etapa:</label>
          <select class="form-select form-select-sm w-auto" id="log-filter-etapa">
            <option value="all" selected>Todas as Etapas</option>
            <option value="escrituracao">Escrituração</option>
            <option value="notas">Notas</option>
            <option value="dam">DAM</option>
            <option value="certidao">Certidão</option>
          </select>
        </div>
        <div class="log-content">
          <pre class="bg-light p-3 rounded small text-dark border" id="full-log-content"></pre>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Fechar</button>
      </div>
    </div>
  </div>
</div>

<!-- Modal CNPJ -->
<div class="modal fade" id="modalCNPJ" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title" id="cnpj-modal-title">Adicionar Novo CNPJ</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <form id="form-cnpj" class="row g-3">
          <input type="hidden" id="cnpj-id">
          <div class="col-12">
            <label class="form-label small text-muted">Conta de Acesso</label>
            <select class="form-select form-select-sm" id="cnpj-account"></select>
          </div>
          <div class="col-12">
            <label class="form-label small text-muted">CNPJ</label>
            <input class="form-control form-control-sm" id="inp-cnpj" placeholder="00.000.000/0000-00" required>
          </div>
          <div class="col-12">
            <label class="form-label small text-muted">Razão Social</label>
            <input class="form-control form-control-sm" id="inp-razao" placeholder="Nome da Empresa">
          </div>
          <div class="col-12">
            <label class="form-label small text-muted">Código Domínio (Opcional)</label>
            <input class="form-control form-control-sm" id="inp-dominio" placeholder="Código de integração">
          </div>
        </form>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
        <button type="submit" form="form-cnpj" class="btn btn-primary">Salvar CNPJ</button>
      </div>
    </div>
  </div>
</div>

<!-- Modal Import -->
<div class="modal fade" id="modalImport" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title">Importar CNPJs via XLSX</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <form id="form-import" class="row g-3">
          <div class="col-12">
            <label class="form-label small text-muted">Conta de Acesso para Vinculação</label>
            <select class="form-select form-select-sm" id="import-account"></select>
          </div>
          <div class="col-12">
            <label class="form-label small text-muted">Arquivo XLSX</label>
            <input class="form-control form-control-sm" type="file" id="import-file" accept=".xlsx" required>
          </div>
          <div class="col-12">
            <p class="small text-muted">O arquivo deve conter as colunas: CNPJ, Razão Social, Código Domínio.</p>
          </div>
        </form>
        <div class="mt-3 small text-center" id="import-status"></div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
        <button type="submit" form="form-import" class="btn btn-primary">Importar</button>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
  document.addEventListener('DOMContentLoaded', () => {
    // ---------- Delegated Tooltip (UMA instância somente) ----------
    // Usa delegação para evitar criar muitas instâncias e travar o navegador.
    try {
      new bootstrap.Tooltip(document.body, {
        selector: '[data-bs-toggle="tooltip"]',
        trigger: 'hover focus'
      });
    } catch (e) {
      // se bootstrap não estiver disponível por algum motivo, ignora
      console.warn('Tooltip delegation init falhou:', e);
    }

    const meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"];
    const hoje = new Date();
    const mesSelect = document.getElementById('mes');
    const anoSelect = document.getElementById('ano');
    const anoAtual = hoje.getFullYear();
    const mesAtual = hoje.getMonth();

    const navUsernameEl = document.getElementById('nav-username');

    meses.forEach((m, i) => {
      const o = document.createElement('option');
      o.value = (i + 1).toString().padStart(2,'0');
      o.textContent = m;
      if (i === mesAtual) o.selected = true;
      mesSelect.appendChild(o);
    });
    for (let i = 0; i < 3; i++) {
      const ano = anoAtual - i;
      const o = document.createElement('option');
      o.value = ano;
      o.textContent = ano;
      if (i === 0) o.selected = true;
      anoSelect.appendChild(o);
    }

    const chkAllLancar = document.getElementById('chk-all-lancar');
    const selCountLancar = document.getElementById('sel-count-lancar');
    const tbodySelectorLancar = document.getElementById('tbody-selector-lancar');
    const searchLancar = document.getElementById('cnpj-search-lancar');
    const filterAccLancar = document.getElementById('filter-account-lancar');
    const filterDomLancar = document.getElementById('filter-dominio-lancar');

    const tbodyStatus = document.getElementById('tbody-selector-status');
    const tbodyCnpjs = document.getElementById('tbody-cnpjs');
    const accListDiv = document.getElementById('acc-list');
    const cnpjAccountSelect = document.getElementById('cnpj-account');
    const importAccountSelect = document.getElementById('import-account');

    const kpiRunning = document.getElementById('kpi-running');
    const kpiQueue = document.getElementById('kpi-queue');
    const kpiTotal = document.getElementById('kpi-total-cnpjs');
    const kpiLastRun = document.getElementById('kpi-last-run');

    const enfileirarResult = document.getElementById('enfileirar-result');
    const btnEnfileirar = document.getElementById('btn-enfileirar');

    let ACCS = {};   // id -> account (apenas localStorage)
    let CNPJS = {};  // id -> cnpj   (apenas localStorage)
    let FULL_LOG_TEXT = ''; // usado no filtro

    let STORAGE_KEY_ACCOUNTS = '';
    let STORAGE_KEY_CNPJS = '';

    // LOG refresh periódico dentro do modal
    let LOG_REFRESH_INTERVAL = null;

    // NOVO: WebSocket de status / logs em tempo real
    let STATUS_WS = null;
    let WS_RETRY_MS = 1000;
    let CURRENT_RUNS = {};

    function initStorageKeys(colabNorm) {
      const base = 'iss_front_' + (colabNorm || 'default') + '_';
      STORAGE_KEY_ACCOUNTS = base + 'accounts';
      STORAGE_KEY_CNPJS = base + 'cnpjs';
    }

    function loadFromLocal() {
      if (!STORAGE_KEY_ACCOUNTS) return;
      try {
        const rawAcc = localStorage.getItem(STORAGE_KEY_ACCOUNTS);
        ACCS = rawAcc ? JSON.parse(rawAcc) : {};
      } catch {
        ACCS = {};
      }
      try {
        const rawCnp = localStorage.getItem(STORAGE_KEY_CNPJS);
        CNPJS = rawCnp ? JSON.parse(rawCnp) : {};
      } catch {
        CNPJS = {};
      }
    }

    function saveToLocal() {
      if (STORAGE_KEY_ACCOUNTS) {
        localStorage.setItem(STORAGE_KEY_ACCOUNTS, JSON.stringify(ACCS));
      }
      if (STORAGE_KEY_CNPJS) {
        localStorage.setItem(STORAGE_KEY_CNPJS, JSON.stringify(CNPJS));
      }
    }

    function findLocalCnpj(cnpjDigits) {
      return Object.values(CNPJS).find(c => c.cnpj === cnpjDigits) || null;
    }

    function genId() {
      if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
      return 'id_' + Date.now().toString(16) + '_' + Math.random().toString(16).slice(2);
    }

    // ====================== HIGIENIZAÇÃO DE LOGS ======================

    function simplifyExceptionLine(line) {
        const m = line.match(/^[\\w\\.]*?(\\w+Error):\\s*(.*)$/);
        if (!m) return line;

        const errorType = m[1];
        const rest = m[2] || "";

        const parts = rest.split(":");
        const msg = (parts.length > 1 ? parts.slice(1).join(":") : rest).trim();

        switch (errorType) {
            case "LoginError":
                return "Erro de login: " + msg;
            case "CnpjInexistenteError":
                return "CNPJ não encontrado: " + msg;
            case "CnpjMismatchError":
                return "CNPJ divergente: " + msg;
            default:
                return "Erro: " + msg;
        }
    }

    // Deixa cada linha “bonita” mas SEM perder coisas como flow=escrituracao
    function formatLogLinePretty(line) {
        line = (line || "").trimEnd();
        if (!line) return "";

        // Cabeçalho de job
        if (line.startsWith("=== Job ") && line.endsWith(" ===")) {
            const m = line.match(/^=== Job ([0-9a-fA-F]+) ===$/);
            const id = m ? m[1] : line;
            const shortId = id.slice(0, 8) + "…";
            return "\\n──────── Job " + shortId + " ────────";
        }

        // Tenta pegar timestamp + nível
        const m = line.match(/^(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2}),\\d+\\s+\\[(\\w+)\\]\\s+(.*)$/);
        if (!m) {
            // Linha simples (ex: "Erro de login: ...")
            if (/^Erro/i.test(line)) {
                return "⚠️ " + line;
            }
            // Deixa como está
            return line;
        }

        const date = m[1];              // 2025-11-24
        const time = m[2];              // 14:25:36
        const level = m[3];             // INFO / WARNING / ERROR
        let rest = m[4] || "";          // resto

        // Extrai tag [STEP], [FLOW_START], [FLOW_END], [EVENT], etc
        let tag = "";
        let msg = rest;
        const tagMatch = rest.match(/^\\[(\\w+)\\]\\s*(.*)$/);
        if (tagMatch) {
            tag = tagMatch[1];
            msg = tagMatch[2] || "";
        }

        // Deixa mensagens mais humanas, mas preservando flow= / cnpj= / etc
        let human = msg;

        if (tag === "FLOW_START" || tag === "FLOW_END" || tag === "EVENT") {
            const parts = msg.split("::");
            const trailing = (parts[parts.length - 1] || "").trim();
            if (tag === "FLOW_START") {
                human = "▶ " + trailing;
            } else if (tag === "FLOW_END") {
                human = "✔ " + trailing;
            } else {
                human = "• " + trailing;
            }
        } else if (tag === "STEP") {
            // Exemplo:
            // flow=escrituracao ... step=Login :: Step: Login
            let stepName = "";
            const stepMatch = msg.match(/step=([^ ]+)/i);
            if (stepMatch) {
                stepName = stepMatch[1];
            }
            const parts = msg.split("::");
            const trailing = (parts[parts.length - 1] || "").trim(); // "Step: Login"
            human = "STEP" + (stepName ? " (" + stepName + ")" : "") + " — " + trailing;
        }

        // Monta prefixo com hora + nível
        let prefix = "[" + time + "] [" + level + "]";
        if (tag && !["STEP","FLOW_START","FLOW_END","EVENT"].includes(tag)) {
            prefix += " [" + tag + "]";
        }

        let full = prefix + " " + human;

        // Destaca erros visualmente
        if (level === "ERROR" || /Erro de /.test(human)) {
            full = "⚠️ " + full;
        }

        return full;
    }

    function sanitizeLogText(text) {
        const lines = (text || "").split("\\n");
        const out = [];
        let skippingTraceback = false;

        for (let rawLine of lines) {
            let line = rawLine;

            // início do traceback
            if (!skippingTraceback && line.startsWith("Traceback (most recent call last):")) {
                skippingTraceback = true;
                continue;
            }

            if (skippingTraceback) {
                // quando chega na linha do erro final, volta a exibir
                if (line.startsWith("flow_core.") || line.match(/^[\\w\\.]+Error:/)) {
                    line = simplifyExceptionLine(line.trim());
                    out.push(formatLogLinePretty(line));
                    skippingTraceback = false;
                }
                continue; // ignora tudo dentro do traceback
            }

            // ignora frames de stacktrace
            if (/^\\s*File ".*", line \\d+, in /.test(line)) continue;
            if (/^\\s*raise /.test(line)) continue;
            if (/^\\s*During handling of the above exception/.test(line)) continue;
            if (/^\\s*The above exception was the direct cause of the following exception:/.test(line)) continue;

            out.push(formatLogLinePretty(line));
        }

        return out.join("\\n").trim();
    }

    // ====================== FIM HIGIENIZAÇÃO ======================

    function fmtCnpj(d) {
      const c = d.padStart(14,'0');
      return c.substr(0,2) + '.' + c.substr(2,3) + '.' + c.substr(5,3) + '/' + c.substr(8,4) + '-' + c.substr(12,2);
    }

    async function fetchJSON(url, opts = {}) {
      const r = await fetch(url, opts);
      if (r.status === 401) {
        window.location.href = '/login';
        throw new Error('Não autenticado');
      }
      if (!r.ok) {
        let msg = await r.text();
        throw new Error(msg || ('HTTP ' + r.status));
      }
      return r.json();
    }

    function renderAccounts() {
      accListDiv.innerHTML = '';
      const list = document.createElement('div');
      list.className = 'list-group list-group-flush';
      Object.values(ACCS).forEach(acc => {
        const item = document.createElement('div');
        item.className = 'list-group-item d-flex justify-content-between align-items-center py-2';
        item.dataset.id = acc.id;
        item.innerHTML = `
          <div>
            <b>${acc.user}</b> <span class="badge bg-primary-subtle text-primary ms-2">${acc.alias || '(sem alias)'}</span>
            <div class="text-muted small">Provedor: ISS Fortaleza (CE)</div>
          </div>
          <div class="btn-group btn-group-sm">
            <button class="btn btn-outline-primary" data-edit="${acc.id}"><i class="bi bi-pencil"></i></button>
            <button class="btn btn-outline-danger" data-del="${acc.id}"><i class="bi bi-trash"></i></button>
          </div>`;
        list.appendChild(item);
      });
      accListDiv.appendChild(list);

      // atualiza selects de conta
      cnpjAccountSelect.innerHTML = '';
      importAccountSelect.innerHTML = '';
      Object.values(ACCS).forEach(acc => {
        const opt = document.createElement('option');
        opt.value = acc.id;
        opt.textContent = acc.alias || acc.user;
        cnpjAccountSelect.appendChild(opt);
        importAccountSelect.appendChild(opt.cloneNode(true));
      });

      // filtro de conta na aba lançar
      filterAccLancar.innerHTML = '<option value="">Filtrar por Conta</option>';
      Object.values(ACCS).forEach(acc => {
        const opt = document.createElement('option');
        opt.value = acc.id;
        opt.textContent = acc.alias || acc.user;
        filterAccLancar.appendChild(opt);
      });
    }

    function renderCnpjs() {
      // tabela de manutenção
      tbodyCnpjs.innerHTML = '';
      Object.values(CNPJS).forEach(c => {
        const tr = document.createElement('tr');
        const acc = ACCS[c.account_id] || {};
        tr.dataset.id = c.id;
        tr.innerHTML = `
          <td><code>${fmtCnpj(c.cnpj)}</code></td>
          <td>${c.razao || ''}</td>
          <td>${acc.alias || acc.user || ''}</td>
          <td>${c.dominio ? '<code>' + c.dominio + '</code>' : '—'}</td>
          <td class="text-end">
            <div class="btn-group btn-group-sm">
              <button class="btn btn-outline-primary" data-edit="${c.id}"><i class="bi bi-pencil"></i></button>
              <button class="btn btn-outline-danger" data-del="${c.id}"><i class="bi bi-trash"></i></button>
            </div>
          </td>`;
        tbodyCnpjs.appendChild(tr);
      });

      renderCnpjsLancar();
    }

    function renderCnpjsLancar() {
      const txt = (searchLancar.value || '').toLowerCase();
      const accFilter = filterAccLancar.value || '';
      const domFilter = filterDomLancar.value || '';

      const selectedIds = new Set();
      tbodySelectorLancar.querySelectorAll('tr').forEach(tr => {
        const id = tr.dataset.id;
        const chk = tr.querySelector('input[type="checkbox"]');
        if (chk && chk.checked) selectedIds.add(id);
      });

      tbodySelectorLancar.innerHTML = '';
      Object.values(CNPJS).forEach(c => {
        const acc = ACCS[c.account_id] || {};
        const texto = (fmtCnpj(c.cnpj) + ' ' + (c.razao || '')).toLowerCase();
        if (txt && !texto.includes(txt)) return;
        if (accFilter && c.account_id !== accFilter) return;
        if (domFilter === 'with' && !c.dominio) return;
        if (domFilter === 'without' && c.dominio) return;

        const tr = document.createElement('tr');
        tr.dataset.id = c.id;
        const isSel = selectedIds.size === 0 || selectedIds.has(c.id);
        tr.innerHTML = `
          <td><input class="form-check-input" type="checkbox" ${isSel ? 'checked' : ''}></td>
          <td>
            <div class="cnpj-info">
              <div class="cnpj">${fmtCnpj(c.cnpj)}</div>
              <div class="razao">${c.razao || ''}</div>
            </div>
          </td>
          <td>${acc.alias || acc.user || ''}</td>
          <td>${c.dominio ? '<code>' + c.dominio + '</code>' : '—'}</td>`;
        tbodySelectorLancar.appendChild(tr);
      });
      updateSelectedCountLancar();
    }

    function updateSelectedCountLancar() {
      const n = tbodySelectorLancar.querySelectorAll('input[type="checkbox"]:checked').length;
      selCountLancar.textContent = n;
    }

    function applyKpis(k) {
      if (!k || !k.kpis) return;
      kpiRunning.textContent = k.kpis.running ?? 0;
      kpiQueue.textContent = k.kpis.queue ?? 0;
      // Total de CNPJs: usamos o que está no localStorage
      kpiTotal.textContent = Object.keys(CNPJS).length;
      kpiLastRun.textContent = k.kpis.last_run_str || '—';
    }

    function etapaLabel(etapa) {
      switch ((etapa || '').toLowerCase()) {
        case 'escrituracao': return 'Escrituração';
        case 'notas': return 'Notas';
        case 'dam': return 'DAM';
        case 'certidao': return 'Certidão';
        default: return etapa || '-';
      }
    }

    function badgeEtapa(status, etapaName) {
      const iconByStatus = {
        success: 'bi-check-lg',
        running: 'bi-arrow-repeat',
        waiting: 'bi-hourglass-split',
        queued: 'bi-hourglass-split',
        pending: 'bi-clock',
        error: 'bi-x-lg',
      };
      const titleByStatus = {
        success: 'Concluído',
        running: 'Em Execução',
        waiting: 'Aguardando',
        queued: 'Na fila',
        pending: 'Não Iniciado',
        error: 'Erro',
      };
      const ic = iconByStatus[status] || 'bi-dot';
      const tt = titleByStatus[status] || status;
      const etapaPart = etapaName ? (etapaName + ': ') : '';
      // mantém data-bs-toggle, title e delegação cuidará da instanciação
      return `<span class="status-etapa ${status}" data-bs-toggle="tooltip" title="${etapaPart}${tt}">
        <i class="bi ${ic}"></i>
      </span>`;
    }

    function renderEtapasCell(etapasObj) {
      const order = ['escrituracao','notas','dam','certidao'];
      const labels = {
        escrituracao: 'Escrituração',
        notas: 'Notas',
        dam: 'DAM',
        certidao: 'Certidão'
      };
      return order.map(key => {
        const st = (etapasObj && etapasObj[key]) || 'pending';
        return badgeEtapa(st, labels[key]);
      }).join('');
    }

    function renderStatusTable(data) {
      tbodyStatus.innerHTML = '';
      const rows = (data && data.rows) || [];

      rows.forEach(r => {
        const tr = document.createElement('tr');
        tr.dataset.runId = r.run_id;
        tr.dataset.cnpjDigits = r.cnpj_digits;

        const localCnpj = findLocalCnpj(r.cnpj_digits);
        let razao = r.razao || '';
        let conta = r.conta || '';

        if (localCnpj) {
          razao = localCnpj.razao || razao;
          const acc = ACCS[localCnpj.account_id] || {};
          conta = acc.alias || acc.user || conta;
        }

        tr.innerHTML = `
          <td>
            <div class="cnpj-info">
              <div class="cnpj">${fmtCnpj(r.cnpj_digits)}</div>
              <div class="razao">${razao || ''}</div>
            </div>
          </td>
          <td>${conta || ''}</td>
          <td class="text-center">${(r.mes || '').toString().padStart(2,'0')}/${r.ano || ''}</td>
          <td class="text-center">${renderEtapasCell(r.etapas || {})}</td>
          <td class="text-center">${badgeEtapa(r.status_geral)}</td>
          <td class="text-center">
            <button class="btn btn-sm btn-outline-info btn-log-cnpj"
                    data-run-id="${r.run_id}"
                    data-cnpj="${r.cnpj_digits}"
                    data-bs-toggle="modal"
                    data-bs-target="#modalLogs">
              <i class="bi bi-journal-text"></i>
            </button>
          </td>`;
        tbodyStatus.appendChild(tr);
      });

      // NÃO criamos tooltips individualmente aqui — usamos delegação única inicializada no DOMContentLoaded
    }

    async function loadAll() {
      loadFromLocal();
      renderAccounts();
      renderCnpjs();
      try {
        const [kpisData, statusData] = await Promise.all([
          fetchJSON('/api/kpis'),
          fetchJSON('/api/status'),
        ]);
        applyKpis(kpisData);
        renderStatusTable(statusData);
      } catch (e) {
        console.error(e);
      }
    }

    async function loadStatus() {
      try {
        const [kpisData, statusData] = await Promise.all([
          fetchJSON('/api/kpis'),
          fetchJSON('/api/status'),
        ]);
        applyKpis(kpisData);
        renderStatusTable(statusData);
      } catch (e) {
        console.error(e);
      }
    }

    // ====================== WEBSOCKET STATUS/LOGS ======================

    function connectWS() {
      if (STATUS_WS && (STATUS_WS.readyState === WebSocket.OPEN || STATUS_WS.readyState === WebSocket.CONNECTING)) {
        return;
      }
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      STATUS_WS = new WebSocket(proto + '://' + window.location.host + '/ws');

      STATUS_WS.onopen = () => {
        console.log('[WS] conectado');
        WS_RETRY_MS = 1000;
      };

      STATUS_WS.onmessage = (ev) => {
        let msg;
        try {
          msg = JSON.parse(ev.data);
        } catch {
          return;
        }
        if (msg.type === 'hello') {
          console.log('[WS] hello tenant=', msg.tenant);
        } else if (msg.type === 'runs') {
          CURRENT_RUNS = msg.runs || {};
        } else if (msg.type === 'status') {
          applyKpis({ kpis: msg.kpis || {} });
          renderStatusTable({ rows: msg.rows || [] });
        } else if (msg.type === 'log') {
          // opcional: exibir log em tempo real em alguma área
          // console.log('[WS LOG]', msg.job_id, msg.line);
        } else if (msg.type === 'ping') {
          // ignore
        }
      };

      STATUS_WS.onclose = () => {
        console.log('[WS] fechado, tentando reconectar...');
        STATUS_WS = null;
        setTimeout(connectWS, WS_RETRY_MS);
        WS_RETRY_MS = Math.min(WS_RETRY_MS * 2, 10000);
      };

      STATUS_WS.onerror = () => {
        try { STATUS_WS.close(); } catch (e) {}
      };
    }

    // ========= AÇÕES DE FORMULÁRIO DE CONTA (LOCALSTORAGE) =========
    document.getElementById('form-account').addEventListener('submit', (e) => {
      e.preventDefault();
      const id = document.getElementById('acc-id').value || genId();
      const provider = document.getElementById('acc-provider').value || '1';
      const user = document.getElementById('acc-user').value.trim();
      const password = document.getElementById('acc-pass').value;
      const alias = document.getElementById('acc-alias').value.trim();
      if (!user || !password) return;

      ACCS[id] = {
        id,
        provider,
        user,
        password,
        alias,
      };
      saveToLocal();
      document.getElementById('acc-id').value = '';
      document.getElementById('acc-pass').value = '';
      document.getElementById('acc-user').value = '';
      document.getElementById('acc-alias').value = '';
      renderAccounts();
      renderCnpjs();
      loadStatus();
    });

    document.getElementById('acc-reset').addEventListener('click', () => {
      document.getElementById('acc-id').value = '';
      document.getElementById('acc-user').value = '';
      document.getElementById('acc-pass').value = '';
      document.getElementById('acc-alias').value = '';
    });

    accListDiv.addEventListener('click', (e) => {
      const btn = e.target.closest('button');
      if (!btn) return;
      const editId = btn.getAttribute('data-edit');
      const delId = btn.getAttribute('data-del');
      if (editId && ACCS[editId]) {
        const a = ACCS[editId];
        document.getElementById('acc-id').value = a.id;
        document.getElementById('acc-user').value = a.user;
        document.getElementById('acc-alias').value = a.alias || '';
      } else if (delId) {
        if (!confirm('Remover conta? CNPJs que apontarem para ela ficarão sem conta.')) return;
        delete ACCS[delId];
        Object.values(CNPJS).forEach(c => {
          if (c.account_id === delId) c.account_id = '';
        });
        saveToLocal();
        renderAccounts();
        renderCnpjs();
        loadStatus();
      }
    });

    // === CNPJs (modal / LOCALSTORAGE) ===
    document.getElementById('form-cnpj').addEventListener('submit', (e) => {
      e.preventDefault();
      const id = document.getElementById('cnpj-id').value || genId();
      const account_id = document.getElementById('cnpj-account').value;
      const cnpj = document.getElementById('inp-cnpj').value;
      const razao = document.getElementById('inp-razao').value;
      const dominio = document.getElementById('inp-dominio').value;
      if (!account_id || !cnpj) return;

      CNPJS[id] = {
        id,
        account_id,
        cnpj: cnpj.replace(/\\D/g, ''),
        razao,
        dominio,
      };
      saveToLocal();

      document.getElementById('cnpj-id').value = '';
      document.getElementById('inp-cnpj').value = '';
      document.getElementById('inp-razao').value = '';
      document.getElementById('inp-dominio').value = '';
      const modalEl = document.getElementById('modalCNPJ');
      const modal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
      modal.hide();
      renderCnpjs();
      renderCnpjsLancar();
      loadStatus();
    });

    tbodyCnpjs.addEventListener('click', (e) => {
      const btn = e.target.closest('button');
      if (!btn) return;
      const editId = btn.getAttribute('data-edit');
      const delId = btn.getAttribute('data-del');
      if (editId && CNPJS[editId]) {
        const c = CNPJS[editId];
        document.getElementById('cnpj-id').value = c.id;
        document.getElementById('cnpj-account').value = c.account_id;
        document.getElementById('inp-cnpj').value = fmtCnpj(c.cnpj);
        document.getElementById('inp-razao').value = c.razao || '';
        document.getElementById('inp-dominio').value = c.dominio || '';
        document.getElementById('cnpj-modal-title').textContent = 'Editar CNPJ';
        const modalEl = document.getElementById('modalCNPJ');
        const modal = new bootstrap.Modal(modalEl);
        modal.show();
      } else if (delId) {
        if (!confirm('Remover este CNPJ?')) return;
        delete CNPJS[delId];
        saveToLocal();
        renderCnpjs();
        renderCnpjsLancar();
        loadStatus();
      }
    });

    // === Import XLSX (servidor só parseia, front grava em localStorage) ===
    document.getElementById('form-import').addEventListener('submit', async (e) => {
      e.preventDefault();
      const accId = importAccountSelect.value;
      const fileInput = document.getElementById('import-file');
      const statusEl = document.getElementById('import-status');
      if (!accId || !fileInput.files[0]) return;
      const fd = new FormData();
      fd.append('file', fileInput.files[0]);
      fd.append('account_id', accId);
      statusEl.textContent = 'Importando...';
      try {
        const resp = await fetchJSON('/api/import_cnpjs', { method: 'POST', body: fd });
        const list = resp.cnpjs || {};
        let imported = 0;
        (list || []).forEach(row => {
          const id = genId();
          CNPJS[id] = {
            id,
            account_id: accId,
            cnpj: (row.cnpj || '').replace(/\\D/g, ''),
            razao: row.razao || '',
            dominio: row.dominio || '',
          };
          imported++;
        });
        saveToLocal();
        statusEl.textContent = 'Importados: ' + imported;
        renderCnpjs();
        renderCnpjsLancar();
        loadStatus();
      } catch (err) {
        statusEl.textContent = 'Erro ao importar: ' + err.message;
      }
    });

    // === Seleção de CNPJs (aba lançar) ===
    tbodySelectorLancar.addEventListener('change', (e) => {
      if (e.target.type === 'checkbox') updateSelectedCountLancar();
    });
    if (chkAllLancar) {
      chkAllLancar.addEventListener('change', () => {
        tbodySelectorLancar.querySelectorAll('input[type="checkbox"]').forEach(chk => {
          chk.checked = chkAllLancar.checked;
        });
        updateSelectedCountLancar();
      });
    }
    searchLancar.addEventListener('input', renderCnpjsLancar);
    filterAccLancar.addEventListener('change', renderCnpjsLancar);
    filterDomLancar.addEventListener('change', renderCnpjsLancar);

    // === Enfileirar execução (COM confirmação) ===
    btnEnfileirar.addEventListener('click', async () => {
      const provider = document.getElementById('provider').value || '1';
      const mes = document.getElementById('mes').value;
      const ano = document.getElementById('ano').value;
      const etapas = {
        escrituracao: document.getElementById('etapa_escrituracao').checked,
        notas: document.getElementById('etapa_notas').checked,
        dam: document.getElementById('etapa_dam').checked,
        certidao: document.getElementById('etapa_certidao').checked,
      };
      const formato_dominio = document.getElementById('formato_dominio').checked;
      const cnpj_ids = [];
      tbodySelectorLancar.querySelectorAll('tr').forEach(tr => {
        const chk = tr.querySelector('input[type="checkbox"]');
        if (chk && chk.checked) cnpj_ids.push(tr.dataset.id);
      });
      if (!cnpj_ids.length) {
        enfileirarResult.className = 'mt-3 small text-center text-danger';
        enfileirarResult.textContent = 'Selecione pelo menos um CNPJ.';
        return;
      }

      // Confirmação clara para o usuário
      const confirmMsg = `Tem certeza que deseja iniciar a execução para ${cnpj_ids.length} CNPJ(s)?\\nMês/Ano: ${mes}/${ano}`;
      if (!confirm(confirmMsg)) {
        return;
      }

      // feedback visual no botão
      const originalHtml = btnEnfileirar.innerHTML;
      btnEnfileirar.disabled = true;
      btnEnfileirar.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Enfileirando...';

      try {
        const resp = await fetchJSON('/api/enqueue', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            provider,
            mes,
            ano,
            etapas,
            formato_dominio,
            cnpj_ids,
            accounts: Object.values(ACCS),
            cnpjs: Object.values(CNPJS),
          }),
        });
        enfileirarResult.className = 'mt-3 small text-center text-success';
        enfileirarResult.innerHTML = `<i class="bi bi-check-circle-fill me-1"></i> ${resp.total_jobs} job(s) enfileirados para ${resp.total_cnpjs} CNPJ(s).`;
        setTimeout(() => { enfileirarResult.textContent = ''; }, 5000);
        await loadStatus();
      } catch (err) {
        enfileirarResult.className = 'mt-3 small text-center text-danger';
        enfileirarResult.textContent = 'Erro ao enfileirar: ' + err.message;
      } finally {
        btnEnfileirar.disabled = false;
        btnEnfileirar.innerHTML = originalHtml;
      }
    });

    // === Parar tudo ===
    document.getElementById('btn-parar-tudo').addEventListener('click', async () => {
      if (!confirm('Aplicar STOP em todas as contas/CNPJs deste painel?')) return;
      try {
        const resp = await fetchJSON('/api/stop_all', { method: 'POST' });
        alert(`STOP aplicado.\\nJobs abortados: ${resp.stopped_jobs}\\nPré-fila limpa: ${resp.prequeue_removed} itens.`);
        await loadStatus();
      } catch (err) {
        alert('Erro ao aplicar STOP: ' + err.message);
      }
    });

    // === Download ZIP real ===
    document.getElementById('btn-download').addEventListener('click', () => {
      window.location.href = '/api/download_zip';
    });

    // === Logs modal (por EXECUÇÃO / JOB / CNPJ) ===
    const logFilterSelect = document.getElementById('log-filter-etapa');
    const logContentElement = document.getElementById('full-log-content');
    const logsStatusSummary = document.getElementById('logs-status-summary');
    const modalLogsEl = document.getElementById('modalLogs');

    function applyLogFilter() {
      const selectedEtapa = logFilterSelect.value;

      if (!FULL_LOG_TEXT) {
        logContentElement.textContent = 'Nenhum log encontrado.';
        return;
      }

      if (selectedEtapa === 'all') {
        logContentElement.textContent = FULL_LOG_TEXT;
        return;
      }

      const needle = 'flow=' + selectedEtapa;
      const lines = FULL_LOG_TEXT.split("\\n");
      const filtered = lines
        .filter(line => line.toLowerCase().includes(needle))
        .join("\\n");

      logContentElement.textContent =
        filtered || 'Nenhum log encontrado para esta etapa.';
    }

    modalLogsEl.addEventListener('show.bs.modal', async (event) => {
      const btn = event.relatedTarget;
      if (!btn) return;

      const runId = btn.getAttribute('data-run-id');
      const jobId = btn.getAttribute('data-job-id');
      const cnpjDigitsAttr = btn.getAttribute('data-cnpj');

      logsStatusSummary.innerHTML = '';
      logContentElement.textContent = 'Carregando...';
      FULL_LOG_TEXT = '';

      if (LOG_REFRESH_INTERVAL) {
        clearInterval(LOG_REFRESH_INTERVAL);
        LOG_REFRESH_INTERVAL = null;
      }

      const loadLogs = async () => {
        try {
          let data;
          if (runId) {
            data = await fetchJSON('/api/logs_run/' + runId);
          } else if (jobId) {
            data = await fetchJSON('/api/logs_job/' + jobId);
          } else if (cnpjDigitsAttr) {
            data = await fetchJSON('/api/logs/' + cnpjDigitsAttr);
          } else {
            logContentElement.textContent = 'Nenhum identificador informado.';
            return;
          }

          const cnpjDigits = data.cnpj || cnpjDigitsAttr;
          const cnpjMask = data.cnpj_mask || (cnpjDigits ? fmtCnpj(cnpjDigits) : '');
          const e = data.etapas || {};

          if (data.run_id) {
            const mm = (data.mes || '').toString().padStart(2,'0');
            document.getElementById('modalLogsTitle').textContent =
              `Logs de Execução - ${cnpjMask} (${mm}/${data.ano || ''})`;
          } else if (data.job_id) {
            const jobShort = data.job_id.substring(0, 8) + '…';
            const etapa = etapaLabel(data.etapa || '');
            document.getElementById('modalLogsTitle').textContent =
              `Logs de Execução - Job ${jobShort} / ${cnpjMask} (${etapa || 'Etapa desconhecida'})`;
          } else {
            document.getElementById('modalLogsTitle').textContent =
              'Logs de Execução - CNPJ ' + cnpjMask;
          }

          logsStatusSummary.innerHTML = `
            <div class="col-md-6">
              <div class="log-item d-flex justify-content-between align-items-center">
                <span class="fw-bold">Escrituração:</span>
                <span class="badge status-badge text-bg-${badgeColor(e.escrituracao)}">${labelStatus(e.escrituracao)}</span>
              </div>
              <div class="log-item d-flex justify-content-between align-items-center">
                <span class="fw-bold">Notas:</span>
                <span class="badge status-badge text-bg-${badgeColor(e.notas)}">${labelStatus(e.notas)}</span>
              </div>
            </div>
            <div class="col-md-6">
              <div class="log-item d-flex justify-content-between align-items-center">
                <span class="fw-bold">DAM:</span>
                <span class="badge status-badge text-bg-${badgeColor(e.dam)}">${labelStatus(e.dam)}</span>
              </div>
              <div class="log-item d-flex justify-content-between align-items-center">
                <span class="fw-bold">Certidão:</span>
                <span class="badge status-badge text-bg-${badgeColor(e.certidao)}">${labelStatus(e.certidao)}</span>
              </div>
            </div>`;

          FULL_LOG_TEXT = sanitizeLogText(data.logs || '');
          applyLogFilter();
        } catch (err) {
          logContentElement.textContent = 'Erro ao carregar logs: ' + err.message;
        }
      };

      await loadLogs();
      LOG_REFRESH_INTERVAL = setInterval(() => {
        loadLogs().catch(() => {});
      }, 3000);
    });

    modalLogsEl.addEventListener('hidden.bs.modal', () => {
      if (LOG_REFRESH_INTERVAL) {
        clearInterval(LOG_REFRESH_INTERVAL);
        LOG_REFRESH_INTERVAL = null;
      }
      FULL_LOG_TEXT = '';
      logsStatusSummary.innerHTML = '';
      logContentElement.textContent = '';
    });

    function badgeColor(status) {
      switch (status) {
        case 'success': return 'success';
        case 'running': return 'primary';
        case 'waiting':
        case 'queued': return 'warning';
        case 'error': return 'danger';
        case 'pending': return 'secondary';
        default: return 'secondary';
      }
    }
    function labelStatus(status) {
      switch (status) {
        case 'success': return 'Concluído';
        case 'running': return 'Executando';
        case 'waiting':
        case 'queued': return 'Aguardando';
        case 'error': return 'Erro';
        case 'pending': return 'Não iniciado';
        default: return status || '-';
      }
    }

    logFilterSelect.addEventListener('change', () => {
      applyLogFilter();
    });

    document.getElementById('btn-logout').addEventListener('click', () => {
      window.location.href = '/logout';
    });

    // Carrega usuário logado, inicializa storage por colaborador e começa loop + WS
    fetchJSON('/api/me').then(data => {
      if (data && data.colaborador && navUsernameEl) {
        navUsernameEl.textContent = data.colaborador;
      }
      initStorageKeys((data && data.colaborador_norm) || 'default');
      loadAll();     // snapshot inicial via REST
      connectWS();   // tempo real via WS
      // fallback: se WS morrer, ainda fazemos um refresh leve
      setInterval(() => {
        if (!STATUS_WS || STATUS_WS.readyState !== WebSocket.OPEN) {
          loadStatus();
        }
      }, 30000);
    }).catch(() => {
      initStorageKeys('default');
      loadAll();
      connectWS();
      setInterval(() => {
        if (!STATUS_WS || STATUS_WS.readyState !== WebSocket.OPEN) {
          loadStatus();
        }
      }, 30000);
    });
  });
</script>
</body>
</html>
"""

# ===================== ROTAS (REST) =====================

@app.get("/")
async def root():
    return HTMLResponse(PAGE_CLIENT.replace("REPLACE_WITH_YOUR_FULL_PAGE_CLIENT_HTML", ""), media_type="text/html")

@app.get("/login")
async def login_page():
    return HTMLResponse(PAGE_LOGIN, media_type="text/html")

@app.post("/login")
async def do_login(request: Request):
    rd = app.state.redis_cmd
    form = await request.form()
    colaborador = (form.get("colaborador") or "").strip().lower()
    senha = form.get("senha") or ""

    validUsers = {
        "valdinar": "123456",
        "julio": "123456",
        "samia": "123456",
        "alan": "123456",
        "isack": "123456",
        "thais": "123456",
        "legiscontabilidade": "iss2025@",
        "procontabil": "iss@2025",
        "laryssa": "123456",
        "leticia-pro": "@iss2025",
        "Alessandra-pro": "@iss2025",
        "Euciene-pro": "@iss2025",
        "Beatriz-pro": "@iss2025",
        "Luan-pro": "@iss2025",
    }
    if colaborador not in validUsers or validUsers[colaborador] != senha:
        dbg("[login] inválido para", colaborador)
        raise HTTPException(401, "Usuário/senha inválidos")

    token = uuid.uuid4().hex
    sess_data = {"colaborador": colaborador, "colaborador_norm": slugify(colaborador), "created_at": time.time()}
    await rd.set(session_key(token), json.dumps(sess_data), ex=SESSION_TTL)

    dbg("[login] ok para", colaborador, "token", token[:8]+"…")

    resp = JSONResponse({"status": "ok", "colaborador": colaborador})
    resp.set_cookie(key=SESSION_COOKIE_NAME, value=token, max_age=SESSION_TTL, httponly=True, samesite="lax")
    return resp

@app.get("/logout")
async def logout(request: Request):
    rd = app.state.redis_cmd
    token = request.cookies.get(SESSION_COOKIE_NAME)

    if token:
        try:
            await rd.delete(session_key(token))
            dbg("[logout] sessão removida", token[:8]+"…")
        except Exception as e:
            dbg("[logout] erro ao deletar sessão:", repr(e))

    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie(SESSION_COOKIE_NAME)
    return resp

@app.get("/api/me")
async def api_me(request: Request):
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    return {"colaborador": sess.get("colaborador", ""), "colaborador_norm": sess.get("colaborador_norm", "")}

# --------- NOVO: import XLSX (para evitar 404) ---------
@app.post("/api/import_cnpjs")
async def api_import_cnpjs(account_id: str = Form(...), file: UploadFile = File(...)):
    """
    Espera colunas: CNPJ, Razão Social, Código Domínio
    """
    dbg("[import] recebendo arquivo:", file.filename, "para account:", account_id)
    try:
        from openpyxl import load_workbook
    except Exception as e:
        dbg("[import] openpyxl ausente:", repr(e))
        raise HTTPException(500, "Dependência 'openpyxl' não instalada no servidor.")

    try:
        content = await file.read()
        from io import BytesIO
        wb = load_workbook(BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        headers = []
        rows_out = []

        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            if i == 1:
                headers = [str(x or "").strip().lower() for x in row]
                continue
            record = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
            cnpj = str(record.get("cnpj") or record.get("cnpjs") or "").strip()
            razao = str(record.get("razão social") or record.get("razao social") or record.get("razao") or "").strip()
            dominio = str(record.get("código domínio") or record.get("codigo dominio") or record.get("dominio") or "").strip()
            if not cnpj:
                continue
            rows_out.append({"cnpj": cnpj, "razao": razao, "dominio": dominio})
        dbg(f"[import] linhas extraídas: {len(rows_out)}")
        return {"cnpjs": rows_out}
    except Exception as e:
        dbg("[import] erro:", repr(e))
        raise HTTPException(500, f"Falha ao ler XLSX: {e}")

@app.post("/api/enqueue")
async def api_enqueue(request: Request, payload: Dict[str, Any]):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm")
    if not tenant:
        raise HTTPException(401, "Sessão inválida")

    runs = await load_runs(rd, tenant)

    provider = str(payload.get("provider") or "1")
    mes = str(payload.get("mes") or "").zfill(2)
    ano = str(payload.get("ano") or "")
    etapas = payload.get("etapas") or {}
    formato_dominio = bool(payload.get("formato_dominio"))
    cnpj_ids = payload.get("cnpj_ids") or []

    accounts_list = payload.get("accounts") or []
    cnpjs_list = payload.get("cnpjs") or []
    accs = {str(a.get("id")): a for a in accounts_list if a.get("id")}
    cnps = {str(c.get("id")): c for c in cnpjs_list if c.get("id")}

    if not cnpj_ids:
        raise HTTPException(400, "Nenhum CNPJ selecionado.")

    mes_str = f"{mes}/{ano}"
    tipo_estrutura = "dominio" if formato_dominio else "convencional"

    func_map = {"escrituracao":"job_escrituracao","notas":"job_notas","dam":"job_dam","certidao":"job_certidao"}

    total_jobs = 0
    used_cnpjs = set()

    for cid in cnpj_ids:
        c = cnps.get(cid)
        if not c: continue

        account_id = c.get("account_id") or ""
        acc = accs.get(account_id)
        if not acc: continue

        usuario = (acc.get("user") or "").strip()
        senha = acc.get("password") or ""
        if not usuario or not senha: continue

        colaborador_norm = tenant
        cd = cnpj_digits(str(c.get("cnpj") or ""))
        if not cd or cd == "0"*14: continue

        used_cnpjs.add(cd)

        run_id = uuid.uuid4().hex
        run_job_ids: List[str] = []
        run_etapas_flags = {"escrituracao": False, "notas": False, "dam": False, "certidao": False}

        codigo_dom = (c.get("dominio") or "").strip() or None

        for etapa_key, enabled in etapas.items():
            if not enabled: continue
            func = func_map.get(etapa_key)
            if not func: continue

            if func == "job_notas":
                args = [colaborador_norm, cd, mes_str, usuario, senha, tipo_estrutura, codigo_dom]
            else:
                args = [colaborador_norm, cd, mes_str, usuario, senha, tipo_estrutura]

            job_id = uuid.uuid4().hex
            env = {"func": func, "args": args, "enqueue_kwargs": {"_job_id": job_id}, "colaborador_norm": colaborador_norm}

            y_key = f"{PREQUEUE_NS}:colab:{colaborador_norm}"
            await rd.sadd(PREQUEUE_COLABS_SET, colaborador_norm)
            await rd.rpush(y_key, json.dumps(env))
            total_jobs += 1
            run_job_ids.append(job_id)
            run_etapas_flags[etapa_key] = True

        if run_job_ids:
            runs[run_id] = {
                "id": run_id, "cnpj": cd, "mes": mes, "ano": ano, "mes_str": mes_str,
                "etapas": run_etapas_flags, "job_ids": run_job_ids, "account_id": account_id,
                "provider": provider, "tipo_estrutura": tipo_estrutura, "colaborador_norm": colaborador_norm,
                "created_at": time.time(),
            }

    if total_jobs == 0:
        raise HTTPException(400, "Nenhum job foi enfileirado (verifique senhas e contas).")

    await save_runs(rd, tenant, runs)
    await rd.publish(f"{REDIS_LOG_NS}:front:{tenant}:runs:updated", json.dumps(runs))
    await rd.set(FRONT_LAST_RUN_KEY, str(time.time()))

    dbg(f"[enqueue] tenant={tenant} jobs={total_jobs} cnpjs={len(used_cnpjs)} mes={mes_str}")
    return {"status":"ok","total_jobs": total_jobs,"total_cnpjs": len(used_cnpjs),"arq_enabled": ARQ_ENABLED}

@app.get("/api/kpis")
async def api_kpis(request: Request):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm", "")

    runs = await load_runs(rd, tenant)

    rel_cnpjs: Dict[str, Dict[str, Any]] = {}
    distinct_cnpjs = set()
    for run in runs.values():
        cd = cnpj_digits(run.get("cnpj", ""))
        mes_str = run.get("mes_str") or ""
        if not cd: continue
        distinct_cnpjs.add(cd)
        key = f"{cd}:{mes_str}" if mes_str else cd
        rel_cnpjs[key] = {"cnpj": cd, "mes": mes_str}

    idx = await build_status_index(rel_cnpjs, allowed_colab_prefix=tenant)
    kpis = idx["kpis"]
    kpis["total_cnpjs"] = len(distinct_cnpjs)

    last_raw = await rd.get(FRONT_LAST_RUN_KEY)
    last_str = "—"
    if last_raw:
        try:
            ts = float(last_raw)
            last_str = time.strftime("%d/%m %H:%M", time.localtime(ts))
        except Exception:
            pass
    kpis["last_run_str"] = last_str
    kpis["arq_enabled"] = ARQ_ENABLED
    return {"kpis": kpis}

# ===================== STATUS INDEX =====================

async def build_status_index(relevant_cnpjs: Dict[str, Dict[str, Any]], allowed_colab_prefix: Optional[str] = None) -> Dict[str, Any]:
    rd: AsyncRedisWS = rds()
    job_events: Dict[str, Dict[str, Any]] = {}
    etapas = ["escrituracao", "notas", "dam", "certidao"]

    keys = list(relevant_cnpjs.keys())
    if not keys:
        return {"status_per_cnpj": {}, "job_events": {}, "kpis": {"running": 0, "queue": 0, "total_cnpjs": 0}}

    events: Dict[str, Dict[str, List[Dict[str, Any]]]] = {k: {e: [] for e in etapas} for k in keys}

    running_count = 0
    queue_count = 0

    def add_job_event(job_id: Optional[str], cd: str, mes: str, etapa: str, status: str):
        if not job_id:
            return
        prev = job_events.get(job_id)
        if not prev:
            job_events[job_id] = {"cnpj": cd, "mes": mes or "", "etapa": etapa, "status": status}
        else:
            prev_status = prev.get("status", "pending")
            if STATUS_PRIORITY.get(status, 0) >= STATUS_PRIORITY.get(prev_status, 0):
                prev.update({"status": status, "cnpj": cd or prev.get("cnpj", ""), "mes": mes or prev.get("mes", ""), "etapa": etapa or prev.get("etapa", "")})

    def append_to_event_maps(cd: str, mes: str, etapa: str, st_str: str, job_id: str):
        key1 = cd
        key2 = f"{cd}:{mes}" if mes else None
        if key2 and key2 in events:
            events[key2].setdefault(etapa, []).append({"status": st_str, "job_id": job_id})
        if key1 in events:
            events[key1].setdefault(etapa, []).append({"status": st_str, "job_id": job_id})

    # 1) Pré-fila (waiting)
    try:
        colabs = await rd.smembers(PREQUEUE_COLABS_SET)
    except Exception:
        colabs = []

    for colab in colabs:
        y_key = f"{PREQUEUE_NS}:colab:{colab}"
        try:
            raw_items = await rd.lrange(y_key, 0, -1)
        except Exception:
            raw_items = []
        for raw in raw_items:
            try:
                env = json.loads(raw)
            except Exception:
                continue
            func = env.get("func") or ""
            args = env.get("args") or []
            meta = extract_meta(func, args, {})
            cd = cnpj_digits(meta["cnpj"])
            mes = meta.get("mes", "") or ""
            etapa = meta["etapa"] or func
            colab_norm = meta.get("colaborador_norm") or ""
            if allowed_colab_prefix and not str(colab_norm).startswith(allowed_colab_prefix):
                continue
            jid = env.get("enqueue_kwargs", {}).get("_job_id")
            st_str = "waiting"
            append_to_event_maps(cd, mes, etapa, st_str, jid or "")
            add_job_event(jid, cd, mes, etapa, st_str)
            queue_count += 1

    # 2) Jobs já rodados (logs das runs)
    tenant = allowed_colab_prefix or ""
    runs_for_tenant: Dict[str, Any] = {}
    if tenant:
        try:
            runs_for_tenant = await load_runs(rd, tenant)
        except Exception:
            runs_for_tenant = {}

    for run in runs_for_tenant.values():
        cd = cnpj_digits(run.get("cnpj", ""))
        if not cd:
            continue

        mes_str = run.get("mes_str") or ""
        if not mes_str:
            mes_only = str(run.get("mes") or "").zfill(2)
            ano_only = str(run.get("ano") or "").strip()
            mes_str = f"{mes_only}/{ano_only}" if mes_only and ano_only else mes_only

        job_ids = run.get("job_ids") or []
        for jid in job_ids:
            try:
                jlogs = await rd.lrange(task_logs_key(jid), 0, -1)
            except Exception:
                jlogs = []
            if not jlogs:
                continue

            etapa = detect_etapa_from_logs(jlogs) or ""
            if not etapa:
                flags = run.get("etapas") or {}
                true_steps = [k for k, v in flags.items() if v]
                if len(true_steps) == 1:
                    etapa = true_steps[0]

            status = detect_job_status_from_logs(jlogs)
            if not etapa:
                add_job_event(jid, cd, mes_str, "", status)
                continue

            append_to_event_maps(cd, mes_str, etapa, status, jid)
            add_job_event(jid, cd, mes_str, etapa, status)
            if status == "running":
                running_count += 1

    def reduce_status(lst: List[Dict[str, Any]]) -> str:
        if not lst:
            return "pending"
        flags = [e["status"] for e in lst]
        if "running" in flags: return "running"
        if "success" in flags and "error" not in flags: return "success"
        if "error" in flags: return "error"
        if "waiting" in flags or "queued" in flags: return "waiting"
        return "pending"

    status_per_cnpj: Dict[str, Dict[str, str]] = {}
    for key in events:
        s = {etapa: reduce_status(events[key].get(etapa, [])) for etapa in etapas}
        status_per_cnpj[key] = s

    return {"status_per_cnpj": status_per_cnpj, "job_events": job_events, "kpis": {"running": running_count, "queue": queue_count, "total_cnpjs": len(keys)}}

@app.get("/api/status")
async def api_status(request: Request):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm", "")

    runs = await load_runs(rd, tenant)
    rel_cnpjs: Dict[str, Dict[str, Any]] = {}
    for run in runs.values():
        cd = cnpj_digits(run.get("cnpj", ""))
        mes_str = run.get("mes_str") or ""
        if cd:
            key = f"{cd}:{mes_str}" if mes_str else cd
            rel_cnpjs[key] = {"cnpj": cd, "mes": mes_str}
    idx = await build_status_index(rel_cnpjs, allowed_colab_prefix=tenant)
    kpis = idx["kpis"]
    job_events: Dict[str, Dict[str, Any]] = idx.get("job_events", {})

    def reduce_status_list(statuses: List[str]) -> str:
        if not statuses: return "pending"
        cur = "pending"
        for st in statuses:
            cur = merge_status(cur, st)
        return cur

    rows: List[Dict[str, Any]] = []
    for run_id, run in runs.items():
        cd = cnpj_digits(run.get("cnpj", ""))
        if not cd: continue
        job_ids: List[str] = run.get("job_ids") or []
        etapas_status = {"escrituracao":"pending","notas":"pending","dam":"pending","certidao":"pending"}
        etapa_to_statuses: Dict[str, List[str]] = {k: [] for k in etapas_status.keys()}

        for jid in job_ids:
            je = job_events.get(jid)
            if not je: continue
            etapa = (je.get("etapa") or "").lower()
            st = je.get("status") or "pending"
            if etapa in etapa_to_statuses:
                etapa_to_statuses[etapa].append(st)

        for e in etapa_to_statuses:
            etapas_status[e] = reduce_status_list(etapa_to_statuses[e])
        geral_status = reduce_status_list(list(etapas_status.values()))

        rows.append({"run_id": run_id,"cnpj_digits": cd,"cnpj_mask": mask_cnpj(cd),"razao": "","conta": "","mes": run.get("mes") or "","ano": run.get("ano") or "","etapas": etapas_status,"status_geral": geral_status})
    rows.sort(key=lambda r: (r["cnpj_digits"], r["ano"], r["mes"], r["run_id"]))
    return {"rows": rows, "kpis": kpis, "arq_enabled": ARQ_ENABLED}

@app.get("/api/logs/{cnpj}")
async def api_logs_for_cnpj(request: Request, cnpj: str):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm", "")
    colab_prefix = tenant + "::" if tenant else ""

    cn = cnpj_digits(cnpj.strip().replace(".","").replace("/","").replace("-",""))
    mes = (request.query_params.get("mes") or "").zfill(2)
    ano = (request.query_params.get("ano") or "").strip()
    mes_key = f"{mes}/{ano}" if mes and ano else (mes if mes else "")
    key = f"{cn}:{mes_key}" if mes_key else cn
    rel_cnpjs = {key: {"cnpj": cn, "mes": mes_key}}
    idx = await build_status_index(rel_cnpjs, allowed_colab_prefix=tenant)
    etapas_status = idx["status_per_cnpj"].get(key) or idx["status_per_cnpj"].get(cn, {})

    logs_lines: List[str] = []
    job_ids_for_cnpj = set()

    runs = await load_runs(rd, tenant)
    for r in runs.values():
        if cnpj_digits(r.get("cnpj", "")) == cn:
            for jid in (r.get("job_ids") or []):
                job_ids_for_cnpj.add(jid)

    try:
        colabs = await rd.smembers(PREQUEUE_COLABS_SET)
    except Exception:
        colabs = []
    for colab in colabs:
        if colab_prefix and not str(colab).startswith(colab_prefix):
            continue
        y_key = f"{PREQUEUE_NS}:colab:{colab}"
        try:
            raw_items = await rd.lrange(y_key, 0, -1)
        except Exception:
            raw_items = []
        for raw_item in raw_items:
            try:
                env = json.loads(raw_item)
            except Exception:
                continue
            meta = extract_meta(env.get("func") or "", env.get("args") or [], {})
            colab_norm = meta.get("colaborador_norm") or ""
            if colab_prefix and not colab_norm.startswith(colab_prefix):
                continue
            if cnpj_digits(meta["cnpj"]) == cn:
                jid = env.get("enqueue_kwargs", {}).get("_job_id", "")
                if jid:
                    job_ids_for_cnpj.add(jid)

    for jid in sorted(job_ids_for_cnpj):
        try:
            jlogs = await rd.lrange(task_logs_key(jid), 0, -1)
        except Exception:
            jlogs = []
        if jlogs:
            logs_lines.append(f"=== Job {jid} ===")
            logs_lines.extend(jlogs)

    return {"cnpj": cn,"cnpj_mask": mask_cnpj(cn),"razao": "","etapas": {"escrituracao": etapas_status.get("escrituracao", "pending"),"notas": etapas_status.get("notas", "pending"),"dam": etapas_status.get("dam", "pending"),"certidao": etapas_status.get("certidao", "pending")},"logs": "\n".join(logs_lines),"arq_enabled": ARQ_ENABLED}

@app.get("/api/logs_job/{job_id}")
async def api_logs_for_job(request: Request, job_id: str):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm", "")
    colab_prefix = tenant + "::" if tenant else ""

    cn = ""; etapa = ""; status = "pending"; mes = ""

    try:
        colabs = await rd.smembers(PREQUEUE_COLABS_SET)
    except Exception:
        colabs = []
    for colab in colabs:
        if colab_prefix and not str(colab).startswith(colab_prefix):
            continue
        y_key = f"{PREQUEUE_NS}:colab:{colab}"
        try:
            raw_items = await rd.lrange(y_key, 0, -1)
        except Exception:
            raw_items = []
        found = False
        for raw_item in raw_items:
            try:
                env = json.loads(raw_item)
            except Exception:
                continue
            jid = env.get("enqueue_kwargs", {}).get("_job_id")
            if jid != job_id:
                continue
            func = env.get("func") or ""
            args = env.get("args") or []
            meta = extract_meta(func, args, {})
            colab_norm = meta.get("colaborador_norm") or ""
            if colab_prefix and not colab_norm.startswith(colab_prefix):
                continue
            cn = cnpj_digits(meta["cnpj"]); etapa = meta.get("etapa") or func; mes = meta.get("mes") or ""
            status = "waiting"; found = True; break
        if found: break

    if not cn:
        runs = await load_runs(rd, tenant)
        for r in runs.values():
            if job_id in (r.get("job_ids") or []):
                cn = cnpj_digits(r.get("cnpj", ""))
                mes = r.get("mes_str") or ""
                if not mes:
                    mes_only = (r.get("mes") or "").zfill(2)
                    ano_only = str(r.get("ano") or "").strip()
                    mes = f"{mes_only}/{ano_only}" if mes_only and ano_only else (mes_only or "")
                break

    if not cn:
        raise HTTPException(404, "Job não encontrado para este usuário.")

    try:
        jlogs = await rd.lrange(task_logs_key(job_id), 0, -1)
    except Exception:
        jlogs = []
    logs = "\n".join(jlogs or [])

    if jlogs:
        status = detect_job_status_from_logs(jlogs)

    etapas_status: Dict[str, str] = {}
    if cn:
        key = f"{cn}:{mes}" if mes else cn
        rel_cnpjs = {key: {"cnpj": cn, "mes": mes}}
        idx = await build_status_index(rel_cnpjs, allowed_colab_prefix=tenant)
        etapas_status = idx["status_per_cnpj"].get(key) or idx["status_per_cnpj"].get(cn, {})

    return {"job_id": job_id,"cnpj": cn,"cnpj_mask": mask_cnpj(cn) if cn else "","razao": "","etapa": etapa,"status": status,"etapas": {"escrituracao": etapas_status.get("escrituracao", "pending"),"notas": etapas_status.get("notas", "pending"),"dam": etapas_status.get("dam", "pending"),"certidao": etapas_status.get("certidao", "pending")},"logs": logs,"arq_enabled": ARQ_ENABLED}

@app.get("/api/logs_run/{run_id}")
async def api_logs_for_run(request: Request, run_id: str):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm", "")

    runs = await load_runs(rd, tenant)
    run = runs.get(run_id)
    if not run:
        raise HTTPException(404, "Execução não encontrada.")

    cn = cnpj_digits(run.get("cnpj", ""))
    job_ids: List[str] = run.get("job_ids") or []

    etapas_status_run: Dict[str, str] = {"escrituracao":"pending","notas":"pending","dam":"pending","certidao":"pending"}
    logs_lines: List[str] = []

    for jid in job_ids:
        try:
            jlogs = await rd.lrange(task_logs_key(jid), 0, -1)
        except Exception:
            jlogs = []
        if not jlogs:
            continue
        logs_lines.append(f"=== Job {jid} ==="); logs_lines.extend(jlogs)
        etapa_detectada = detect_etapa_from_logs(jlogs)
        job_status = detect_job_status_from_logs(jlogs)
        if etapa_detectada:
            cur = etapas_status_run.get(etapa_detectada, "pending")
            etapas_status_run[etapa_detectada] = merge_status(cur, job_status)

    return {"run_id": run_id,"cnpj": cn,"cnpj_mask": mask_cnpj(cn),"razao": "","mes": run.get("mes") or "","ano": run.get("ano") or "","etapas": etapas_status_run,"logs": "\n".join(logs_lines),"arq_enabled": ARQ_ENABLED}

@app.post("/api/stop_all")
async def api_stop_all(request: Request):
    rd = app.state.redis_cmd
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado")
    tenant = sess.get("colaborador_norm", "")
    tenant_prefix = tenant + "::" if tenant else ""

    runs = await load_runs(rd, tenant)
    colabs = set()
    try:
        prequeue_colabs = await rd.smembers(PREQUEUE_COLABS_SET)
        for c in prequeue_colabs:
            if tenant_prefix and str(c).startswith(tenant_prefix):
                colabs.add(str(c))
    except Exception:
        pass
    for r in runs.values():
        colab_norm = r.get("colaborador_norm")
        if colab_norm and (not tenant_prefix or str(colab_norm).startswith(tenant_prefix)):
            colabs.add(str(colab_norm))

    prequeue_removed = 0

    for c in colabs:
        key = f"{PREQUEUE_NS}:colab:{c}"
        try:
            n = await rd.llen(key)
            if n and n > 0:
                await rd.delete(key)
                prequeue_removed += n
        except Exception:
            pass
        try:
            await rd.srem(PREQUEUE_COLABS_SET, c)
        except Exception:
            pass

    dbg(f"[stop_all] tenant={tenant} prequeue_removed={prequeue_removed}")
    return {"status": "ok","stopped_jobs": 0,"prequeue_removed": prequeue_removed,"ids": [],"arq_enabled": ARQ_ENABLED}

# ================= PCLOUD (ZIP PÚBLICO 100% FUNCIONAL) =================


PCLOUD_API   = "https://api.pcloud.com"
PCLOUD_TOKEN = "MuRLgkZbz3P7ZeaIUlazWc9F7GAuXnBCeK4WPre7y"
PCLOUD_ROOT  = "/issbot"

def _pcloud_request(endpoint: str, *, params=None) -> dict:
    """
    Chama a API pCloud e retorna o JSON.
    Lança RuntimeError se 'result' != 0.
    """
    url = f"{PCLOUD_API}/{endpoint}"
    p = dict(params or {})
    p.setdefault("auth", PCLOUD_TOKEN)

    resp = requests.get(url, params=p, timeout=30)
    resp.raise_for_status()
    js = resp.json()
    if js.get("result") not in (0, True):
        # Algumas rotas usam 'result': True
        raise RuntimeError(js.get("error", f"Erro desconhecido em {endpoint}"))
    return js

def ensure_folder_published(folderid: int) -> str:
    """
    Garante que a pasta já tenha um link público.
    Tenta getfolderpublink → se 404, chama setpublink → então getfolderpublink de novo.
    Retorna o 'code'.
    """
    try:
        js = _pcloud_request("getfolderpublink", params={"folderid": folderid})
        # resposta: { "result":0, "metadata":{...}, "code":"XYZ", ... }
        return js["code"]
    except requests.HTTPError as e:
        # se deu 404, vamos publicar
        if e.response.status_code == 404:
            # cria novo
            js2 = _pcloud_request("setpublink", params={"folderid": folderid})
            code = js2.get("code")
            if not code:
                raise RuntimeError("Falha ao setpublink(publishfolder)")
            # então recupera
            js3 = _pcloud_request("getfolderpublink", params={"folderid": folderid})
            return js3["code"]
        raise

def get_zip_url_for_subpath(remote_subpath: str) -> str:
    """
    Gera um URL para download do ZIP público.
    """
    remote_subpath = (remote_subpath or "").strip("/")
    remote_path = f"{PCLOUD_ROOT}/{remote_subpath}"

    # 1) Stat para obter folderid
    js_stat = _pcloud_request("stat", params={"path": remote_path})
    md = js_stat.get("metadata") or {}
    folderid = md.get("folderid")
    if not folderid:
        raise RuntimeError(f"Pasta não existe em pCloud: {remote_path}")

    # 2) Garante link público e obtém o código
    code = ensure_folder_published(folderid)

    # 3) Gera o ZIP público
    js_zip = _pcloud_request("getpubziplink", params={"code": code})
    hosts    = js_zip.get("hosts")
    zip_path = js_zip.get("path")
    if not hosts or not zip_path:
        raise RuntimeError("Resposta inválida de getpubziplink")

    return f"https://{hosts[0]}{zip_path}"

# ================= FASTAPI ENDPOINT =================

@app.get("/api/download_zip")
async def api_download_zip(request: Request):
    # 1) Autenticação
    sess = getattr(request.state, "session", None) or await get_session_from_request(request)
    if not sess:
        raise HTTPException(401, "Não autenticado.")

    colaborador_norm = (sess.get("colaborador_norm") or "").strip()
    mes_norm         = (sess.get("mes_norm") or "").strip()

    if not colaborador_norm:
        raise HTTPException(400, "Sessão inválida (colaborador_norm ausente).")

    # 2) Monta caminho no pCloud
    remote_path = colaborador_norm + (f"/{mes_norm}" if mes_norm else "")

    # 3) Tenta gerar URL
    try:
        zip_url = get_zip_url_for_subpath(remote_path)
    except Exception as exc:
        raise HTTPException(500, f"Erro ao gerar ZIP no pCloud: {exc}")

    # 4) Redireciona para o ZIP
    return RedirectResponse(zip_url, status_code=302)



# ===================== WEBSOCKET: PUSH runs/status/log =====================

class LiveHub:
    def __init__(self):
        self.tenants_clients: Dict[str, Set[WebSocket]] = {}
        self.tenants_sub_task: Dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()

    async def attach(self, tenant: str, ws: WebSocket):
        async with self.lock:
            self.tenants_clients.setdefault(tenant, set()).add(ws)
            if tenant not in self.tenants_sub_task:
                self.tenants_sub_task[tenant] = asyncio.create_task(self._run_subscriber_loop(tenant))
        dbg("[livehub] attach tenant=", tenant, "clients=", len(self.tenants_clients.get(tenant, set())))

    async def detach(self, tenant: str, ws: WebSocket):
        async with self.lock:
            if tenant in self.tenants_clients:
                self.tenants_clients[tenant].discard(ws)
        dbg("[livehub] detach tenant=", tenant)

    async def _broadcast(self, tenant: str, payload: Dict[str, Any]):
        conns = self.tenants_clients.get(tenant, set())
        for c in list(conns):
            try:
                await c.send_json(payload)
            except Exception as e:
                conns.discard(c)
        dbg("[livehub] broadcast", payload.get("type"), "to", len(conns), "client(s)")

    async def _run_subscriber_loop(self, tenant: str):
        # Cada tenant tem seu próprio pubsub / conexão WS com o Redis
        pubsub = AsyncRedisPubSubWS(REDIS_WS_URL, token=REDIS_TOKEN)
        await pubsub.connect()

        runs_channel = f"{REDIS_LOG_NS}:front:{tenant}:runs:updated"
        await pubsub.subscribe(runs_channel)

        known_job_channels: Set[str] = set()
        runs: Dict[str, Any] = {}

        # Snapshot inicial
        try:
            runs = await load_runs(app.state.redis_cmd, tenant)
            await self._broadcast(tenant, {"type": "runs", "runs": runs})
            rows, kpis = await compute_status_for_tenant(tenant, runs)
            await self._broadcast(tenant, {"type": "status", "rows": rows, "kpis": kpis})

            for r in runs.values():
                for jid in (r.get("job_ids") or []):
                    ch = f"{REDIS_LOG_NS}:task:{jid}:logs"
                    if ch not in known_job_channels:
                        await pubsub.subscribe(ch)
                        known_job_channels.add(ch)
        except Exception as e:
            print("[livehub] snapshot inicial erro:", repr(e))

        try:
            while True:
                try:
                    msg = await pubsub.read_message()
                    if not msg:
                        await asyncio.sleep(0.05)
                        continue

                    if msg["type"] == "message":
                        channel = msg["channel"]
                        payload = msg.get("payload") or ""

                        if channel == runs_channel:
                            try:
                                runs = json.loads(payload or "{}")
                            except Exception:
                                runs = {}
                            await self._broadcast(tenant, {"type": "runs", "runs": runs})

                            new_jobs: Set[str] = set()
                            for r in runs.values():
                                for jid in (r.get("job_ids") or []):
                                    new_jobs.add(jid)
                            for jid in new_jobs:
                                ch = f"{REDIS_LOG_NS}:task:{jid}:logs"
                                if ch not in known_job_channels:
                                    await pubsub.subscribe(ch)
                                    known_job_channels.add(ch)

                            asyncio.create_task(self._push_status_async(tenant, runs))

                        elif channel.startswith(f"{REDIS_LOG_NS}:task:") and channel.endswith(":logs"):
                            try:
                                jid = channel.split(":task:")[1].rsplit(":", 1)[0]
                            except Exception:
                                jid = ""
                            await self._broadcast(
                                tenant,
                                {
                                    "type": "log",
                                    "job_id": jid,
                                    "line": payload,
                                    "channel": channel,
                                },
                            )
                            asyncio.create_task(self._push_status_async(tenant, runs))
                except Exception as e:
                    print("[livehub] loop erro:", repr(e))
                    await asyncio.sleep(0.25)
        finally:
            await pubsub.aclose()


    async def _push_status_async(self, tenant: str, runs: Dict[str, Any]):
        try:
            rows, kpis = await compute_status_for_tenant(tenant, runs)
            await self._broadcast(tenant, {"type": "status", "rows": rows, "kpis": kpis})
        except Exception as e:
            print("[livehub] _push_status_async erro:", repr(e))

livehub = LiveHub()

async def compute_status_for_tenant(tenant: str, runs: Dict[str, Any]):
    rel_cnpjs: Dict[str, Dict[str, Any]] = {}
    for run in runs.values():
        cd = cnpj_digits(run.get("cnpj", ""))
        if not cd: continue
        mes_str = run.get("mes_str") or ""
        key = f"{cd}:{mes_str}" if mes_str else cd
        rel_cnpjs[key] = {"cnpj": cd, "mes": mes_str}

    idx = await build_status_index(rel_cnpjs, allowed_colab_prefix=tenant)
    kpis = idx["kpis"]; job_events: Dict[str, Dict[str, Any]] = idx.get("job_events", {})

    def reduce_status_list(statuses: List[str]) -> str:
        if not statuses: return "pending"
        cur = "pending"
        for st in statuses:
            cur = merge_status(cur, st)
        return cur

    rows: List[Dict[str, Any]] = []
    for run_id, run in runs.items():
        cd = cnpj_digits(run.get("cnpj", ""))
        if not cd: continue
        job_ids: List[str] = run.get("job_ids") or []
        etapas_status = {"escrituracao":"pending","notas":"pending","dam":"pending","certidao":"pending"}
        etapa_to_statuses: Dict[str, List[str]] = {k: [] for k in etapas_status.keys()}

        for jid in job_ids:
            je = job_events.get(jid)
            if not je: continue
            etapa = (je.get("etapa") or "").lower()
            st = je.get("status") or "pending"
            if etapa in etapa_to_statuses:
                etapa_to_statuses[etapa].append(st)

        for e in etapa_to_statuses:
            etapas_status[e] = reduce_status_list(etapa_to_statuses[e])
        geral_status = reduce_status_list(list(etapas_status.values()))

        rows.append({"run_id": run_id,"cnpj_digits": cd,"cnpj_mask": mask_cnpj(cd),"razao": "","conta": "","mes": run.get("mes") or "","ano": run.get("ano") or "","etapas": etapas_status,"status_geral": geral_status})
    rows.sort(key=lambda r: (r["cnpj_digits"], r["ano"], r["mes"], r["run_id"]))

    last_raw = await app.state.redis_cmd.get(FRONT_LAST_RUN_KEY)
    last_str = "—"
    if last_raw:
        try:
            ts = float(last_raw)
            last_str = time.strftime("%d/%m %H:%M", time.localtime(ts))
        except Exception:
            pass
    kpis["last_run_str"] = last_str
    kpis["arq_enabled"] = ARQ_ENABLED

    return rows, kpis

@app.websocket("/ws")
async def ws_bridge(websocket: WebSocket):
    await websocket.accept()
    try:
        cookie_header = websocket.headers.get("cookie", "") or websocket.headers.get("Cookie", "")
        token = ""
        for part in cookie_header.split(";"):
            part = part.strip()
            if not part: continue
            if part.startswith(SESSION_COOKIE_NAME + "="):
                token = part.split("=", 1)[1]; break

        if not token:
            await websocket.send_json({"type": "error", "detail": "Não autenticado"}); await websocket.close(); return

        rd = app.state.redis_cmd
        raw = await rd.get(session_key(token))
        if not raw:
            await websocket.send_json({"type": "error", "detail": "Sessão inválida"}); await websocket.close(); return

        sess = json.loads(raw)
        tenant = sess.get("colaborador_norm") or ""
        if not tenant:
            await websocket.send_json({"type": "error", "detail": "Sessão sem tenant"}); await websocket.close(); return
    except Exception as e:
        await websocket.send_json({"type": "error", "detail": f"Falha de sessão: {e!s}"}); await websocket.close(); return

    await websocket.send_json({"type": "hello", "tenant": tenant})
    await livehub.attach(tenant, websocket)

    try:
        while True:
            try:
                _ = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping", "ts": time.time()})
    except WebSocketDisconnect:
        pass
    finally:
        await livehub.detach(tenant, websocket)

# ===================== SCHEDULER INTERNO — Y ➜ ARQ =====================

async def scheduler_y_to_arq():
    print("[scheduler] iniciado!")
    rd: AsyncRedisWS = app.state.redis_scheduler

    while True:
        try:
            colabs = await rd.smembers(PREQUEUE_COLABS_SET)
            if not colabs:
                await asyncio.sleep(1)
                continue

            for colab in colabs:
                y_key = f"{PREQUEUE_NS}:colab:{colab}"

                raw = await rd.lpop(y_key)
                if not raw:
                    continue

                env = json.loads(raw)
                func = env.get("func")
                args_list = env.get("args") or []
                job_id = env.get("enqueue_kwargs", {}).get("_job_id")
                if not job_id:
                    continue

                now_ms = int(time.time() * 1000)

                jobdef = {"t": 1,"f": func,"a": tuple(args_list),"k": {},"et": now_ms}

                await rd.set(f"arq:job:{job_id}", pickle.dumps(jobdef))
                await rd.zadd("arq:queue", now_ms, job_id)

                print(f"[scheduler] job {job_id} → ARQ OK")
        except Exception as e:
            print("[scheduler] erro:", repr(e))

        await asyncio.sleep(0.25)

# ===================== MAIN =====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("queue_client_front:app", host="0.0.0.0", port=8001, reload=True)

