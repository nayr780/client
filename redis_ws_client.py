# redis_ws_client.py
import asyncio
from typing import Any, List, Optional, Tuple, Union
import websockets

# NOTE: RESP encoder/decoder minimal (suficiente para os comandos usados no front)
# Supports: Simple String (+), Error (-), Integer (:), Bulk String ($), Array (*)

class RedisRESPError(Exception):
    pass

class AsyncRedisWS:
    def __init__(self, url: str, token: str = "", connect_timeout: float = 10.0):
        # url = "wss://redisrender.onrender.com"
        self.url = f"{url}?token={token}" if token else url
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._lock = asyncio.Lock()
        self._connect_timeout = connect_timeout
        self._recv_task = None

    async def connect(self):
        if self._ws is not None and not getattr(self._ws, "closed", False):
            return
        self._ws = await websockets.connect(self.url, max_size=None)

    async def aclose(self):
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _send_raw(self, data: bytes):
        if self._ws is None or getattr(self._ws, "closed", False):
            await self.connect()
        await self._ws.send(data)

    async def _recv_raw(self) -> bytes:
        if self._ws is None or getattr(self._ws, "closed", False):
            await self.connect()
        msg = await self._ws.recv()
        return msg if isinstance(msg, (bytes, bytearray)) else msg.encode()

    # RESP encoding
    def _encode_cmd(self, *parts: Union[str, bytes, int]) -> bytes:
        out = bytearray()
        out.extend(f"*{len(parts)}\r\n".encode())
        for p in parts:
            s = str(p).encode() if not isinstance(p, (bytes, bytearray)) else bytes(p)
            out.extend(f"${len(s)}\r\n".encode())
            out.extend(s)
            out.extend(b"\r\n")
        return bytes(out)

    # RESP parsing
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
            prefix = chr(buf[pos])
            pos += 1

            if prefix == "+":
                line, pos2 = self._read_line(buf, pos)
                pos = pos2
                return line.decode()

            if prefix == "-":
                line, pos2 = self._read_line(buf, pos)
                pos = pos2
                raise RedisRESPError(line.decode())

            if prefix == ":":
                line, pos2 = self._read_line(buf, pos)
                pos = pos2
                return int(line.decode())

            if prefix == "$":
                line, pos2 = self._read_line(buf, pos)
                pos = pos2
                n = int(line.decode())
                if n == -1:
                    return None
                res = bytes(buf[pos:pos+n])
                pos += n + 2
                return res.decode()

            if prefix == "*":
                line, pos2 = self._read_line(buf, pos)
                pos = pos2
                n = int(line.decode())
                if n == -1:
                    return None
                arr = []
                for _ in range(n):
                    arr.append(parse_at())
                return arr

            raise RedisRESPError("unknown prefix")

        return parse_at()

    # executor genérico
    async def execute(self, *parts: Union[str, int, bytes]) -> Any:
        async with self._lock:
            payload = self._encode_cmd(*parts)
            await self._send_raw(payload)
            data = await self._recv_raw()
            return self._parse_resp(data)

    # -------------------------------------------------------
    # COMANDOS BÁSICOS
    # -------------------------------------------------------
    async def ping(self) -> bool:
        r = await self.execute("PING")
        return (r == "PONG") or (r is not None)

    async def get(self, key: str) -> Optional[str]:
        r = await self.execute("GET", key)
        return None if r is None else str(r)

    async def set(self, key: str, value: str, ex: Optional[int] = None):
        if ex is None:
            return await self.execute("SET", key, value)
        return await self.execute("SET", key, value, "EX", str(int(ex)))

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
        r = await self.execute("LRANGE", key, str(start), str(stop))
        return [] if not r else [str(x) for x in r]

    async def lpop(self, key: str) -> Optional[str]:
        r = await self.execute("LPOP", key)
        return None if r is None else str(r)

    async def scan(self, cursor: str = "0", match: str = None, count: int = 10):
        cmd = ["SCAN", cursor]
        if match:
            cmd += ["MATCH", match]
        if count:
            cmd += ["COUNT", str(count)]
        r = await self.execute(*cmd)
        if not r:
            return "0", []
        return str(r[0]), [str(x) for x in r[1]]

    async def ttl(self, key: str) -> Optional[int]:
        r = await self.execute("TTL", key)
        try: return int(r)
        except: return None

    # -------------------------------------------------------
    # HASHES (necessário pro ARQ + scheduler)
    # -------------------------------------------------------
    async def hmset(self, key: str, mapping: dict):
        # HMSET key field value field value...
        parts = ["HMSET", key]
        for f, v in mapping.items():
            parts.append(str(f))
            parts.append(str(v))
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

    # -------------------------------------------------------
    # ZSET (ESSENCIAL p/ ARQ)
    # -------------------------------------------------------
    async def zadd(self, key: str, score: float, member: str):
        return await self.execute("ZADD", key, str(score), member)

    async def zrem(self, key: str, *members: str):
        return await self.execute("ZREM", key, *members)

    async def zrange(self, key: str, start: int, stop: int, withscores=False):
        if withscores:
            r = await self.execute("ZRANGE", key, str(start), str(stop), "WITHSCORES")
            out = []
            it = iter(r)
            for m, s in zip(it, it):
                out.append((str(m), float(s)))
            return out
        else:
            r = await self.execute("ZRANGE", key, str(start), str(stop))
            return [] if not r else [str(x) for x in r]

    async def zrangebyscore(self, key: str, min_v: str, max_v: str, withscores=False):
        # ARQ usa ZRANGEBYSCORE ... WITHSCORES
        if withscores:
            r = await self.execute(
                "ZRANGEBYSCORE", key, min_v, max_v, "WITHSCORES"
            )
            out = []
            it = iter(r)
            for m, s in zip(it, it):
                out.append((str(m), float(s)))
            return out
        else:
            r = await self.execute("ZRANGEBYSCORE", key, min_v, max_v)
            return [] if not r else [str(x) for x in r]
