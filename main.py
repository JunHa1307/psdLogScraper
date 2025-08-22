import asyncio
import websockets
import json
import os
import logging
import time
from typing import Dict, Set
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

LOG_DIR = "battle_logs"
WS_URL = "wss://sim3.psim.us/showdown/websocket"
ROOMLIST_PREFIX = "|queryresponse|roomlist|"
NUM_CONNS = 20      # number of WebSocket connections
MAX_PER_CONN = 20   # sockets per connection
POLL_INTERVAL = 120  # seconds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
os.makedirs(LOG_DIR, exist_ok=True)

class Conn:
    RECONNECT_THRESHOLD = 300       # 5분
    RECONNECT_CHECK_INTERVAL = 30
    JOIN_RATE_LIMIT_PER_SEC = 3

    def __init__(self, idx: int):
        self.idx = idx
        self.ws = None
        self.tracked: Set[str] = set()
        self.handles: Dict[str, any] = {}
        self.players: Dict[str, Set[str]] = {}
        self.last_activity: Dict[str, float] = {}
        self.room_cooldown: Dict[str, float] = {}   # 방별 재조인 쿨다운
        self._join_sem = asyncio.Semaphore(self.JOIN_RATE_LIMIT_PER_SEC)
        self._recv_task: asyncio.Task | None = None
        self._watch_task: asyncio.Task | None = None
        self._stop = False
        self.ready = asyncio.Event()  # ← 연결 준비 상태

    async def _rate_limiter(self):
        # 초당 JOIN_RATE_LIMIT_PER_SEC 토큰
        while not self._stop:
            await asyncio.sleep(1)
            try:
                # 세마포어 현재값을 조회해 부족한 만큼만 채움
                missing = self.JOIN_RATE_LIMIT_PER_SEC - self._join_sem._value
                for _ in range(max(0, missing)):
                    self._join_sem.release()
            except Exception:
                pass

    async def run(self):
        # 하나의 상위 루프에서 연결/수신/감시/재연결까지 일원화
        limiter_task = asyncio.create_task(self._rate_limiter())
        backoff = 1
        try:
            while not self._stop:
                try:
                    await self._connect_once()
                    backoff = 1  # 성공 시 백오프 초기화

                    # 수신/감시 태스크 기동
                    self._recv_task = asyncio.create_task(self.recv_loop())
                    self._watch_task = asyncio.create_task(self._room_watch())

                    # 둘 중 하나라도 끝나면 재연결로
                    await asyncio.wait(
                        {self._recv_task, self._watch_task},
                        return_when=asyncio.FIRST_COMPLETED
                    )

                finally:
                    # 태스크/소켓 정리
                    for t in (self._recv_task, self._watch_task):
                        if t and not t.done():
                            t.cancel()
                            try:
                                await t
                            except asyncio.CancelledError:
                                pass
                    self._recv_task = self._watch_task = None

                    if self.ws:
                        try:
                            await self.ws.close()
                            await self.ws.wait_closed()
                        except Exception:
                            pass
                        self.ws = None
                        self.ready.clear()   # 소켓 닫히면 ready 해제

                if self._stop:
                    break

                # 지수 백오프 (최대 60초)
                await asyncio.sleep(min(60, backoff))
                backoff = min(60, backoff * 2)
        finally:
            self._stop = True
            limiter_task.cancel()
            try:
                await limiter_task
            except asyncio.CancelledError:
                pass

    async def _connect_once(self):
        self.ready.clear()
        while True:
            try:
                self.ws = await websockets.connect(
                    WS_URL,
                    ping_interval=45,   # 완화
                    ping_timeout=20,
                    close_timeout=10,
                    open_timeout=30,
                    max_queue=1024
                )
                logging.info(f"[Conn{self.idx}] Connected")
                self.ready.set()  # 연결 완료

                # 이미 추적 중인 방은 재조인을 '천천히'
                for room in list(self.tracked):
                    await self._safe_join(room, rejoin=True)
                return
            except Exception as e:
                logging.warning(f"[Conn{self.idx}] connect failed: {e}; retry in 5s")
                await asyncio.sleep(5)

    async def _room_watch(self):
        """비활성 방만 천천히 재조인; 쿨다운 준수"""
        while True:
            await asyncio.sleep(self.RECONNECT_CHECK_INTERVAL)
            now = time.time()
            candidates = [
                r for r, ts in self.last_activity.items()
                if (r in self.tracked) and (now - ts > self.RECONNECT_THRESHOLD)
            ]
            for room in candidates:
                if self.room_cooldown.get(room, 0) > now:
                    continue
                logging.info(f"[Conn{self.idx}] room idle {room}, soft-rejoin")
                try:
                    await self._safe_leave(room)
                    await asyncio.sleep(1.5)
                    await self._safe_join(room, rejoin=True)
                    self.room_cooldown[room] = time.time() + 180  # 3분 쿨다운
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logging.warning(f"[Conn{self.idx}] soft-rejoin failed {room}: {e}")

    async def _safe_send(self, text: str, timeout: float = 5.0):
        # websockets 12+의 ClientConnection에는 .closed 속성이 없음
        # 연결 준비 여부는 ready Event로만 확인하고, 전송은 try/except로 판별
        if not self.ready.is_set() or self.ws is None:
            raise ConnectionClosedError(1006, "socket not open")

        try:
            await asyncio.wait_for(self.ws.send(text), timeout=timeout)
        except (ConnectionClosedOK, ConnectionClosedError):
            # 상위 run() 루프가 재연결하도록 동일 예외 유지
            raise
        except Exception as e:
            # 기타 전송 오류는 연결 문제로 간주
            raise ConnectionClosedError(1006, f"send failed: {e}")

    async def _safe_join(self, room: str, rejoin: bool = False):
        # 준비되지 않았으면 조인 스킵
        if not self.ready.is_set():
            logging.debug(f"[Conn{self.idx}] skip join {room}: not ready")
            return

        # 레이트리밋
        async with self._join_sem:
            await asyncio.sleep(0.05)  # 약간의 간격
            try:
                await self._safe_send(f"|/j {room}")
            except Exception as e:
                logging.warning(f"[Conn{self.idx}] join send failed {room}: {e}")
                return

            # 파일 오픈/상태 갱신
            try:
                fh = self.handles.get(room)
                if fh is None or fh.closed:
                    fh = open(os.path.join(LOG_DIR, f"{room}.log"), "a", encoding="utf-8")
                    self.handles[room] = fh
                self.tracked.add(room)
                self.players.setdefault(room, set())
                self.last_activity[room] = time.time()
                logging.info(f"[Conn{self.idx}] {'Re-joined' if rejoin else 'Joined'} {room}")
            except OSError as e:
                logging.error(f"[Conn{self.idx}] log open failed {room}: {e}")

    async def _safe_leave(self, room: str):
        if room not in self.tracked:
            return
        try:
            await self._safe_send(f"|/leave {room}")
        except Exception as e:
            logging.debug(f"[Conn{self.idx}] leave send failed {room}: {e}")
        fh = self.handles.pop(room, None)
        if fh:
            try:
                fh.close()
            except Exception:
                pass
        self.tracked.discard(room)
        self.players.pop(room, None)
        self.last_activity.pop(room, None)
        logging.info(f"[Conn{self.idx}] Left {room}")

    async def recv_loop(self):
        current_room = None
        async for msg in self.ws:
            now = time.time()
            for line in msg.split("\n"):
                if not line:
                    continue
                if line.startswith(">battle-"):
                    current_room = line[1:]
                    continue

                if current_room in self.tracked:
                    self.last_activity[current_room] = now
                    
                if line.startswith("|init|") and current_room:
                    self._on_init(current_room)

                if line == "|raw|<strong class=\"message-throttle-notice\">Your message was not sent because you've been typing too quickly.</strong>":
                    continue

                fh = self.handles.get(current_room)
                if fh and not fh.closed:
                    fh.write(line + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())

                if line.startswith("|player|") and current_room in self.players:
                    parts = line.split("|")
                    if len(parts) >= 5 and parts[3].strip():
                        uname = f"☆{parts[3].strip()}"
                        self.players[current_room].add(uname)

                if line.startswith("|j|") and current_room in self.players:
                    parts = line.split("|")
                    if len(parts) >= 3 and parts[2].strip():
                        raw = parts[2].strip()
                        uname = f"☆{raw}" if (not raw.startswith("☆") and f"☆{raw}" in self.players[current_room]) else raw
                        self.players[current_room].add(uname)
                        
                if line.startswith('|noinit|nonexistent|The room "'):
                    asyncio.create_task(self._schedule_leave(current_room))
                    
                if line.startswith("|deinit"):
                    logging.info(f"[Conn{self.idx}] deinit {current_room}, scheduling leave")
                    asyncio.create_task(self._schedule_leave(current_room))

                if line.startswith("|l|") and current_room in self.players:
                    parts = line.split("|")
                    if len(parts) >= 3 and parts[2].strip():
                        raw = parts[2].strip().lstrip("☆")
                        leaving = f"☆{raw}"
                        self.players[current_room].discard(leaving)
                    if not self.players[current_room]:
                        logging.info(f"[Conn{self.idx}] all left {current_room}, scheduling leave")
                        asyncio.create_task(self._schedule_leave(current_room))

    async def _schedule_leave(self, room: str):
        await asyncio.sleep(1.0)
        await self._safe_leave(room)
        
    def _on_init(self, room: str):
        """이 방에서 |init|을 받으면 파일을 갈아엎고(0바이트로) 새로 기록 시작"""
        fh = self.handles.get(room)
        if fh is None or fh.closed:
            # 혹시 핸들이 없다면 생성
            fh = open(os.path.join(LOG_DIR, f"{room}.log"), "a", encoding="utf-8")
            self.handles[room] = fh
        try:
            fh.seek(0)
            fh.truncate()
            fh.flush()
            os.fsync(fh.fileno())
        except Exception as e:
            logging.warning(f"[Conn{self.idx}] init truncate failed {room}: {e}")
        # 플레이어/상태도 초기화해두면 일관성 ↑
        self.players[room] = set()
        # init를 받았으니 마지막 활동 갱신
        self.last_activity[room] = time.time()

async def query_all_rooms() -> Set[str]:
    try:
        async with websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            open_timeout=30
        ) as ws:
            await ws.send("|/query roomlist")
            while True:
                msg = await ws.recv()
                for line in msg.split("\n"):
                    if line.startswith(ROOMLIST_PREFIX):
                        data = json.loads(line[len(ROOMLIST_PREFIX):] or "{}")
                        rooms = set(data.get("rooms", {}))
                        return {r for r in rooms if r.startswith("battle-")}
    except asyncio.TimeoutError:
        logging.warning("query_all_rooms: opening handshake timed out, retrying later")
        return set()
    except (ConnectionClosedOK, ConnectionClosedError):
        logging.info("query_all_rooms: 정상 종료, 빈 집합 반환")
        return set()
    except Exception as e:
        logging.error(f"query_all_rooms: 예기치 않은 오류: {e}", exc_info=True)
        return set()

async def main():
    conns = [Conn(i) for i in range(NUM_CONNS)]
    runners = [asyncio.create_task(c.run()) for c in conns]

    while True:
        all_rooms = await query_all_rooms()
        if not all_rooms:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        tracked_rooms = {r for c in conns for r in c.tracked}
        new_rooms = [r for r in (all_rooms - tracked_rooms)
                     if ('gen9' in r.lower() and 'random' not in r.lower())]

        for room in new_rooms:
            # ready 된 커넥션 중 가장 적게 추적 중인 커넥션 선택
            ready_conns = [c for c in conns if c.ready.is_set() and len(c.tracked) < MAX_PER_CONN]
            if not ready_conns:
                break
            target = min(ready_conns, key=lambda c: len(c.tracked))
            target.last_activity[room] = time.time()
            asyncio.create_task(target._safe_join(room))
            await asyncio.sleep(0.05)  # 조인 사이에 약간의 텀

        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
