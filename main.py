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
POLL_INTERVAL = 60  # seconds
MAX_PER_CONN = 50   # sockets per connection
NUM_CONNS = 50      # number of WebSocket connections

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
os.makedirs(LOG_DIR, exist_ok=True)

class Conn:
    RECONNECT_THRESHOLD = 120   # 2분(초) 동안 비활성 방만 재조인
    RECONNECT_CHECK_INTERVAL = 30  # 30초마다 방별 상태 점검
    
    def __init__(self, idx: int):
        self.idx = idx
        self.ws: websockets.WebSocketClientProtocol = None
        self.tracked: Set[str] = set()
        self.handles: Dict[str, any] = {}
        self.players: Dict[str, Set[str]] = {}
        self.last_activity: Dict[str, float] = {}

    async def connect(self):
        # 재시도 루프
        while True:
            try:
                self.ws = await websockets.connect(
                    WS_URL,
                    ping_interval=20,     # 20초마다 ping
                    ping_timeout=10,      # pong을 10초 안에 못 받으면 끊김
                    close_timeout=5
                )
                logging.info(f"[Conn{self.idx}] Connected")
                # 재연결 이후, 이미 추적 중인 방들에 다시 조인
                for room in list(self.tracked):
                    await self.ws.send(f"|/j {room}")
                    logging.info(f"[Conn{self.idx}] Re-joined {room} after reconnect")
                    self.last_activity[room] = time.time()
                # 메시지 처리 및 idle watch 코루틴 실행
                asyncio.create_task(self.recv_loop())
                asyncio.create_task(self._reconnect_watch())
                break
            except Exception as e:
                logging.warning(f"[Conn{self.idx}] Connection failed: {e}, retrying in 5s")
                await asyncio.sleep(5)

    async def _reconnect_watch(self):
        """2분 inactivity 방만 leave/join 해서 재조인"""
        while True:
            await asyncio.sleep(self.RECONNECT_CHECK_INTERVAL)
            now = time.time()
            for room, ts in list(self.last_activity.items()):
                # 아직 트래킹 중이고, 마지막 메시지가 RECONNECT_THRESHOLD 초 넘게 없으면
                if room in self.tracked and now - ts > self.RECONNECT_THRESHOLD:
                    logging.info(f"[Conn{self.idx}] No activity in {room} for {self.RECONNECT_THRESHOLD}s, reconnecting room")
                    try:
                        # 1) leave
                        await self.leave_room(room)
                        
                        await asyncio.sleep(1)
                        
                        await self.join_room(room)
                        
                    except Exception as e:
                        logging.error(f"[Conn{self.idx}] Failed to reconnect {room}: {e}", exc_info=True)

    async def recv_loop(self):
        current_room = None
        try:
            async for msg in self.ws:
                now = time.time()
                for line in msg.split("\n"):
                    if not line:
                        continue

                    # 방 전환 태그 처리
                    if line.startswith(">battle-"):
                        current_room = line[1:]
                        continue

                    # 방별 마지막 활동 시각 갱신
                    if current_room in self.tracked:
                        self.last_activity[current_room] = now

                    if line == "|raw|<strong class=\"message-throttle-notice\">Your message was not sent because you've been typing too quickly.</strong>":
                        continue
                    
                    # 로그 기록
                    if current_room in self.handles:
                        fh = self.handles[current_room]
                        fh.write(line + "\n")
                        fh.flush()
                        os.fsync(fh.fileno())
                        

                    # 초기 플레이어 리스트
                    if line.startswith("|player|") and current_room in self.players:
                        parts = line.split("|")
                        if len(parts) >= 5 and parts[3].strip():
                            uname = f"☆{parts[3].strip()}"
                            self.players[current_room].add(uname)

                    # (선택) 관전자(join) 처리
                    if line.startswith("|j|") and current_room in self.players:
                        parts = line.split("|")
                        if len(parts) >= 3 and parts[2].strip():
                            raw = parts[2].strip()
                            if not raw.startswith("☆") and f"☆{raw}" in self.players[current_room]:
                                uname = f"☆{raw}"
                            else:
                                uname = raw
                            self.players[current_room].add(uname)
                            logging.info(f"[Conn{self.idx}] Player join {current_room}: {uname}")
                            
                    if line.startswith("|deinit"):
                        logging.info(f"[Conn{self.idx}] deinit {current_room}, scheduling leave")
                        asyncio.create_task(self._schedule_leave(current_room))

                    # 플레이어 퇴장 처리
                    if line.startswith("|l|") and current_room in self.players:
                        parts = line.split("|")
                        if len(parts) >= 3 and parts[2].strip():
                            raw = parts[2].strip().lstrip("☆")
                            leaving = f"☆{raw}"
                            self.players[current_room].discard(leaving)
                            logging.info(f"[Conn{self.idx}] Player left {current_room}: {leaving}")
                        if not self.players[current_room]:
                            logging.info(f"[Conn{self.idx}] All players left {current_room}, scheduling leave")
                            asyncio.create_task(self._schedule_leave(current_room))
                            
        except (ConnectionClosedOK, ConnectionClosedError) as e:
            logging.warning(f"[Conn{self.idx}] Connection closed: {e}. Reconnecting...")
            await self.connect()
        except Exception as e:
            logging.error(f"[Conn{self.idx}] Unexpected error: {e}", exc_info=True)
            await self.connect()

    async def _schedule_leave(self, room: str):
        await asyncio.sleep(1)
        if room in self.tracked:
            await self.leave_room(room)
            self.last_activity.pop(room, None)

    async def join_room(self, room: str):
        try:
            # 5초 안에 메시지 전송이 완료되지 않으면 TimeoutError
            await asyncio.wait_for(self.ws.send(f"|/j {room}"), timeout=5)
        except asyncio.TimeoutError:
            logging.error(f"[Conn{self.idx}] send join {room} timed out")
            # 필요하다면 여기서 재연결 시도하거나, 이 방은 건너뛸지 결정
            return
        except ConnectionClosedError as e:
            logging.warning(f"[Conn{self.idx}] Connection closed while sending join: {e}")
            # 재연결 로직으로 빠질 수 있도록 예외를 다시 던지거나 connect() 호출
            await self.connect()
            return
        except Exception as e:
            logging.error(f"[Conn{self.idx}] Unexpected error in join_room({room}): {e}", exc_info=True)
        
        # send 가 정상 완료되면 파일 오픈
        try:
            fh = open(os.path.join(LOG_DIR, f"{room}.log"), "a", encoding="utf-8")
        except OSError as e:
            logging.error(f"[Conn{self.idx}] Cannot open log file for {room}: {e}")
            return

        self.handles[room] = fh
        self.tracked.add(room)
        self.players[room] = set()
        logging.info(f"[Conn{self.idx}] Joined {room}")

    async def leave_room(self, room: str):
        await self.ws.send(f"|/leave {room}")
        fh = self.handles.pop(room, None)
        if fh:
            fh.close()
        self.tracked.remove(room)
        self.players.pop(room, None)
        logging.info(f"[Conn{self.idx}] Left {room}")

async def query_all_rooms() -> Set[str]:
    async with websockets.connect(WS_URL) as ws:
        try:
            await ws.send("|/query roomlist")
            while True:
                msg = await ws.recv()
                for line in msg.split("\n"):
                    if line.startswith(ROOMLIST_PREFIX):
                        data = json.loads(line[len(ROOMLIST_PREFIX):] or "{}")
                        rooms = set(data.get("rooms", {}))
                        return {r for r in rooms if r.startswith("battle-")}
        except ConnectionClosedOK:
            logging.info("query_all_rooms: Normal closure, retrying")
            return set()

async def main():
    conns = [Conn(i) for i in range(NUM_CONNS)]
    # 각 커넥션을 독립적으로 시작
    await asyncio.gather(*(c.connect() for c in conns))

    while True:
        all_rooms = await query_all_rooms()
        if not all_rooms:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        tracked_rooms = {r for c in conns for r in c.tracked}
        new_rooms = all_rooms - tracked_rooms

        for room in new_rooms:
            low = room.lower()
            if 'gen9' not in low or 'random' in low:
                continue
            for c in conns:
                if len(c.tracked) < MAX_PER_CONN:
                    c.last_activity[room] = time.time()
                    await c.join_room(room)
                    break
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
