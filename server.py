#!/usr/bin/env python3
"""
LAN Room & Multiplayer Tetris Server
Zero-external-dependency (except aiohttp).
In-memory state for lobby chat and real-time multiplayer Tetris.
"""

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from typing import Dict, Any, Optional

from aiohttp import web, WSMsgType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lan_tetris")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(BASE_DIR, "index.html")

def get_lan_ips():
    """Detect local LAN IP addresses for display on server launch."""
    ips = []
    try:
        # Connect dummy socket to external IP to determine default route IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        primary_ip = s.getsockname()[0]
        s.close()
        if primary_ip and primary_ip != "127.0.0.1":
            ips.append(primary_ip)
    except Exception:
        pass

    try:
        hostname = socket.gethostname()
        for ip in socket.gethostbyname_ex(hostname)[2]:
            if not ip.startswith("127.") and ip not in ips:
                ips.append(ip)
    except Exception:
        pass

    return ips or ["127.0.0.1"]


class UserManager:
    """Manages connected clients and their state."""
    def __init__(self):
        self.users: Dict[str, Dict[str, Any]] = {}

    def add_user(self, user_id: str, ws: web.WebSocketResponse, nickname: str) -> Dict[str, Any]:
        user = {
            "id": user_id,
            "ws": ws,
            "nickname": nickname,
            "joined_at": time.time(),
            "in_battle": False,
            "ready": False,
            "alive": True,
            "score": 0,
            "lines": 0
        }
        self.users[user_id] = user
        return user

    def remove_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self.users.pop(user_id, None)

    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self.users.get(user_id)

    def get_public_list(self):
        return [
            {
                "id": u["id"],
                "nickname": u["nickname"],
                "in_battle": u["in_battle"]
            }
            for u in self.users.values()
        ]

    async def broadcast(self, message: dict, exclude_id: Optional[str] = None):
        """Broadcast JSON message to all connected clients."""
        payload = json.dumps(message, ensure_ascii=False)
        tasks = []
        for uid, user in list(self.users.items()):
            if exclude_id and uid == exclude_id:
                continue
            ws: web.WebSocketResponse = user.get("ws")
            if ws and not ws.closed:
                tasks.append(ws.send_str(payload))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class BattleManager:
    """Manages multiplayer Tetris match lifecycle and events."""
    def __init__(self, user_manager: UserManager):
        self.um = user_manager
        self.state = "waiting"  # "waiting", "countdown", "playing", "ended"
        self.host_id: Optional[str] = None
        self.players: Dict[str, Dict[str, Any]] = {}
        self.countdown_task: Optional[asyncio.Task] = None
        self.round_id = 0

    def join_battle(self, user_id: str) -> bool:
        user = self.um.get_user(user_id)
        if not user:
            return False
        user["in_battle"] = True
        user["ready"] = False
        user["alive"] = (self.state != "playing")
        user["score"] = 0
        user["lines"] = 0

        if self.host_id is None or self.host_id not in self.players:
            self.host_id = user_id

        self.players[user_id] = {
            "id": user_id,
            "nickname": user["nickname"],
            "ready": False,
            "alive": user["alive"],
            "score": 0,
            "lines": 0,
            "is_host": (user_id == self.host_id),
            "is_break": False,
            "spectator": (self.state == "playing")
        }

        # If game is already playing, this player joins as spectator for current match
        if self.state == "playing":
            self.players[user_id]["alive"] = False

        return True

    def leave_battle(self, user_id: str):
        user = self.um.get_user(user_id)
        if user:
            user["in_battle"] = False
            user["ready"] = False
        self.players.pop(user_id, None)

        if self.host_id == user_id:
            if self.players:
                self.host_id = list(self.players.keys())[0]
            else:
                self.host_id = None

        for p in self.players.values():
            p["is_host"] = (p["id"] == self.host_id)

        if not self.players:
            self._reset_match()
            return

        if self.state == "countdown":
            if not self._can_start():
                self._cancel_countdown()
        elif self.state == "playing":
            asyncio.create_task(self.check_game_over_conditions())

    def transfer_host(self, from_id: str, target_id: str) -> bool:
        if self.host_id == from_id and target_id in self.players:
            self.host_id = target_id
            for p in self.players.values():
                p["is_host"] = (p["id"] == self.host_id)
            return True
        return False

    def set_break(self, user_id: str, is_break: bool) -> bool:
        if user_id in self.players and self.state in ("waiting", "ended"):
            p = self.players[user_id]
            p["is_break"] = is_break
            if is_break:
                p["ready"] = False
            if self.state == "countdown":
                if not self._can_start():
                    self._cancel_countdown()
            return True
        return False

    def set_ready(self, user_id: str, is_ready: bool):
        if user_id in self.players and self.state in ("waiting", "ended"):
            p = self.players[user_id]
            if p.get("is_break", False):
                return
            p["ready"] = is_ready
            user = self.um.get_user(user_id)
            if user:
                user["ready"] = is_ready

            if self.state == "ended":
                self.state = "waiting"

            if self.state == "countdown" and not self._can_start():
                self._cancel_countdown()

    def _can_start(self) -> bool:
        active = [p for p in self.players.values() if not p.get("is_break", False)]
        if len(active) < 2:
            return False
        for p in active:
            if p["id"] != self.host_id and not p.get("ready", False):
                return False
        return True

    def start_battle(self, user_id: str) -> bool:
        if user_id != self.host_id or self.state not in ("waiting", "ended"):
            return False
        if not self._can_start():
            return False
        self.start_countdown()
        return True

    def start_countdown(self):
        if self.countdown_task and not self.countdown_task.done():
            return
        self.state = "countdown"
        self.round_id += 1
        self.countdown_task = asyncio.create_task(self._countdown_loop(self.round_id))

    def _cancel_countdown(self):
        if self.countdown_task and not self.countdown_task.done():
            self.countdown_task.cancel()
            self.countdown_task = None
        if self.state == "countdown":
            self.state = "waiting"

    def _reset_match(self):
        self._cancel_countdown()
        self.state = "waiting"
        for p in self.players.values():
            p["ready"] = False
            p["alive"] = True
            p["score"] = 0
            p["lines"] = 0
            p["spectator"] = False

    async def _countdown_loop(self, round_id: int):
        try:
            for count in [3, 2, 1]:
                if self.round_id != round_id:
                    return
                await self.broadcast_battle({
                    "type": "countdown",
                    "count": count
                })
                await asyncio.sleep(1.0)

            if self.round_id != round_id:
                return

            # GO!
            self.state = "playing"
            for p in self.players.values():
                if p.get("is_break", False):
                    p["alive"] = False
                    p["spectator"] = True
                else:
                    p["alive"] = True
                    p["spectator"] = False
                p["ready"] = True
                p["score"] = 0
                p["lines"] = 0

            await self.broadcast_battle({
                "type": "game_start",
                "players": list(self.players.values())
            })
            await self.broadcast_battle({
                "type": "battle_players",
                "battle": self.get_battle_info()
            })
        except asyncio.CancelledError:
            pass

    async def handle_player_death(self, user_id: str, score: int, lines: int):
        if user_id in self.players and self.state == "playing":
            p = self.players[user_id]
            p["alive"] = False
            p["score"] = score
            p["lines"] = lines

            await self.broadcast_battle({
                "type": "player_died",
                "id": user_id,
                "nickname": p["nickname"],
                "score": score,
                "lines": lines
            })

            await self.check_game_over_conditions()

    async def check_game_over_conditions(self):
        if self.state != "playing":
            return

        alive_players = [p for p in self.players.values() if p.get("alive", False) and not p.get("spectator", False)]
        total_participants = len([p for p in self.players.values() if not p.get("spectator", False)])

        if total_participants >= 2 and len(alive_players) <= 1:
            winner = alive_players[0] if alive_players else None
            self.state = "ended"
            await self.broadcast_battle({
                "type": "game_end",
                "winner_id": winner["id"] if winner else None,
                "winner_name": winner["nickname"] if winner else "DRAW",
                "players": list(self.players.values())
            })
        elif total_participants == 1 and len(alive_players) == 0:
            self.state = "ended"
            await self.broadcast_battle({
                "type": "game_end",
                "winner_id": None,
                "winner_name": None,
                "players": list(self.players.values())
            })

    def rematch(self, user_id: str):
        if user_id in self.players:
            p = self.players[user_id]
            p["ready"] = False
            if not p.get("is_break", False):
                p["alive"] = True
            if self.state == "ended":
                self.state = "waiting"

    async def route_garbage(self, from_id: str, lines: int, target_id: Optional[str] = None):
        """Route garbage attack lines to opponent(s)."""
        if self.state != "playing" or lines <= 0:
            return

        sender = self.players.get(from_id)
        sender_name = sender["nickname"] if sender else "Opponent"

        eligible_targets = [
            pid for pid, p in self.players.items()
            if pid != from_id and p.get("alive", False) and not p.get("spectator", False)
        ]

        if not eligible_targets:
            return

        if target_id and target_id in eligible_targets:
            destinations = [target_id]
        else:
            destinations = eligible_targets

        lines_per_target = max(1, lines // len(destinations)) if len(destinations) > 1 else lines

        for tid in destinations:
            target_user = self.um.get_user(tid)
            if target_user and target_user.get("ws"):
                msg = {
                    "type": "garbage_incoming",
                    "from_id": from_id,
                    "from_name": sender_name,
                    "lines": lines_per_target
                }
                try:
                    await target_user["ws"].send_str(json.dumps(msg, ensure_ascii=False))
                except Exception as e:
                    logger.warning("Error routing garbage to %s: %s", tid, e)

    async def broadcast_battle(self, message: dict, exclude_id: Optional[str] = None):
        payload = json.dumps(message, ensure_ascii=False)
        tasks = []
        for pid in list(self.players.keys()):
            if exclude_id and pid == exclude_id:
                continue
            user = self.um.get_user(pid)
            if user and user.get("ws") and not user["ws"].closed:
                tasks.append(user["ws"].send_str(payload))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_battle_info(self):
        return {
            "state": self.state,
            "host_id": self.host_id,
            "players": list(self.players.values())
        }


# Global Singletons
user_manager = UserManager()
battle_manager = BattleManager(user_manager)


async def http_index_handler(request: web.Request) -> web.Response:
    """Serve index.html with no-cache headers."""
    if not os.path.exists(INDEX_PATH):
        return web.Response(text="index.html not found on server", status=404)
    with open(INDEX_PATH, "r", encoding="utf-8") as f:
        content = f.read()
    return web.Response(
        text=content,
        content_type="text/html",
        charset="utf-8",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """WebSocket connection handler for both chat and Tetris battle."""
    ws = web.WebSocketResponse(heartbeat=25.0)
    await ws.prepare(request)

    user_id = str(uuid.uuid4())[:8]
    user_nickname = "Anonymous"
    is_joined = False

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue

                mtype = data.get("type")

                # 1. Join Request
                if mtype == "join":
                    raw_name = str(data.get("nickname", "")).strip()
                    if not raw_name:
                        raw_name = f"User_{user_id}"
                    # Truncate to max 20 chars
                    user_nickname = raw_name[:20]
                    user_manager.add_user(user_id, ws, user_nickname)
                    is_joined = True

                    # Welcome packet to this user
                    await ws.send_str(json.dumps({
                        "type": "welcome",
                        "user_id": user_id,
                        "nickname": user_nickname,
                        "users": user_manager.get_public_list(),
                        "battle": battle_manager.get_battle_info()
                    }, ensure_ascii=False))

                    # Broadcast user list update
                    await user_manager.broadcast({
                        "type": "user_list",
                        "users": user_manager.get_public_list(),
                        "count": len(user_manager.users)
                    })

                    # Broadcast system join notification in chat
                    await user_manager.broadcast({
                        "type": "chat",
                        "id": str(uuid.uuid4())[:8],
                        "sender": "SYSTEM",
                        "sender_id": "system",
                        "text": f"{user_nickname} が入室しました",
                        "timestamp": int(time.time() * 1000),
                        "is_system": True
                    })

                elif not is_joined:
                    continue

                # 2. Chat Message
                elif mtype == "chat":
                    raw_text = str(data.get("text", "")).strip()
                    if raw_text:
                        clean_text = raw_text[:1000]
                        await user_manager.broadcast({
                            "type": "chat",
                            "id": str(uuid.uuid4())[:8],
                            "sender": user_nickname,
                            "sender_id": user_id,
                            "text": clean_text,
                            "timestamp": int(time.time() * 1000),
                            "is_system": False
                        })

                # 3. Tetris Battle Room Actions
                elif mtype == "battle_join":
                    battle_manager.join_battle(user_id)
                    await battle_manager.broadcast_battle({
                        "type": "battle_players",
                        "battle": battle_manager.get_battle_info()
                    })
                    await user_manager.broadcast({
                        "type": "user_list",
                        "users": user_manager.get_public_list(),
                        "count": len(user_manager.users)
                    })

                elif mtype == "battle_leave":
                    battle_manager.leave_battle(user_id)
                    await battle_manager.broadcast_battle({
                        "type": "battle_players",
                        "battle": battle_manager.get_battle_info()
                    })
                    await user_manager.broadcast({
                        "type": "user_list",
                        "users": user_manager.get_public_list(),
                        "count": len(user_manager.users)
                    })

                elif mtype == "battle_ready":
                    is_ready = bool(data.get("ready", False))
                    battle_manager.set_ready(user_id, is_ready)
                    await battle_manager.broadcast_battle({
                        "type": "battle_players",
                        "battle": battle_manager.get_battle_info()
                    })

                elif mtype == "battle_break":
                    is_break = bool(data.get("is_break", False))
                    battle_manager.set_break(user_id, is_break)
                    await battle_manager.broadcast_battle({
                        "type": "battle_players",
                        "battle": battle_manager.get_battle_info()
                    })

                elif mtype == "battle_transfer_host":
                    target_id = str(data.get("target_id", ""))
                    if battle_manager.transfer_host(user_id, target_id):
                        await battle_manager.broadcast_battle({
                            "type": "battle_players",
                            "battle": battle_manager.get_battle_info()
                        })

                elif mtype == "battle_start":
                    if battle_manager.start_battle(user_id):
                        await battle_manager.broadcast_battle({
                            "type": "battle_players",
                            "battle": battle_manager.get_battle_info()
                        })

                elif mtype == "battle_rematch":
                    battle_manager.rematch(user_id)
                    await battle_manager.broadcast_battle({
                        "type": "battle_players",
                        "battle": battle_manager.get_battle_info()
                    })

                # 4. In-Game Sync Events
                elif mtype == "garbage":
                    lines = int(data.get("lines", 0))
                    target_id = data.get("target_id")
                    if 1 <= lines <= 20:
                        await battle_manager.route_garbage(user_id, lines, target_id)

                elif mtype == "board_update":
                    grid = data.get("grid")
                    score = int(data.get("score", 0))
                    lines = int(data.get("lines", 0))
                    incoming = int(data.get("incoming", 0))
                    if user_id in battle_manager.players:
                        battle_manager.players[user_id]["score"] = score
                        battle_manager.players[user_id]["lines"] = lines
                    await battle_manager.broadcast_battle({
                        "type": "opponent_board",
                        "id": user_id,
                        "grid": grid,
                        "score": score,
                        "lines": lines,
                        "incoming": incoming
                    }, exclude_id=user_id)

                elif mtype == "game_over":
                    score = int(data.get("score", 0))
                    lines = int(data.get("lines", 0))
                    await battle_manager.handle_player_death(user_id, score, lines)

            elif msg.type == WSMsgType.ERROR:
                logger.warning("WS connection error: %s", ws.exception())
    finally:
        if is_joined:
            user_manager.remove_user(user_id)
            battle_manager.leave_battle(user_id)

            await user_manager.broadcast({
                "type": "user_list",
                "users": user_manager.get_public_list(),
                "count": len(user_manager.users)
            })

            await user_manager.broadcast({
                "type": "chat",
                "id": str(uuid.uuid4())[:8],
                "sender": "SYSTEM",
                "sender_id": "system",
                "text": f"{user_nickname} が退出しました",
                "timestamp": int(time.time() * 1000),
                "is_system": True
            })

            await battle_manager.broadcast_battle({
                "type": "battle_players",
                "battle": battle_manager.get_battle_info()
            })

    return ws


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", http_index_handler)
    app.router.add_get("/ws", websocket_handler)
    return app


def main():
    # Render supplies PORT automatically. Locally, keep the original port 8000.
    port = int(os.environ.get("PORT", "8000"))
    host = "0.0.0.0"

    print("=" * 60)
    print("      LAN ROOM & MULTIPLAYER TETRIS SERVER")
    print("=" * 60)
    print(f"  Server: http://{host}:{port}")
    if "PORT" not in os.environ:
        print(f"  Local Access: http://localhost:{port}")
        for ip in get_lan_ips():
            print(f"  LAN Access:   http://{ip}:{port}")
    print("=" * 60)

    app = create_app()
    web.run_app(app, host=host, port=port, print=None)


if __name__ == "__main__":
    main()
