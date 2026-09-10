import asyncio
import json
import random
import string
import time
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

WIN_LINES = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),
    (0, 3, 6), (1, 4, 7), (2, 5, 8),
    (0, 4, 8), (2, 4, 6),
]

MAX_PARTICIPANTS = 8
DISCONNECT_GRACE_SECONDS = 45
HEARTBEAT_SECONDS = 25

rooms: Dict[str, dict] = {}
connections: Dict[str, Dict[str, WebSocket]] = {}
pending_removal: Dict[str, Dict[str, asyncio.Task]] = {}
lock = asyncio.Lock()


def new_code() -> str:
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if code not in rooms:
            return code


def new_room(host_id: str, host_name: str) -> dict:
    return {
        "code": None,
        "host_id": host_id,
        "participants": {
            host_id: {
                "id": host_id, "name": host_name, "role": "spectator",
                "joined_at": time.time(), "connected": True,
            }
        },
        "pending": [],
        "board": [None] * 9,
        "turn": "X",
        "status": "lobby",  # lobby | playing | finished
        "winner": None,
        "scores": {"X": 0, "O": 0, "draws": 0},
        "created_at": time.time(),
    }


def check_winner(board):
    for a, b, c in WIN_LINES:
        if board[a] and board[a] == board[b] == board[c]:
            return board[a], (a, b, c)
    if all(board):
        return "draw", None
    return None, None


def public_state(room: dict) -> dict:
    participants = sorted(room["participants"].values(), key=lambda p: p["joined_at"])
    return {
        "type": "room_state",
        "code": room["code"],
        "host_id": room["host_id"],
        "participants": participants,
        "pending": [
            room["participants_pending"][uid]
            for uid in room["pending"]
            if uid in room.get("participants_pending", {})
        ],
        "board": room["board"],
        "turn": room["turn"],
        "status": room["status"],
        "winner": room["winner"],
        "win_line": room.get("win_line"),
        "scores": room["scores"],
    }


async def broadcast(code: str):
    room = rooms.get(code)
    if not room:
        return
    msg = json.dumps(public_state(room))
    dead = []
    for uid, ws in connections.get(code, {}).items():
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(uid)
    for uid in dead:
        connections[code].pop(uid, None)


async def send_to(code: str, user_id: str, payload: dict):
    ws = connections.get(code, {}).get(user_id)
    if ws:
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:
            pass


def reassign_host_if_needed(room: dict):
    if room["host_id"] not in room["participants"]:
        remaining = sorted(room["participants"].values(), key=lambda p: p["joined_at"])
        room["host_id"] = remaining[0]["id"] if remaining else None


def cancel_pending_removal(code: str, user_id: str):
    task = pending_removal.get(code, {}).pop(user_id, None)
    if task:
        task.cancel()


async def remove_participant_now(code: str, user_id: str):
    """Fully remove a participant (explicit leave, or grace period expired)."""
    room = rooms.get(code)
    if not room:
        return
    room["participants"].pop(user_id, None)
    room.get("participants_pending", {}).pop(user_id, None)
    if user_id in room["pending"]:
        room["pending"].remove(user_id)
    reassign_host_if_needed(room)
    cancel_pending_removal(code, user_id)
    if not room["participants"]:
        rooms.pop(code, None)
        connections.pop(code, None)
        pending_removal.pop(code, None)
    else:
        await broadcast(code)


async def schedule_removal(code: str, user_id: str):
    try:
        await asyncio.sleep(DISCONNECT_GRACE_SECONDS)
    except asyncio.CancelledError:
        return
    async with lock:
        room = rooms.get(code)
        if not room:
            return
        p = room["participants"].get(user_id)
        # Only actually remove if they never reconnected
        if p and not p.get("connected", True):
            await remove_participant_now(code, user_id)


async def heartbeat(websocket: WebSocket):
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            await websocket.send_text(json.dumps({"type": "heartbeat"}))
    except Exception:
        return


@app.websocket("/ws/{code}")
async def ws_endpoint(websocket: WebSocket, code: str):
    await websocket.accept()
    code = code.upper()
    user_id = websocket.query_params.get("uid")
    name = websocket.query_params.get("name", "Player")[:24] or "Player"
    action = websocket.query_params.get("action", "join")

    if not user_id:
        await websocket.close(code=4000)
        return

    async with lock:
        if action == "create" or code not in rooms:
            room = new_room(user_id, name)
            code = new_code()
            room["code"] = code
            rooms[code] = room
            connections[code] = {}
        else:
            room = rooms[code]
            if user_id in room["participants"]:
                # Reconnecting participant (e.g. brief network blip) — restore them,
                # don't treat this as a new join request.
                room["participants"][user_id]["connected"] = True
                cancel_pending_removal(code, user_id)
            else:
                if len(room["participants"]) + len(room["pending"]) >= MAX_PARTICIPANTS:
                    await websocket.close(code=4001)
                    return
                room.setdefault("participants_pending", {})
                room["participants_pending"][user_id] = {
                    "id": user_id, "name": name, "joined_at": time.time()
                }
                if user_id not in room["pending"]:
                    room["pending"].append(user_id)

        connections.setdefault(code, {})[user_id] = websocket

    await websocket.send_text(json.dumps({"type": "joined", "your_id": user_id, "code": code}))
    await broadcast(code)

    hb_task = asyncio.create_task(heartbeat(websocket))

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            async with lock:
                room = rooms.get(code)
                if not room:
                    continue
                mtype = msg.get("type")
                is_host = user_id == room["host_id"]

                if mtype == "approve" and is_host:
                    target = msg.get("user_id")
                    pend = room.get("participants_pending", {}).pop(target, None)
                    if target in room["pending"]:
                        room["pending"].remove(target)
                    if pend and len(room["participants"]) < MAX_PARTICIPANTS:
                        room["participants"][target] = {
                            "id": target, "name": pend["name"], "role": "spectator",
                            "joined_at": pend["joined_at"], "connected": True,
                        }

                elif mtype == "decline" and is_host:
                    target = msg.get("user_id")
                    room.get("participants_pending", {}).pop(target, None)
                    if target in room["pending"]:
                        room["pending"].remove(target)
                    await send_to(code, target, {"type": "declined"})

                elif mtype == "assign_role" and is_host:
                    target = msg.get("user_id")
                    role = msg.get("role")  # player_x | player_o | spectator
                    if target in room["participants"] and role in ("player_x", "player_o", "spectator"):
                        for p in room["participants"].values():
                            if p["role"] == role:
                                p["role"] = "spectator"
                        room["participants"][target]["role"] = role

                elif mtype == "start_match" and is_host:
                    roles = [p["role"] for p in room["participants"].values()]
                    if "player_x" in roles and "player_o" in roles:
                        room["board"] = [None] * 9
                        room["turn"] = "X"
                        room["status"] = "playing"
                        room["winner"] = None
                        room["win_line"] = None

                elif mtype == "move":
                    cell = msg.get("cell")
                    me = room["participants"].get(user_id)
                    if (
                        me and room["status"] == "playing"
                        and isinstance(cell, int) and 0 <= cell <= 8
                        and room["board"][cell] is None
                        and ((me["role"] == "player_x" and room["turn"] == "X")
                             or (me["role"] == "player_o" and room["turn"] == "O"))
                    ):
                        room["board"][cell] = room["turn"]
                        winner, line = check_winner(room["board"])
                        if winner == "draw":
                            room["status"] = "finished"
                            room["winner"] = "draw"
                            room["scores"]["draws"] += 1
                        elif winner:
                            room["status"] = "finished"
                            room["winner"] = winner
                            room["win_line"] = line
                            room["scores"][winner] += 1
                        else:
                            room["turn"] = "O" if room["turn"] == "X" else "X"

                elif mtype == "play_again" and is_host:
                    room["board"] = [None] * 9
                    room["turn"] = "X"
                    room["status"] = "lobby"
                    room["winner"] = None
                    room["win_line"] = None

                elif mtype == "signal":
                    target = msg.get("target")
                    await send_to(code, target, {
                        "type": "signal", "from": user_id, "data": msg.get("data"),
                    })
                    continue  # no broadcast needed for signaling

                elif mtype == "mute":
                    if user_id in room["participants"]:
                        room["participants"][user_id]["muted"] = bool(msg.get("muted"))

                elif mtype == "leave":
                    # Intentional leave — remove immediately, no grace period.
                    connections.get(code, {}).pop(user_id, None)
                    await remove_participant_now(code, user_id)
                    await websocket.close()
                    return

                await broadcast(code)

    except WebSocketDisconnect:
        pass
    finally:
        hb_task.cancel()
        async with lock:
            connections.get(code, {}).pop(user_id, None)
            room = rooms.get(code)
            if room:
                if user_id in room["participants"]:
                    # Unexpected disconnect (network blip, tab backgrounded, etc).
                    # Give them a grace period to reconnect before removing them
                    # for real, instead of instantly kicking them / killing the room.
                    room["participants"][user_id]["connected"] = False
                    pending_removal.setdefault(code, {})[user_id] = asyncio.create_task(
                        schedule_removal(code, user_id)
                    )
                    await broadcast(code)
                else:
                    # Was only a pending join request — safe to drop right away.
                    room.get("participants_pending", {}).pop(user_id, None)
                    if user_id in room["pending"]:
                        room["pending"].remove(user_id)
                    await broadcast(code)


@app.get("/api/health")
async def health():
    return {"status": "ok", "rooms": len(rooms)}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
