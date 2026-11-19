"""
YT Music Jam - senkron dinleme sunucusu.

Odalari (jam) yonetir, host'un oynatma durumunu misafirlere iletir,
ortak sarki sirasini tutar. Hicbir sey diske yazilmaz; her sey bellekte.

Calistirma:
    pip install -r requirements.txt
    python server.py            # varsayilan port 8787
    PORT=10000 python server.py # Render vb. platformlar PORT verir
"""

import asyncio
import json
import os
import random
import string
import time
import uuid
from http import HTTPStatus

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

PORT = int(os.environ.get("PORT", "8787"))
HOST_GRACE_SECONDS = 45      # host baglantisi koparsa bu sure beklenir, sonra devredilir
EMPTY_ROOM_TTL = 300         # kimse cevrimici degilse oda bu sure sonra silinir
MAX_QUEUE = 200
MAX_MEMBERS = 20
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # karistirilan karakterler yok (0/O, 1/I)


def now_ms() -> int:
    return int(time.time() * 1000)


def clean_str(value, limit=200) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()[:limit]


class Member:
    def __init__(self, client_id, name, avatar):
        self.client_id = client_id
        self.name = name
        self.avatar = avatar
        self.ws = None
        self.online = False
        self.offline_since = None
        self.joined_at = now_ms()

    def public(self):
        return {
            "clientId": self.client_id,
            "name": self.name,
            "avatar": self.avatar,
            "online": self.online,
        }


class Room:
    def __init__(self, code, host_id):
        self.code = code
        self.host_id = host_id
        self.members: dict[str, Member] = {}
        self.queue = []          # ortak jam sirasi
        self.host_queue = []     # host'un kendi YT Music sirasi (sadece gosterim)
        self.playback = None     # host'un son oynatma durumu
        self.playback_ts = 0     # sunucu saatine gore alinma zamani
        self.settings = {"guestControl": True, "guestQueue": True}
        self.empty_since = None

    def snapshot(self):
        return {
            "code": self.code,
            "hostId": self.host_id,
            "members": sorted(
                (m.public() for m in self.members.values()),
                key=lambda m: (m["clientId"] != self.host_id, m["name"].lower()),
            ),
            "settings": self.settings,
            "queue": self.queue,
            "hostQueue": self.host_queue,
        }

    def online_members(self):
        return [m for m in self.members.values() if m.online]


rooms: dict[str, Room] = {}
client_room: dict[str, str] = {}  # clientId -> oda kodu


def new_code() -> str:
    while True:
        code = "".join(random.choices(CODE_ALPHABET, k=6))
        if code not in rooms:
            return code


async def send(ws, payload):
    if ws is None:
        return
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except ConnectionClosed:
        pass


async def broadcast(room: Room, payload, exclude=None):
    msg = json.dumps(payload, ensure_ascii=False)
    tasks = []
    for m in room.members.values():
        if m.online and m.ws is not None and m.client_id != exclude:
            tasks.append(m.ws.send(msg))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def broadcast_room(room: Room):
    await broadcast(room, {"type": "room", "room": room.snapshot()})


def playback_payload(room: Room):
    return {
        "type": "playback",
        "playback": room.playback,
        "serverTs": room.playback_ts,
        "serverTime": now_ms(),
    }


def sanitize_track(item):
    if not isinstance(item, dict):
        return None
    video_id = clean_str(item.get("videoId"), 20)
    if not video_id or not all(c.isalnum() or c in "-_" for c in video_id):
        return None
    return {
        "videoId": video_id,
        "title": clean_str(item.get("title")) or video_id,
        "artist": clean_str(item.get("artist")),
        "thumb": clean_str(item.get("thumb"), 500),
        "duration": clean_str(item.get("duration"), 12),
    }


async def leave_room(client_id, notify=True):
    code = client_room.pop(client_id, None)
    if not code or code not in rooms:
        return
    room = rooms[code]
    member = room.members.pop(client_id, None)
    if notify and member and member.ws is not None:
        await send(member.ws, {"type": "left"})
    if not room.members:
        rooms.pop(code, None)
        return
    if room.host_id == client_id:
        promote_next_host(room)
    await broadcast_room(room)


def promote_next_host(room: Room):
    candidates = sorted(room.online_members(), key=lambda m: m.joined_at)
    if not candidates:
        candidates = sorted(room.members.values(), key=lambda m: m.joined_at)
    if candidates:
        room.host_id = candidates[0].client_id
        room.playback = None


async def handle_message(ws, ctx, msg):
    mtype = msg.get("type")
    client_id = ctx.get("clientId")

    if mtype == "hello":
        client_id = clean_str(msg.get("clientId"), 64) or str(uuid.uuid4())
        ctx["clientId"] = client_id
        ctx["name"] = clean_str(msg.get("name"), 40) or "Misafir"
        ctx["avatar"] = clean_str(msg.get("avatar"), 500)
        await send(ws, {"type": "welcome", "clientId": client_id, "serverTime": now_ms()})
        return

    if mtype == "ping":
        await send(ws, {"type": "pong", "t0": msg.get("t0"), "serverTime": now_ms()})
        return

    if not client_id:
        await send(ws, {"type": "error", "message": "Önce hello gönderilmeli."})
        return

    if mtype == "create":
        await leave_room(client_id, notify=False)
        code = new_code()
        room = Room(code, client_id)
        member = Member(client_id, ctx["name"], ctx["avatar"])
        member.ws, member.online = ws, True
        room.members[client_id] = member
        rooms[code] = room
        client_room[client_id] = code
        await broadcast_room(room)
        return

    if mtype == "join":
        code = clean_str(msg.get("code"), 10).upper()
        room = rooms.get(code)
        if not room:
            client_room.pop(client_id, None)
            await send(ws, {"type": "error", "code": "no_room", "message": "Bu kodla bir jam bulunamadı."})
            return
        if client_room.get(client_id) not in (None, code):
            await leave_room(client_id, notify=False)
        member = room.members.get(client_id)
        if member is None:
            if len(room.members) >= MAX_MEMBERS:
                await send(ws, {"type": "error", "message": "Jam dolu."})
                return
            member = Member(client_id, ctx["name"], ctx["avatar"])
            room.members[client_id] = member
        else:
            member.name, member.avatar = ctx["name"], ctx["avatar"]
        member.ws, member.online, member.offline_since = ws, True, None
        room.empty_since = None
        client_room[client_id] = code
        await broadcast_room(room)
        if room.playback:
            await send(ws, playback_payload(room))
        return

    # --- bundan sonrasi bir odada olmayi gerektirir ---
    code = client_room.get(client_id)
    room = rooms.get(code) if code else None
    if not room:
        await send(ws, {"type": "error", "code": "no_room", "message": "Bir jam'de değilsin."})
        return
    is_host = room.host_id == client_id

    if mtype == "leave":
        await leave_room(client_id)
        return

    if mtype == "state":
        if not is_host:
            return
        pb = msg.get("playback")
        if not isinstance(pb, dict):
            return
        room.playback = {
            "videoId": clean_str(pb.get("videoId"), 20),
            "title": clean_str(pb.get("title")),
            "artist": clean_str(pb.get("artist")),
            "thumb": clean_str(pb.get("thumb"), 500),
            "currentTime": float(pb.get("currentTime") or 0),
            "duration": float(pb.get("duration") or 0),
            "isPlaying": bool(pb.get("isPlaying")),
        }
        room.playback_ts = now_ms()
        await broadcast(room, playback_payload(room), exclude=client_id)
        return

    if mtype == "hostQueue":
        if not is_host:
            return
        items = msg.get("items") if isinstance(msg.get("items"), list) else []
        room.host_queue = [t for t in (sanitize_track(i) for i in items[:30]) if t]
        await broadcast_room(room)
        return

    if mtype == "command":
        action = clean_str(msg.get("action"), 20)
        if action not in ("play", "pause", "toggle", "next", "previous", "seek"):
            return
        if not is_host and not room.settings["guestControl"]:
            await send(ws, {"type": "error", "message": "Host, misafirlerin kontrolünü kapattı."})
            return
        host = room.members.get(room.host_id)
        if host and host.online:
            await send(host.ws, {
                "type": "command",
                "action": action,
                "value": msg.get("value"),
                "from": ctx["name"],
            })
        return

    if mtype == "queueAdd":
        if not is_host and not room.settings["guestQueue"]:
            await send(ws, {"type": "error", "message": "Host, misafirlerin sıraya eklemesini kapattı."})
            return
        track = sanitize_track(msg.get("item"))
        if not track:
            await send(ws, {"type": "error", "message": "Geçersiz şarkı."})
            return
        if len(room.queue) >= MAX_QUEUE:
            await send(ws, {"type": "error", "message": "Sıra dolu."})
            return
        track["id"] = uuid.uuid4().hex[:10]
        track["addedBy"] = ctx["name"]
        track["addedById"] = client_id
        room.queue.append(track)
        await broadcast_room(room)
        return

    if mtype in ("queueRemove", "queueMove", "queuePlayNow", "queueShift"):
        item_id = clean_str(msg.get("id"), 20)
        idx = next((i for i, t in enumerate(room.queue) if t["id"] == item_id), -1)
        if idx < 0:
            return
        item = room.queue[idx]

        if mtype == "queueRemove":
            if not (is_host or item["addedById"] == client_id):
                await send(ws, {"type": "error", "message": "Sadece kendi eklediğin şarkıyı kaldırabilirsin."})
                return
            room.queue.pop(idx)
        elif mtype == "queueMove":
            if not is_host:
                return
            new_idx = max(0, min(len(room.queue) - 1, idx + (-1 if msg.get("dir") == "up" else 1)))
            room.queue.insert(new_idx, room.queue.pop(idx))
        elif mtype == "queuePlayNow":
            if not is_host and not room.settings["guestControl"]:
                await send(ws, {"type": "error", "message": "Host, misafirlerin kontrolünü kapattı."})
                return
            # Ogeyi en basa al ve host'tan calmasini iste; host calinca queueShift gonderir.
            room.queue.insert(0, room.queue.pop(idx))
            host = room.members.get(room.host_id)
            if host and host.online:
                await send(host.ws, {"type": "command", "action": "playJam", "value": item["id"], "from": ctx["name"]})
        elif mtype == "queueShift":
            if not is_host:
                return
            room.queue.pop(idx)
        await broadcast_room(room)
        return

    if mtype == "settings":
        if not is_host:
            return
        for key in ("guestControl", "guestQueue"):
            if isinstance(msg.get(key), bool):
                room.settings[key] = msg[key]
        await broadcast_room(room)
        return

    if mtype == "transferHost":
        target = clean_str(msg.get("clientId"), 64)
        if is_host and target in room.members:
            room.host_id = target
            room.playback = None
            await broadcast_room(room)
        return

    if mtype == "kick":
        target = clean_str(msg.get("clientId"), 64)
        if is_host and target != client_id and target in room.members:
            await leave_room(target)
        return


async def handler(ws):
    ctx = {}
    try:
        async for raw in ws:
            if isinstance(raw, bytes) or len(raw) > 64_000:
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(msg, dict):
                await handle_message(ws, ctx, msg)
    except ConnectionClosed:
        pass
    finally:
        client_id = ctx.get("clientId")
        code = client_room.get(client_id) if client_id else None
        room = rooms.get(code) if code else None
        if room and client_id in room.members:
            member = room.members[client_id]
            if member.ws is ws:  # yeni bir baglanti devralmadiysa
                member.online, member.ws, member.offline_since = False, None, time.time()
                if not room.online_members():
                    room.empty_since = time.time()
                await broadcast_room(room)


async def janitor():
    """Kopan host'lari devreder, bos odalari temizler."""
    while True:
        await asyncio.sleep(5)
        t = time.time()
        for code, room in list(rooms.items()):
            if room.empty_since and t - room.empty_since > EMPTY_ROOM_TTL:
                for cid in list(room.members):
                    client_room.pop(cid, None)
                rooms.pop(code, None)
                continue
            host = room.members.get(room.host_id)
            if host and not host.online and host.offline_since and t - host.offline_since > HOST_GRACE_SECONDS:
                if room.online_members():
                    promote_next_host(room)
                    await broadcast_room(room)


def health(connection, request):
    # Normal HTTP istekleri (saglik kontrolu) icin basit yanit
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(HTTPStatus.OK, f"YT Music Jam sunucusu calisiyor. Aktif oda: {len(rooms)}\n")
    return None


async def main():
    asyncio.create_task(janitor())
    async with serve(handler, "0.0.0.0", PORT, process_request=health, ping_interval=20, ping_timeout=20) as server:
        print(f"YT Music Jam sunucusu ws://localhost:{PORT} adresinde calisiyor")
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
