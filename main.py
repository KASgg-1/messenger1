import random
import sqlite3
import time
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

BASE = Path(__file__).parent
DB = BASE / "messenger.db"
app = FastAPI()

# Активные WebSocket-подключения: user_id -> набор сокетов
connections: dict[int, set[WebSocket]] = {}


def run(sql, args=()):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, args)
        conn.commit()
        return cur.fetchall(), cur.lastrowid
    finally:
        conn.close()


run("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
run("CREATE TABLE IF NOT EXISTS groups (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
run("CREATE TABLE IF NOT EXISTS group_members (group_id INTEGER, user_id INTEGER, "
    "PRIMARY KEY (group_id, user_id))")
run("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, chat TEXT, "
    "sender_id INTEGER, sender_name TEXT, text TEXT, created REAL)")


class NameIn(BaseModel):
    name: str


class LoginIn(BaseModel):
    id: int
    name: str


class GroupIn(BaseModel):
    name: str
    members: list[int]


class MsgIn(BaseModel):
    chat: str
    text: str


def get_me(x_user_id: str | None) -> int:
    if not x_user_id or not x_user_id.isdigit():
        raise HTTPException(401, "Нужно войти в аккаунт")
    rows, _ = run("SELECT id FROM users WHERE id = ?", (int(x_user_id),))
    if not rows:
        raise HTTPException(401, "Пользователь не найден")
    return int(x_user_id)


def members_of(chat: str) -> list[int]:
    """Возвращает ID участников чата: 'd:a:b' для личного, 'g:id' для группы."""
    try:
        kind, *rest = chat.split(":")
        if kind == "d" and len(rest) == 2:
            return [int(x) for x in rest]
        if kind == "g" and len(rest) == 1:
            rows, _ = run("SELECT user_id FROM group_members WHERE group_id = ?", (int(rest[0]),))
            return [r["user_id"] for r in rows]
    except ValueError:
        pass
    raise HTTPException(400, "Некорректный чат")


async def notify(uids, payload):
    for uid in uids:
        for ws in list(connections.get(uid, set())):
            try:
                await ws.send_json(payload)
            except Exception:
                connections.get(uid, set()).discard(ws)


@app.get("/")
def index():
    return FileResponse(BASE / "static" / "index.html")


@app.post("/api/register")
def register(data: NameIn):
    name = data.name.strip()[:30]
    if not name:
        raise HTTPException(400, "Введите имя")
    while True:
        uid = random.randint(100000, 999999)
        rows, _ = run("SELECT 1 FROM users WHERE id = ?", (uid,))
        if not rows:
            break
    run("INSERT INTO users (id, name) VALUES (?, ?)", (uid, name))
    return {"id": uid, "name": name}


@app.post("/api/login")
def login(data: LoginIn):
    rows, _ = run("SELECT id, name FROM users WHERE id = ? AND name = ?",
                  (data.id, data.name.strip()))
    if not rows:
        raise HTTPException(401, "Неверный ID или имя")
    return {"id": rows[0]["id"], "name": rows[0]["name"]}


@app.get("/api/user/{uid}")
def user_info(uid: int):
    rows, _ = run("SELECT id, name FROM users WHERE id = ?", (uid,))
    if not rows:
        raise HTTPException(404, "Пользователь с таким ID не найден")
    return {"id": rows[0]["id"], "name": rows[0]["name"]}


@app.post("/api/groups")
async def create_group(data: GroupIn, x_user_id: str | None = Header(None)):
    me = get_me(x_user_id)
    name = data.name.strip()[:40]
    if not name:
        raise HTTPException(400, "Введите название группы")
    _, gid = run("INSERT INTO groups (name) VALUES (?)", (name,))
    ids = set(data.members) | {me}
    existing = []
    for uid in ids:
        rows, _ = run("SELECT 1 FROM users WHERE id = ?", (uid,))
        if rows:
            run("INSERT OR IGNORE INTO group_members (group_id, user_id) VALUES (?, ?)", (gid, uid))
            existing.append(uid)
    await notify(existing, {"type": "refresh"})
    return {"id": gid, "name": name}


@app.get("/api/chats")
def chats(x_user_id: str | None = Header(None)):
    me = get_me(x_user_id)
    result = []
    rows, _ = run("SELECT g.id, g.name FROM groups g JOIN group_members m ON m.group_id = g.id "
                  "WHERE m.user_id = ?", (me,))
    result += [{"chat": f"g:{r['id']}", "title": r["name"], "type": "group"} for r in rows]

    rows, _ = run("SELECT DISTINCT chat FROM messages WHERE chat LIKE 'd:%'")
    for r in rows:
        ids = [int(x) for x in r["chat"].split(":")[1:]]
        if me in ids:
            other = ids[1] if ids[0] == me else ids[0]
            u, _ = run("SELECT name FROM users WHERE id = ?", (other,))
            title = u[0]["name"] if u else "Пользователь"
            result.append({"chat": r["chat"], "title": f"{title} (ID {other})", "type": "direct"})
    return result


@app.get("/api/messages")
def get_messages(chat: str, x_user_id: str | None = Header(None)):
    me = get_me(x_user_id)
    if me not in members_of(chat):
        raise HTTPException(403, "Нет доступа к этому чату")
    rows, _ = run("SELECT id, chat, sender_id, sender_name, text, created FROM messages "
                  "WHERE chat = ? ORDER BY id DESC LIMIT 200", (chat,))
    return [dict(r) for r in reversed(rows)]


@app.post("/api/messages")
async def send_message(data: MsgIn, x_user_id: str | None = Header(None)):
    me = get_me(x_user_id)
    ids = members_of(data.chat)
    if me not in ids:
        raise HTTPException(403, "Нет доступа к этому чату")
    if data.chat.startswith("d:"):
        for uid in ids:
            rows, _ = run("SELECT 1 FROM users WHERE id = ?", (uid,))
            if not rows:
                raise HTTPException(404, "Собеседник с таким ID не найден")
    text = data.text.strip()[:2000]
    if not text:
        raise HTTPException(400, "Пустое сообщение")

    rows, _ = run("SELECT name FROM users WHERE id = ?", (me,))
    name = rows[0]["name"]
    created = time.time()
    _, mid = run("INSERT INTO messages (chat, sender_id, sender_name, text, created) "
                 "VALUES (?, ?, ?, ?, ?)", (data.chat, me, name, text, created))
    msg = {"id": mid, "chat": data.chat, "sender_id": me, "sender_name": name,
           "text": text, "created": created}
    await notify(set(ids), {"type": "message", "message": msg})
    return msg


@app.websocket("/ws/{uid}")
async def websocket_endpoint(websocket: WebSocket, uid: int):
    await websocket.accept()
    connections.setdefault(uid, set()).add(websocket)
    try:
        while True:
            await websocket.receive_text()  # держим соединение открытым
    except WebSocketDisconnect:
        pass
    finally:
        connections.get(uid, set()).discard(websocket)
