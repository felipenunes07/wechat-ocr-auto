#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any


def stable_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def load_existing_map(map_path: Path) -> dict[str, str]:
    existing: dict[str, str] = {}
    if not map_path.exists():
        return existing
    try:
        data = json.loads(map_path.read_text(encoding="utf-8"))
    except Exception:
        return existing
    if not isinstance(data, dict):
        return existing
    for key, value in data.items():
        existing[str(key).strip().lower()] = str(value).strip()
    return existing


def discover_document_wechat_dirs() -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for home_raw in [os.environ.get("USERPROFILE"), str(Path.home())]:
        if not home_raw:
            continue
        home = Path(home_raw)
        for root in (
            home / "Documents" / "WeChat Files",
            home / "OneDrive" / "Documents" / "WeChat Files",
            home / "Documents" / "xwechat_files",
            home / "OneDrive" / "Documents" / "xwechat_files",
        ):
            key = str(root).lower()
            if key in seen:
                continue
            seen.add(key)
            if root.exists() and root.is_dir():
                roots.append(root)

    accounts: list[Path] = []
    for root in roots:
        try:
            children = list(root.iterdir())
        except OSError:
            continue
        for child in children:
            if not child.is_dir():
                continue
            if (child / "msg" / "attach").exists() or (child / "FileStorage" / "MsgAttach").exists():
                accounts.append(child)

    def safe_mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    accounts.sort(key=safe_mtime, reverse=True)
    return accounts


def collect_msgattach_hash_folders(wx_dirs: list[Path]) -> set[str]:
    folder_hashes: set[str] = set()
    for wx_dir in wx_dirs:
        msgattach_dir = wx_dir / "msg" / "attach"
        if not msgattach_dir.exists():
            msgattach_dir = wx_dir / "FileStorage" / "MsgAttach"
        if not msgattach_dir.exists() or not msgattach_dir.is_dir():
            continue
        try:
            entries = list(msgattach_dir.iterdir())
        except OSError:
            continue
        for entry in entries:
            if entry.is_dir():
                folder_hashes.add(entry.name.strip().lower())
    return folder_hashes


def pick_pywxdump_target(infos: list[dict[str, Any]], discovered_dirs: list[Path]) -> tuple[Path, str]:
    preferred_dirs = {str(path).lower(): path for path in discovered_dirs}
    for info in infos:
        wx_dir_raw = str(info.get("wx_dir") or "").strip()
        key = str(info.get("key") or "").strip()
        if not wx_dir_raw or not key:
            continue
        wx_dir = Path(wx_dir_raw)
        if not wx_dir.exists():
            continue
        preferred = preferred_dirs.get(str(wx_dir).lower())
        return (preferred or wx_dir, key)
    raise RuntimeError("wx_info_missing_usable_key_or_dir")


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def fetch_latest_session_names(conn: sqlite3.Connection) -> dict[str, str]:
    if not table_exists(conn, "Session"):
        return {}
    rows = conn.execute(
        """
        SELECT S.strUsrName, COALESCE(S.strNickName, '') AS strNickName, COALESCE(S.nTime, 0) AS nTime
        FROM Session AS S
        INNER JOIN (
            SELECT strUsrName, MAX(nTime) AS MaxTime
            FROM Session
            GROUP BY strUsrName
        ) AS Latest
          ON Latest.strUsrName = S.strUsrName
         AND Latest.MaxTime = S.nTime
        WHERE COALESCE(S.strUsrName, '') <> ''
        ORDER BY S.nTime DESC, S.strUsrName ASC
        """
    ).fetchall()
    out: dict[str, str] = {}
    for row in rows:
        username = str(row["strUsrName"] or "").strip()
        session_name = str(row["strNickName"] or "").strip()
        if username and username not in out and session_name:
            out[username] = session_name
    return out


def fetch_contacts(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    if not table_exists(conn, "Contact"):
        return {}
    rows = conn.execute(
        """
        SELECT UserName, COALESCE(NickName, '') AS NickName, COALESCE(Remark, '') AS Remark, COALESCE(Alias, '') AS Alias
        FROM Contact
        WHERE COALESCE(UserName, '') <> ''
        """
    ).fetchall()
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        username = str(row["UserName"] or "").strip()
        if not username:
            continue
        out[username] = {
            "nickname": str(row["NickName"] or "").strip(),
            "remark": str(row["Remark"] or "").strip(),
            "alias": str(row["Alias"] or "").strip(),
        }
    return out


def fetch_chatroom_usernames(conn: sqlite3.Connection) -> set[str]:
    usernames: set[str] = set()
    if table_exists(conn, "ChatRoom"):
        rows = conn.execute(
            "SELECT ChatRoomName FROM ChatRoom WHERE COALESCE(ChatRoomName, '') <> ''"
        ).fetchall()
        usernames.update(str(row["ChatRoomName"] or "").strip() for row in rows if str(row["ChatRoomName"] or "").strip())
    if table_exists(conn, "Contact"):
        rows = conn.execute(
            "SELECT UserName FROM Contact WHERE UserName LIKE '%@chatroom'"
        ).fetchall()
        usernames.update(str(row["UserName"] or "").strip() for row in rows if str(row["UserName"] or "").strip())
    if table_exists(conn, "Session"):
        rows = conn.execute(
            "SELECT DISTINCT strUsrName FROM Session WHERE strUsrName LIKE '%@chatroom'"
        ).fetchall()
        usernames.update(str(row["strUsrName"] or "").strip() for row in rows if str(row["strUsrName"] or "").strip())
    return usernames


def pick_group_name(
    username: str,
    session_name: str | None,
    nickname: str | None,
    remark: str | None,
    alias: str | None,
) -> str:
    for value in (remark, session_name, nickname, alias):
        text = str(value or "").strip()
        if text:
            return text
    return username


def build_auto_map_from_merged_db(conn: sqlite3.Connection) -> tuple[dict[str, str], set[str]]:
    contacts = fetch_contacts(conn)
    session_names = fetch_latest_session_names(conn)
    chatroom_usernames = fetch_chatroom_usernames(conn)

    if not chatroom_usernames:
        raise RuntimeError("merged_db_missing_chatrooms")

    auto_map: dict[str, str] = {}
    for username in sorted(chatroom_usernames):
        contact = contacts.get(username, {})
        auto_map[stable_hash(username).lower()] = pick_group_name(
            username=username,
            session_name=session_names.get(username),
            nickname=contact.get("nickname"),
            remark=contact.get("remark"),
            alias=contact.get("alias"),
        )

    non_group_usernames = set(contacts.keys()) | set(session_names.keys())
    non_group_hashes = {
        stable_hash(username).lower()
        for username in non_group_usernames
        if username and username not in chatroom_usernames
    }
    return auto_map, non_group_hashes


def ensure_merged_db(pywxdump: Any, wx_dir: Path, key: str, merge_path: Path) -> Path:
    merge_path.parent.mkdir(parents=True, exist_ok=True)
    ok, result = pywxdump.all_merge_real_time_db(key=key, wx_path=str(wx_dir), merge_path=str(merge_path))
    if not ok:
        raise RuntimeError(f"merge_failed:{result}")
    if not merge_path.exists():
        raise RuntimeError(f"merge_missing:{merge_path}")
    return merge_path


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    map_path = base_dir / "clientes_grupos.json"
    merge_path = base_dir / ".runtime" / "wechat_merge.db"

    try:
        import pywxdump  # type: ignore
    except Exception as exc:
        print(f"ERROR: pywxdump_unavailable:{type(exc).__name__}:{exc}")
        return 1

    logging.getLogger("wx_core").setLevel(logging.ERROR)

    try:
        infos = pywxdump.get_wx_info(is_print=False) or []
    except Exception as exc:
        print(f"ERROR: wx_info_failed:{type(exc).__name__}:{exc}")
        return 1
    if not infos:
        print("ERROR: wx_info_empty")
        return 1

    discovered_dirs = discover_document_wechat_dirs()
    try:
        wx_dir, key = pick_pywxdump_target(infos, discovered_dirs)
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}:{exc}")
        return 1

    try:
        ensure_merged_db(pywxdump=pywxdump, wx_dir=wx_dir, key=key, merge_path=merge_path)
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}:{exc}")
        return 1

    conn = sqlite3.connect(str(merge_path))
    conn.row_factory = sqlite3.Row
    try:
        auto_map, non_group_hashes = build_auto_map_from_merged_db(conn)
    except Exception as exc:
        conn.close()
        print(f"ERROR: {type(exc).__name__}:{exc}")
        return 1
    conn.close()

    if not auto_map:
        print("ERROR: merged_db_without_group_names")
        return 1

    existing = load_existing_map(map_path)
    folder_hashes = collect_msgattach_hash_folders([wx_dir] + discovered_dirs)

    changed = 0
    for group_hash, group_name in auto_map.items():
        current = existing.get(group_hash, "")
        if not current:
            existing[group_hash] = group_name
            changed += 1

    for group_hash in sorted(folder_hashes):
        if group_hash in non_group_hashes:
            continue
        existing.setdefault(group_hash, "")

    existing.pop("cole_aqui_id_do_grupo", None)
    for group_hash in non_group_hashes:
        if not existing.get(group_hash, "").strip():
            existing.pop(group_hash, None)

    map_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

    filled = sum(1 for value in existing.values() if str(value).strip())
    unresolved = sum(1 for value in existing.values() if not str(value).strip())
    print(
        "OK: mapa atualizado | "
        f"modo=merge-db | total_hash={len(existing)} | preenchidos={filled} | "
        f"novos={changed} | pendentes={unresolved} | merge={merge_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
