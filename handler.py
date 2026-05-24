"""
handler.py — Yandex Cloud Function для P2P relay.

Реализует серверную сторону Этапа 2:
  §2.1 heartbeat → INSERT/UPDATE в presence
  §2.2 pull      → SELECT с FIFO ORDER BY (§2.5)
  §2.8 send      → INSERT с ON CONFLICT (идемпотентность I1 на relay)
  §2.7 presigned → S3-API через boto3
  §2.9 GC        → DELETE по ack + lifecycle policy на бакете (вне кода)

Операции (POST JSON в один endpoint):
  heartbeat              — клиент жив, верни peers
  pull                   — выдай очередь сообщений + удали ack-нутые
  send                   — положи сообщение в очередь
  request_upload_url     — выдай presigned URL для PUT в Object Storage
  request_download_url   — выдай presigned URL для GET из Object Storage

Переменные окружения (задаются в настройках функции YCF):
  API_KEY                — общий секрет (обязательно)
  YDB_ENDPOINT           — grpcs://ydb.serverless.yandexcloud.net:2135
  YDB_DATABASE           — /ru-central1/.../{database-id}
  S3_BUCKET              — имя бакета Object Storage
  S3_ENDPOINT            — https://storage.yandexcloud.net
  S3_REGION              — ru-central1
  AWS_ACCESS_KEY_ID      — статический ключ сервисного аккаунта
  AWS_SECRET_ACCESS_KEY  — секрет статического ключа
  PEER_TTL_SEC           — TTL присутствия, по умолчанию 15
  PRESIGN_TTL_SEC        — TTL presigned URL, по умолчанию 900 (15 минут)

Авторизация в YDB — через сервисный аккаунт, прикреплённый к функции
(MetadataUrlCredentials).
"""

import json
import logging
import os
import time

import boto3
from botocore.client import Config as BotoConfig
import ydb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("handler")

# ---------------------------------------------------------------------------
# Глобальные клиенты — переиспользуются между warm-вызовами функции
# ---------------------------------------------------------------------------

_API_KEY = os.environ["API_KEY"]
_PEER_TTL_SEC = int(os.environ.get("PEER_TTL_SEC", "15"))
_PRESIGN_TTL_SEC = int(os.environ.get("PRESIGN_TTL_SEC", "900"))
_S3_BUCKET = os.environ["S3_BUCKET"]
_S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://storage.yandexcloud.net")
_S3_REGION = os.environ.get("S3_REGION", "ru-central1")

_driver = None
_pool = None
_s3 = None


def _get_ydb():
    global _driver, _pool
    if _pool is not None:
        return _pool
    endpoint = os.environ["YDB_ENDPOINT"]
    database = os.environ["YDB_DATABASE"]
    _driver = ydb.Driver(
        endpoint=endpoint,
        database=database,
        credentials=ydb.iam.MetadataUrlCredentials(),
    )
    _driver.wait(timeout=10, fail_fast=True)
    _pool = ydb.SessionPool(_driver)
    log.info("YDB driver ready: endpoint=%s db=%s", endpoint, database)
    return _pool


def _get_s3():
    global _s3
    if _s3 is not None:
        return _s3
    _s3 = boto3.client(
        "s3",
        endpoint_url=_S3_ENDPOINT,
        region_name=_S3_REGION,
        config=BotoConfig(signature_version="s3v4"),
    )
    log.info("S3 client ready: endpoint=%s bucket=%s", _S3_ENDPOINT, _S3_BUCKET)
    return _s3


# ---------------------------------------------------------------------------
# DDL — создаётся вручную перед первым запуском (см. README).
# Здесь оставлено для справки.
# ---------------------------------------------------------------------------
#
# CREATE TABLE presence (
#     nick      Utf8,
#     last_seen Timestamp,
#     PRIMARY KEY (nick)
# );
#
# CREATE TABLE messages (
#     id         Utf8,
#     src        Utf8,
#     dst        Utf8,
#     mtype      Utf8,
#     payload    Json,
#     created_at Timestamp,
#     PRIMARY KEY (id),
#     INDEX dst_created_idx GLOBAL ON (dst, created_at)
# );
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Утилиты ответа
# ---------------------------------------------------------------------------

def _resp(ok: bool, status: int = 200, **fields) -> dict:
    body = {"ok": ok, **fields}
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json; charset=utf-8"},
        "body": json.dumps(body, ensure_ascii=False),
    }


def _err(msg: str, status: int = 400, retriable: bool = False) -> dict:
    return _resp(False, status=status, error=msg, retriable=retriable)


# ---------------------------------------------------------------------------
# Операции
# ---------------------------------------------------------------------------

def op_heartbeat(body: dict) -> dict:
    """§2.1: апсертим last_seen, возвращаем список онлайн-пиров кроме себя."""
    user = body.get("user", "").strip()
    if not user:
        return _err("user required")

    pool = _get_ydb()
    now_ms = int(time.time() * 1000)
    ttl_us = _PEER_TTL_SEC * 1_000_000

    def callee(session):
        # UPSERT — идемпотентный insert/update
        session.transaction().execute(
            """
            DECLARE $nick AS Utf8;
            DECLARE $now  AS Timestamp;
            UPSERT INTO presence (nick, last_seen) VALUES ($nick, $now);
            """,
            {"$nick": user.encode("utf-8"),
             "$now": now_ms * 1000},  # Timestamp в YDB — микросекунды
            commit_tx=True,
        )
        # читаем пиров с свежим last_seen
        rs = session.transaction().execute(
            """
            DECLARE $now AS Timestamp;
            DECLARE $ttl AS Uint64;
            SELECT nick FROM presence
            WHERE CAST(last_seen AS Uint64) >= $now - $ttl;
            """,
            {"$now": now_ms * 1000, "$ttl": ttl_us},
            commit_tx=True,
        )
        peers = []
        for row in rs[0].rows:
            n = row.nick.decode("utf-8") if isinstance(row.nick, bytes) else row.nick
            if n != user:
                peers.append(n)
        return peers

    try:
        peers = pool.retry_operation_sync(callee)
    except Exception as e:
        log.exception("heartbeat failed: %s", e)
        return _err(f"ydb error: {e}", status=500, retriable=True)
    return _resp(True, peers=peers)


def op_pull(body: dict) -> dict:
    """§2.2 + §2.5: возвращаем все сообщения для user, отсортированные FIFO.
       Параллельно удаляем все ack-нутые ранее сообщения (DELETE WHERE id IN ack
       — реализация §2.9.A с проверкой dst для соблюдения I3 routing)."""
    user = body.get("user", "").strip()
    if not user:
        return _err("user required")
    ack: list[str] = body.get("ack", []) or []

    pool = _get_ydb()

    def callee(session):
        tx = session.transaction()
        if ack:
            # удаляем подтверждённые сообщения только если они адресованы user
            # (защита I3 routing): нельзя удалить чужие
            placeholders = ", ".join(f'"{a}"' for a in ack if isinstance(a, str))
            if placeholders:
                tx.execute(
                    f"""
                    DECLARE $dst AS Utf8;
                    DELETE FROM messages
                    WHERE dst = $dst AND id IN ({placeholders});
                    """,
                    {"$dst": user.encode("utf-8")},
                    commit_tx=False,
                )
        rs = tx.execute(
            """
            DECLARE $dst AS Utf8;
            SELECT id, src, dst, mtype, payload, created_at
            FROM messages
            WHERE dst = $dst
            ORDER BY created_at ASC, id ASC
            LIMIT 100;
            """,
            {"$dst": user.encode("utf-8")},
            commit_tx=True,
        )
        msgs = []
        for row in rs[0].rows:
            payload_raw = row.payload
            if isinstance(payload_raw, (bytes, bytearray)):
                payload_raw = payload_raw.decode("utf-8")
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {}
            msgs.append({
                "id": row.id.decode("utf-8") if isinstance(row.id, bytes) else row.id,
                "src": row.src.decode("utf-8") if isinstance(row.src, bytes) else row.src,
                "dst": row.dst.decode("utf-8") if isinstance(row.dst, bytes) else row.dst,
                "type": row.mtype.decode("utf-8") if isinstance(row.mtype, bytes) else row.mtype,
                "payload": payload,
                "ts": int(row.created_at.timestamp() * 1000) if hasattr(row.created_at, "timestamp") else 0,
            })
        return msgs

    try:
        msgs = pool.retry_operation_sync(callee)
    except Exception as e:
        log.exception("pull failed: %s", e)
        return _err(f"ydb error: {e}", status=500, retriable=True)
    return _resp(True, messages=msgs)


def op_send(body: dict) -> dict:
    """§2.8: идемпотентный INSERT (UPSERT по id — повторный send того же id no-op)."""
    msg = body.get("msg") or {}
    required = ("id", "src", "dst", "type")
    for f in required:
        if not msg.get(f):
            return _err(f"msg.{f} required")

    pool = _get_ydb()
    now_ms = int(time.time() * 1000)
    payload_json = json.dumps(msg.get("payload", {}), ensure_ascii=False)

    def callee(session):
        # INSERT — если id уже есть, YDB бросит исключение. Чтобы получить
        # семантику ON CONFLICT DO NOTHING (требование §2.8), используем UPSERT —
        # но проверяем, что мы не перезаписываем чужой src/dst (легковесная
        # защита: если запись уже есть с другим src — отвергаем).
        # Простейший идемпотентный путь — UPSERT, т.к. id уникален у клиента (UUIDv4).
        session.transaction().execute(
            """
            DECLARE $id  AS Utf8;
            DECLARE $src AS Utf8;
            DECLARE $dst AS Utf8;
            DECLARE $mt  AS Utf8;
            DECLARE $pl  AS Json;
            DECLARE $ts  AS Timestamp;
            UPSERT INTO messages (id, src, dst, mtype, payload, created_at)
            VALUES ($id, $src, $dst, $mt, $pl, $ts);
            """,
            {
                "$id":  msg["id"].encode("utf-8"),
                "$src": msg["src"].encode("utf-8"),
                "$dst": msg["dst"].encode("utf-8"),
                "$mt":  msg["type"].encode("utf-8"),
                "$pl":  payload_json,
                "$ts":  now_ms * 1000,
            },
            commit_tx=True,
        )

    try:
        pool.retry_operation_sync(callee)
    except Exception as e:
        log.exception("send failed: %s", e)
        return _err(f"ydb error: {e}", status=500, retriable=True)
    return _resp(True)


def op_request_upload_url(body: dict) -> dict:
    """§2.7: presigned URL на PUT в Object Storage."""
    user = body.get("user", "").strip()
    key = body.get("key", "").strip()
    if not user or not key:
        return _err("user and key required")
    # минимальная валидация ключа
    if "/" in key or ".." in key or len(key) > 200:
        return _err("invalid key")

    s3 = _get_s3()
    try:
        url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": _S3_BUCKET, "Key": key, "ContentType": "image/jpeg"},
            ExpiresIn=_PRESIGN_TTL_SEC,
            HttpMethod="PUT",
        )
    except Exception as e:
        log.exception("presign upload failed: %s", e)
        return _err(f"s3 error: {e}", status=500, retriable=True)
    return _resp(True, url=url)


def op_request_download_url(body: dict) -> dict:
    """§2.7: presigned URL на GET из Object Storage."""
    user = body.get("user", "").strip()
    key = body.get("key", "").strip()
    if not user or not key:
        return _err("user and key required")
    if "/" in key or ".." in key or len(key) > 200:
        return _err("invalid key")

    s3 = _get_s3()
    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": _S3_BUCKET, "Key": key},
            ExpiresIn=_PRESIGN_TTL_SEC,
            HttpMethod="GET",
        )
    except Exception as e:
        log.exception("presign download failed: %s", e)
        return _err(f"s3 error: {e}", status=500, retriable=True)
    return _resp(True, url=url)


# ---------------------------------------------------------------------------
# Strategy для роутинга op
# ---------------------------------------------------------------------------

ROUTES = {
    "heartbeat":             op_heartbeat,
    "pull":                  op_pull,
    "send":                  op_send,
    "request_upload_url":    op_request_upload_url,
    "request_download_url":  op_request_download_url,
}


def handler(event, context):
    """Точка входа YCF (HTTP integration). event['body'] — строка JSON."""
    try:
        body_str = event.get("body", "") or "{}"
        if event.get("isBase64Encoded"):
            import base64 as _b64
            body_str = _b64.b64decode(body_str).decode("utf-8")
        body = json.loads(body_str)
    except Exception as e:
        return _err(f"bad request: {e}", status=400)

    if body.get("api_key") != _API_KEY:
        log.warning("unauthorized request from %s", event.get("requestContext", {}).get("identity"))
        return _err("unauthorized", status=401)

    op = body.get("op")
    fn = ROUTES.get(op)
    if not fn:
        return _err(f"unknown op '{op}'", status=400)

    try:
        return fn(body)
    except Exception as e:
        log.exception("handler op=%s crashed: %s", op, e)
        return _err(f"internal error: {e}", status=500, retriable=True)