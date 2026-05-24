"""
handler.py — Yandex Cloud Function для P2P relay.

ВЕРСИЯ 4 (ФИНАЛЬНАЯ): использует session.prepare() для всех запросов.
Это единственный правильный способ в Table API передать параметры —
prepare() заполняет parameters_types из DECLARE, без него SDK
молча отбрасывает параметры (см. ydb/convert.py::parameters_to_pb).
"""

import datetime as _dt
import json
import logging
import os
import time

import boto3
from botocore.client import Config as BotoConfig
import ydb

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("handler")

# ---------------------------------------------------------------------------
# Глобальные клиенты
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
# YQL-запросы (с DECLARE — это критично для session.prepare())
# ---------------------------------------------------------------------------

Q_UPSERT_PRESENCE = """
DECLARE $nick AS Utf8;
DECLARE $now  AS Timestamp;
UPSERT INTO presence (nick, last_seen) VALUES ($nick, $now);
"""

Q_SELECT_PEERS = """
DECLARE $threshold AS Timestamp;
SELECT nick FROM presence WHERE last_seen >= $threshold;
"""

Q_DELETE_ACKED = """
DECLARE $dst AS Utf8;
DECLARE $ids AS List<Utf8>;
DELETE FROM messages WHERE dst = $dst AND id IN $ids;
"""

Q_SELECT_INBOX = """
DECLARE $dst AS Utf8;
SELECT id, src, dst, mtype, payload, created_at
FROM messages
WHERE dst = $dst
ORDER BY created_at ASC, id ASC
LIMIT 100;
"""

Q_UPSERT_MESSAGE = """
DECLARE $id  AS Utf8;
DECLARE $src AS Utf8;
DECLARE $dst AS Utf8;
DECLARE $mt  AS Utf8;
DECLARE $pl  AS Json;
DECLARE $ts  AS Timestamp;
UPSERT INTO messages (id, src, dst, mtype, payload, created_at)
VALUES ($id, $src, $dst, $mt, $pl, $ts);
"""


# ---------------------------------------------------------------------------
# Утилиты
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


def _now_us() -> int:
    """Текущий момент в МИКРОсекундах от epoch — формат YDB Timestamp."""
    return int(time.time() * 1_000_000)


def _ydb_ts_to_ms(v) -> int:
    if isinstance(v, _dt.datetime):
        return int(v.replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
    if isinstance(v, (int, float)):
        return int(v) // 1000
    return 0


def _to_str(v):
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v


# ---------------------------------------------------------------------------
# Операции
# ---------------------------------------------------------------------------

def op_heartbeat(body: dict) -> dict:
    user = body.get("user", "").strip()
    if not user:
        return _err("user required")

    pool = _get_ydb()
    now_us = _now_us()
    threshold_us = now_us - _PEER_TTL_SEC * 1_000_000

    def callee(session):
        # prepare() заполняет parameters_types из DECLARE-инструкций
        upsert_q = session.prepare(Q_UPSERT_PRESENCE)
        select_q = session.prepare(Q_SELECT_PEERS)

        session.transaction().execute(
            upsert_q,
            {"$nick": user, "$now": now_us},
            commit_tx=True,
        )
        rs = session.transaction().execute(
            select_q,
            {"$threshold": threshold_us},
            commit_tx=True,
        )
        peers = []
        for row in rs[0].rows:
            n = _to_str(row.nick)
            if n and n != user:
                peers.append(n)
        return peers

    try:
        peers = pool.retry_operation_sync(callee)
    except Exception as e:
        log.exception("heartbeat failed: %s", e)
        return _err(f"ydb error: {e}", status=500, retriable=True)
    return _resp(True, peers=peers)


def op_pull(body: dict) -> dict:
    user = body.get("user", "").strip()
    if not user:
        return _err("user required")

    ack_raw = body.get("ack", []) or []
    ack = [a for a in ack_raw if isinstance(a, str) and a]

    pool = _get_ydb()

    def callee(session):
        if ack:
            delete_q = session.prepare(Q_DELETE_ACKED)
            session.transaction().execute(
                delete_q,
                {"$dst": user, "$ids": ack},
                commit_tx=True,
            )

        select_q = session.prepare(Q_SELECT_INBOX)
        rs = session.transaction().execute(
            select_q,
            {"$dst": user},
            commit_tx=True,
        )

        out = []
        for row in rs[0].rows:
            payload_raw = row.payload
            if isinstance(payload_raw, (bytes, bytearray)):
                payload_raw = payload_raw.decode("utf-8")
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {}
            out.append({
                "id":      _to_str(row.id),
                "src":     _to_str(row.src),
                "dst":     _to_str(row.dst),
                "type":    _to_str(row.mtype),
                "payload": payload,
                "ts":      _ydb_ts_to_ms(row.created_at),
            })
        return out

    try:
        msgs = pool.retry_operation_sync(callee)
    except Exception as e:
        log.exception("pull failed: %s", e)
        return _err(f"ydb error: {e}", status=500, retriable=True)
    return _resp(True, messages=msgs)


def op_send(body: dict) -> dict:
    msg = body.get("msg") or {}
    for f in ("id", "src", "dst", "type"):
        if not msg.get(f):
            return _err(f"msg.{f} required")

    pool = _get_ydb()
    now_us = _now_us()
    payload_json = json.dumps(msg.get("payload", {}), ensure_ascii=False)

    def callee(session):
        upsert_q = session.prepare(Q_UPSERT_MESSAGE)
        session.transaction().execute(
            upsert_q,
            {
                "$id":  msg["id"],
                "$src": msg["src"],
                "$dst": msg["dst"],
                "$mt":  msg["type"],
                "$pl":  payload_json,
                "$ts":  now_us,
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
    user = body.get("user", "").strip()
    key = body.get("key", "").strip()
    if not user or not key:
        return _err("user and key required")
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


ROUTES = {
    "heartbeat":             op_heartbeat,
    "pull":                  op_pull,
    "send":                  op_send,
    "request_upload_url":    op_request_upload_url,
    "request_download_url":  op_request_download_url,
}


def handler(event, context):
    try:
        body_str = event.get("body", "") or "{}"
        if event.get("isBase64Encoded"):
            import base64 as _b64
            body_str = _b64.b64decode(body_str).decode("utf-8")
        body = json.loads(body_str)
    except Exception as e:
        return _err(f"bad request: {e}", status=400)

    if body.get("api_key") != _API_KEY:
        log.warning("unauthorized request")
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