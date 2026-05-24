#!/usr/bin/env python3
"""
peer.py — кроссплатформенный P2P-клиент (Windows / macOS) для обмена
скриншотами и сообщениями через Yandex Cloud Function + YDB + Object Storage.

Архитектура жёстко соответствует Этапам 1-3 проектирования:
  §1.6 FSM Mealy machine     → Coordinator + TRANSITIONS-таблица
  §1.7 CSP-композиция        → asyncio.Queue + producer-корутины
  §1.8 двухплоскостность     → RelayClient (control) и BlobClient (data)
  §2.1 heartbeat             → HeartbeatProducer
  §2.2 short polling         → PollerProducer
  §2.3 dedup at-least-once   → DedupCache (LRU+TTL)
  §2.4 retry + jitter        → ResilientCaller (decorrelated jitter, AWS)
  §2.6 producer-consumer     → events-queue + Coordinator
  §2.7 pre-signed URL        → UploadCmd flow
  Strategy + Factory         → PlatformFactory для Win/Mac

Запуск: python peer.py
Конфиг: через переменные окружения (см. README) или интерактивный ввод.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import platform
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Protocol

# ============================================================================
# §A. КОНФИГУРАЦИЯ И КОНСТАНТЫ
# ============================================================================

# Параметры из Этапа 1-2 (математически обоснованные)
TAU_HEARTBEAT: float = 5.0       # τ — интервал heartbeat (§2.1)
POLL_INTERVAL: float = 2.0       # p — интервал polling (§2.2, §1.5)
RETRY_BASE: float = 0.5          # base — стартовая задержка retry (§2.4)
RETRY_CAP: float = 30.0          # cap — потолок задержки retry (§2.4)
RETRY_MAX_ATTEMPTS: int = 5      # ограничение для соблюдения I2 (liveness)
JPEG_QUALITY: int = 85           # сжатие скриншота (согласовано на Этапе 0)
DEDUP_CACHE_SIZE: int = 10_000   # размер LRU-кэша дедупликации (§2.3)
DEDUP_CACHE_TTL: int = 86_400    # TTL дедупликации, 24 часа (§2.3)
PEER_TTL_SEC: float = 15.0       # Θ — TTL присутствия (§1.4, отображается клиенту)
HTTP_TIMEOUT: float = 15.0       # таймаут одного HTTP-запроса


@dataclass
class Config:
    relay_url: str
    api_key: str
    nick: str
    hotkey: tuple[str, ...]  # например ("9", "1")
    log_level: str = "INFO"

    @classmethod
    def from_env_and_prompt(cls) -> "Config":
        relay_url = os.environ.get("RELAY_URL", "").strip()
        api_key = os.environ.get("API_KEY", "").strip()
        nick = os.environ.get("NICK", "").strip()
        hotkey_str = os.environ.get("HOTKEY", "9+1").strip()
        log_level = os.environ.get("LOG_LEVEL", "INFO").strip().upper()

        if not relay_url:
            relay_url = input("RELAY_URL (URL Cloud Function): ").strip()
        if not api_key:
            api_key = input("API_KEY (общий секрет): ").strip()
        if not nick:
            nick = input("Ваш никнейм: ").strip()

        if not relay_url or not api_key or not nick:
            print("ОШИБКА: RELAY_URL, API_KEY и NICK обязательны.", file=sys.stderr)
            sys.exit(2)

        try:
            hotkey = tuple(p.strip() for p in hotkey_str.split("+") if p.strip())
        except Exception:
            hotkey = ("9", "1")
        if not hotkey:
            hotkey = ("9", "1")

        return cls(relay_url=relay_url, api_key=api_key, nick=nick,
                   hotkey=hotkey, log_level=log_level)


# ============================================================================
# §A'. ЛОГИРОВАНИЕ — структурированное, по компонентам, цветное в терминале
# ============================================================================

class _ColorFormatter(logging.Formatter):
    """Простой цветной форматтер. ANSI коды работают в Windows Terminal,
    iTerm, Terminal.app и большинстве современных эмуляторов."""
    COLORS = {
        logging.DEBUG: "\x1b[37m",     # серый
        logging.INFO: "\x1b[36m",      # циан
        logging.WARNING: "\x1b[33m",   # жёлтый
        logging.ERROR: "\x1b[31m",     # красный
        logging.CRITICAL: "\x1b[35m",  # пурпурный
    }
    RESET = "\x1b[0m"

    def __init__(self, use_color: bool):
        super().__init__()
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        ms = int(record.msecs)
        comp = record.name
        lvl = record.levelname
        msg = record.getMessage()
        line = f"{ts}.{ms:03d} [{lvl:<7}] {comp:<10} │ {msg}"
        if self.use_color:
            color = self.COLORS.get(record.levelno, "")
            line = f"{color}{line}{self.RESET}"
        return line


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level)
    # удаляем все существующие хендлеры, чтобы не было дублей
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stderr)
    use_color = sys.stderr.isatty() and os.environ.get("NO_COLOR", "") == ""
    handler.setFormatter(_ColorFormatter(use_color=use_color))
    root.addHandler(handler)

    # снижаем шум третьих библиотек
    for noisy in ("urllib3", "asyncio", "aiohttp", "boto3", "botocore", "PIL"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# Геттер логгеров по компонентам (соответствует именам Producer-ов и команд)
def L(component: str) -> logging.Logger:
    return logging.getLogger(component)


# ============================================================================
# §B. ДОМЕННЫЕ ТИПЫ (соответствие §1.2 и §1.6)
# ============================================================================

class State(Enum):
    """Множество состояний Q из §1.6."""
    INIT = "INIT"
    REG = "REG"
    RUN = "RUN"
    CAP = "CAP"
    UPL = "UPL"
    HALT = "HALT"


class EventKind(Enum):
    """Алфавит входных событий Σ из §1.6."""
    NICK_ENTERED = auto()
    HEARTBEAT_OK = auto()
    HEARTBEAT_ERR = auto()
    HOTKEY = auto()
    CAPTURE_OK = auto()
    CAPTURE_ERR = auto()
    UPLOAD_OK = auto()
    UPLOAD_ERR = auto()
    MSG_RECEIVED = auto()
    TEXT_INPUT = auto()
    QUIT = auto()


class MsgType:
    """Типы сообщений из §1.2."""
    PRESENCE = "PRESENCE"
    SCREENSHOT_NOTIFY = "SCREENSHOT_NOTIFY"
    TEXT = "TEXT"


@dataclass(frozen=True)
class Message:
    """M ⊆ ID × U × U × T × P × T — §1.2."""
    id: str
    src: str
    dst: str
    type: str
    payload: dict
    ts: int  # серверный timestamp если возможно


@dataclass(frozen=True)
class Event:
    kind: EventKind
    data: dict = field(default_factory=dict)


# ============================================================================
# §C. ИНТЕРФЕЙСЫ (Protocol-классы, Dependency Inversion)
# ============================================================================

class ScreenCapture(Protocol):
    def grab_jpeg(self, quality: int = JPEG_QUALITY) -> bytes: ...


class SystemNotifier(Protocol):
    def notify(self, title: str, body: str) -> None: ...


class RelayClient(Protocol):
    async def heartbeat(self, nick: str) -> list[str]: ...
    async def pull(self, nick: str, ack: list[str]) -> list[Message]: ...
    async def send(self, msg: Message) -> None: ...
    async def request_upload_url(self, nick: str, key: str) -> str: ...
    async def request_download_url(self, nick: str, key: str) -> str: ...


# ============================================================================
# §D. STRATEGY — платформо-специфичные реализации
# ============================================================================

class MssCapture:
    """Кроссплатформенный скриншот через mss + Pillow.
    mss работает идентично на Win и Mac, поэтому это единая реализация
    (Strategy всё равно применима — для Linux потребуется отдельная,
    с учётом X11/Wayland)."""

    def __init__(self):
        # отложенный импорт чтобы не тащить mss при тестах
        import mss as _mss
        from PIL import Image as _Image
        self._mss = _mss
        self._Image = _Image

    def grab_jpeg(self, quality: int = JPEG_QUALITY) -> bytes:
        log = L("capture")
        with self._mss.mss() as sct:
            # monitors[0] — виртуальный экран, объединяющий все мониторы
            mon = sct.monitors[0]
            shot = sct.grab(mon)
            img = self._Image.frombytes("RGB", shot.size, shot.bgra, "raw", "BGRX")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            data = buf.getvalue()
        log.info("Снимок сделан: %dx%d, %.1f KB, JPEG q=%d",
                 mon["width"], mon["height"], len(data) / 1024, quality)
        return data


class WinNotifier:
    """Системная нотификация на Windows через winotify."""

    def __init__(self, app_name: str = "Peer"):
        from winotify import Notification
        self._Notification = Notification
        self._app_name = app_name

    def notify(self, title: str, body: str) -> None:
        n = self._Notification(app_id=self._app_name, title=title, msg=body)
        n.show()


class MacNotifier:
    """Системная нотификация на macOS через osascript (встроен в систему)."""

    def notify(self, title: str, body: str) -> None:
        # экранируем кавычки чтобы AppleScript не сломался
        t = title.replace('"', '\\"')
        b = body.replace('"', '\\"')
        script = f'display notification "{b}" with title "{t}"'
        subprocess.run(
            ["osascript", "-e", script],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


# ============================================================================
# §E. ABSTRACT FACTORY — выбор стратегий по ОС
# ============================================================================

class PlatformFactory:
    @staticmethod
    def create() -> tuple[ScreenCapture, SystemNotifier]:
        log = L("platform")
        sysname = platform.system()
        log.info("Определена ОС: %s %s", sysname, platform.release())

        capture: ScreenCapture = MssCapture()

        if sysname == "Windows":
            notifier: SystemNotifier = WinNotifier()
        elif sysname == "Darwin":
            notifier = MacNotifier()
        else:
            raise RuntimeError(
                f"ОС '{sysname}' не поддерживается. "
                f"Поддерживаются Windows и macOS."
            )

        return capture, notifier


# ============================================================================
# §G. ИНФРАСТРУКТУРА НАДЁЖНОСТИ
# ============================================================================

class RetriableError(Exception):
    """Ошибка, которую имеет смысл повторить (5xx, сеть, таймаут)."""


class FatalError(Exception):
    """Ошибка, которую повторять бессмысленно (4xx, неверный API_KEY и т.п.)."""


class ResilientCaller:
    """Template Method для §2.4: decorrelated jitter retry."""

    def __init__(self, name: str = "call",
                 max_attempts: int = RETRY_MAX_ATTEMPTS,
                 base: float = RETRY_BASE,
                 cap: float = RETRY_CAP):
        self._name = name
        self._max_attempts = max_attempts
        self._base = base
        self._cap = cap

    async def call(self, fn: Callable, *args, **kwargs):
        log = L("retry")
        sleep_t = self._base
        last_err: Optional[Exception] = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return await fn(*args, **kwargs)
            except FatalError as e:
                log.error("Невосстановимая ошибка в '%s': %s", self._name, e)
                raise
            except RetriableError as e:
                last_err = e
                if attempt == self._max_attempts:
                    log.error(
                        "'%s' провалился окончательно после %d попыток: %s",
                        self._name, attempt, e,
                    )
                    raise
                sleep_t = min(self._cap, random.uniform(self._base, sleep_t * 3))
                log.warning(
                    "'%s' попытка %d/%d не удалась (%s); повтор через %.2f с",
                    self._name, attempt, self._max_attempts, e, sleep_t,
                )
                await asyncio.sleep(sleep_t)
            except Exception as e:
                log.exception("Непредвиденная ошибка в '%s': %s", self._name, e)
                raise
        raise RuntimeError(f"unreachable: last_err={last_err}")


class DedupCache:
    """LRU+TTL кэш для §2.3. Реализует инвариант I1 на клиенте."""

    def __init__(self, maxsize: int = DEDUP_CACHE_SIZE, ttl: int = DEDUP_CACHE_TTL):
        self._maxsize = maxsize
        self._ttl = ttl
        self._cache: "OrderedDict[str, float]" = OrderedDict()

    def _evict(self) -> None:
        now = time.time()
        # выкидываем по TTL
        for k in list(self._cache.keys()):
            if now - self._cache[k] > self._ttl:
                del self._cache[k]
            else:
                break  # OrderedDict сохраняет порядок вставки
        # выкидываем по размеру
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)

    def filter(self, messages: list[Message]) -> list[Message]:
        self._evict()
        out: list[Message] = []
        now = time.time()
        for m in messages:
            if m.id in self._cache:
                continue
            self._cache[m.id] = now
            out.append(m)
        return out


# ============================================================================
# §F. ADAPTERS — внешние сервисы
# ============================================================================

class YCFRelay:
    """Adapter: HTTP-вызовы YCF → методы RelayClient."""

    def __init__(self, base_url: str, api_key: str, session):
        self._url = base_url.rstrip("/")
        self._key = api_key
        self._session = session

    async def _call(self, op: str, payload: dict) -> dict:
        log = L("relay")
        body = {"op": op, "api_key": self._key, **payload}
        log.debug("→ %s %s", op, {k: v for k, v in payload.items()
                                  if k not in ("api_key",)})
        import aiohttp
        try:
            async with self._session.post(
                self._url,
                json=body,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            ) as resp:
                text = await resp.text()
                status = resp.status
        except asyncio.TimeoutError as e:
            raise RetriableError(f"HTTP timeout op={op}") from e
        except aiohttp.ClientError as e:
            raise RetriableError(f"HTTP client error op={op}: {e}") from e

        if status >= 500:
            raise RetriableError(f"HTTP {status} op={op}: {text[:200]}")
        if status >= 400:
            raise FatalError(f"HTTP {status} op={op}: {text[:200]}")

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise RetriableError(f"bad JSON op={op}: {text[:200]}") from e

        if not data.get("ok"):
            err = data.get("error", "unknown")
            # сервер сам помечает retriable
            if data.get("retriable"):
                raise RetriableError(f"server error op={op}: {err}")
            raise FatalError(f"server error op={op}: {err}")

        log.debug("← %s ok", op)
        return data

    async def heartbeat(self, nick: str) -> list[str]:
        data = await self._call("heartbeat", {"user": nick})
        return list(data.get("peers", []))

    async def pull(self, nick: str, ack: list[str]) -> list[Message]:
        data = await self._call("pull", {"user": nick, "ack": ack})
        out: list[Message] = []
        for m in data.get("messages", []):
            out.append(Message(
                id=m["id"], src=m["src"], dst=m["dst"],
                type=m["type"], payload=m.get("payload", {}),
                ts=int(m.get("ts", 0)),
            ))
        return out

    async def send(self, msg: Message) -> None:
        await self._call("send", {
            "msg": {
                "id": msg.id, "src": msg.src, "dst": msg.dst,
                "type": msg.type, "payload": msg.payload, "ts": msg.ts,
            }
        })

    async def request_upload_url(self, nick: str, key: str) -> str:
        data = await self._call("request_upload_url", {"user": nick, "key": key})
        return data["url"]

    async def request_download_url(self, nick: str, key: str) -> str:
        data = await self._call("request_download_url", {"user": nick, "key": key})
        return data["url"]


class HttpBlobClient:
    """Adapter: PUT в pre-signed URL Object Storage. §1.8 data plane."""

    def __init__(self, session):
        self._session = session

    async def put(self, presigned_url: str, data: bytes,
                  content_type: str = "image/jpeg") -> None:
        log = L("blob")
        import aiohttp
        try:
            async with self._session.put(
                presigned_url,
                data=data,
                headers={"Content-Type": content_type},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status >= 500:
                    raise RetriableError(f"S3 HTTP {resp.status}")
                if resp.status >= 400:
                    body = await resp.text()
                    raise FatalError(f"S3 HTTP {resp.status}: {body[:200]}")
        except asyncio.TimeoutError as e:
            raise RetriableError("S3 PUT timeout") from e
        except aiohttp.ClientError as e:
            raise RetriableError(f"S3 PUT client error: {e}") from e
        log.info("Загружено в Object Storage: %.1f KB", len(data) / 1024)


# ============================================================================
# §H. PRODUCERS — асинхронные источники событий
# ============================================================================

class PeersRegistry:
    """Просто общий контейнер для последнего peer-list. Coord читает,
    Heartbeat пишет. Доступ без блокировок: GIL и атомарность присваивания
    Python-объекта гарантируют корректность для одного writer."""

    def __init__(self):
        self._peers: list[str] = []

    def update(self, peers: list[str]) -> None:
        self._peers = list(peers)

    def get(self) -> list[str]:
        return list(self._peers)


class HeartbeatProducer:
    """§2.1 + §1.4. Шлёт heartbeat каждые TAU секунд, получает peer-list."""

    def __init__(self, nick: str, relay: RelayClient,
                 events: asyncio.Queue, peers: PeersRegistry):
        self._nick = nick
        self._relay = relay
        self._events = events
        self._peers = peers
        self._caller = ResilientCaller(name="heartbeat", max_attempts=3)

    async def run(self) -> None:
        log = L("heartbeat")
        log.info("Старт: τ=%.1fs, Θ=%.1fs (TTL для других)",
                 TAU_HEARTBEAT, PEER_TTL_SEC)
        while True:
            try:
                peers = await self._caller.call(self._relay.heartbeat, self._nick)
                old = set(self._peers.get())
                new = set(peers)
                self._peers.update(peers)
                if new != old:
                    appeared = new - old
                    disappeared = old - new
                    if appeared:
                        log.info("Онлайн появились: %s", ", ".join(sorted(appeared)))
                    if disappeared:
                        log.info("Ушли в офлайн: %s", ", ".join(sorted(disappeared)))
                await self._events.put(Event(EventKind.HEARTBEAT_OK, {"peers": peers}))
            except Exception as e:
                log.warning("Heartbeat не прошёл, продолжаем: %s", e)
                await self._events.put(Event(EventKind.HEARTBEAT_ERR, {"error": str(e)}))
            await asyncio.sleep(TAU_HEARTBEAT)


class PollerProducer:
    """§2.2 + §2.3. Опрашивает relay на новые сообщения, дедуплицирует,
    эмитит MSG_RECEIVED в общую очередь."""

    def __init__(self, nick: str, relay: RelayClient,
                 events: asyncio.Queue, dedup: DedupCache):
        self._nick = nick
        self._relay = relay
        self._events = events
        self._dedup = dedup
        self._caller = ResilientCaller(name="poll", max_attempts=3)

    async def run(self) -> None:
        log = L("poller")
        log.info("Старт: p=%.1fs", POLL_INTERVAL)
        ack_buffer: list[str] = []
        while True:
            try:
                msgs = await self._caller.call(self._relay.pull, self._nick, ack_buffer)
                ack_buffer.clear()
                fresh = self._dedup.filter(msgs)
                if msgs and not fresh:
                    log.debug("Все %d сообщений уже видели (дедуп)", len(msgs))
                for m in fresh:
                    log.info("Получено: %s от '%s' (id=%s)", m.type, m.src, m.id[:8])
                    await self._events.put(Event(EventKind.MSG_RECEIVED, {"msg": m}))
                    ack_buffer.append(m.id)
            except Exception as e:
                log.warning("Polling не прошёл: %s", e)
            await asyncio.sleep(POLL_INTERVAL)


class HotkeyProducer:
    """Слушает глобальный хоткей через pynput. Триггер — все клавиши
    из self._combo одновременно зажаты. Debounce: после срабатывания
    ждём отжатия хотя бы одной клавиши."""

    def __init__(self, combo: tuple[str, ...], events: asyncio.Queue,
                 loop: asyncio.AbstractEventLoop):
        self._combo = set(combo)
        self._events = events
        self._loop = loop
        self._pressed: set[str] = set()
        self._armed = True  # для debounce

    def _emit(self) -> None:
        # вызывается из pynput-потока; переключаем в loop безопасно
        asyncio.run_coroutine_threadsafe(
            self._events.put(Event(EventKind.HOTKEY, {})),
            self._loop,
        )

    def _on_press(self, key) -> None:
        try:
            c = key.char
        except AttributeError:
            return
        if c is None or c not in self._combo:
            return
        self._pressed.add(c)
        if self._armed and self._combo.issubset(self._pressed):
            self._armed = False
            L("hotkey").info("Хоткей %s сработал", "+".join(sorted(self._combo)))
            self._emit()

    def _on_release(self, key) -> None:
        try:
            c = key.char
        except AttributeError:
            return
        if c is None:
            return
        self._pressed.discard(c)
        if not self._combo.issubset(self._pressed):
            self._armed = True

    async def run(self) -> None:
        log = L("hotkey")
        log.info("Слушаю глобальный хоткей: %s", "+".join(sorted(self._combo)))
        if platform.system() == "Darwin":
            log.warning(
                "macOS: терминалу нужно разрешение Accessibility. "
                "Если хоткей не срабатывает — System Settings → Privacy & Security "
                "→ Accessibility → добавить Terminal/iTerm."
            )

        try:
            from pynput import keyboard as _kb
        except Exception as e:
            log.error("Не могу импортировать pynput (%s). Хоткей выключен.", e)
            return

        listener = _kb.Listener(on_press=self._on_press, on_release=self._on_release)
        # listener — daemon-поток, его start неблокирующий
        listener.daemon = True
        listener.start()
        try:
            # держим корутину живой
            while True:
                await asyncio.sleep(3600)
        finally:
            listener.stop()


class ConsoleProducer:
    """Читает stdin построчно. Команды:
       /peers           — показать пиров онлайн
       /to <nick>       — выбрать собеседника
       /quit            — выход
       <текст>          — отправить выбранному собеседнику
    """

    def __init__(self, events: asyncio.Queue, peers: PeersRegistry,
                 target_holder: "TargetHolder"):
        self._events = events
        self._peers = peers
        self._target = target_holder

    async def _readline(self) -> str:
        # asyncio не имеет переносимого stdin-ридера, используем executor
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, sys.stdin.readline)

    async def run(self) -> None:
        log = L("console")
        print_hint()
        while True:
            try:
                line = await self._readline()
            except (EOFError, KeyboardInterrupt):
                await self._events.put(Event(EventKind.QUIT))
                return
            if line == "":
                # EOF
                await self._events.put(Event(EventKind.QUIT))
                return
            line = line.rstrip("\r\n")
            if not line:
                continue

            if line.startswith("/"):
                parts = line.split(maxsplit=1)
                cmd = parts[0].lower()
                arg = parts[1] if len(parts) > 1 else ""
                if cmd == "/quit" or cmd == "/exit":
                    await self._events.put(Event(EventKind.QUIT))
                    return
                if cmd == "/peers":
                    peers = self._peers.get()
                    if peers:
                        print(f"Онлайн: {', '.join(peers)}", flush=True)
                    else:
                        print("Никого нет онлайн (или ещё не пришёл heartbeat)",
                              flush=True)
                    continue
                if cmd == "/to":
                    if not arg:
                        print("Использование: /to <nick>", flush=True)
                        continue
                    self._target.set(arg.strip())
                    print(f"Выбран собеседник: {arg.strip()}", flush=True)
                    continue
                print(f"Неизвестная команда: {cmd}", flush=True)
                continue

            # обычный текст — отправка
            tgt = self._target.get()
            if not tgt:
                # авто-выбор если ровно один пир онлайн
                peers = self._peers.get()
                if len(peers) == 1:
                    tgt = peers[0]
                    self._target.set(tgt)
                    print(f"(авто) собеседник: {tgt}", flush=True)
                else:
                    print("Выберите собеседника командой /to <nick>", flush=True)
                    continue
            await self._events.put(Event(EventKind.TEXT_INPUT,
                                         {"text": line, "to": tgt}))


class TargetHolder:
    """Контейнер для текущего выбранного собеседника."""
    def __init__(self):
        self._target: Optional[str] = None

    def get(self) -> Optional[str]:
        return self._target

    def set(self, nick: str) -> None:
        self._target = nick


def print_hint() -> None:
    print(
        "\nКоманды: /peers — кто онлайн, /to <nick> — выбрать, "
        "/quit — выход, иначе — отправить текст выбранному.\n",
        flush=True,
    )


# ============================================================================
# §I. COMMANDS — выходные действия γ ∈ Γ из §1.6
# ============================================================================

class Command(Protocol):
    async def execute(self) -> None: ...


class CaptureCmd:
    """γ_capture: делает снимок в executor (mss — sync) и эмитит CAPTURE_OK/ERR."""

    def __init__(self, capture: ScreenCapture, events: asyncio.Queue):
        self._capture = capture
        self._events = events

    async def execute(self) -> None:
        log = L("capture")
        loop = asyncio.get_running_loop()
        try:
            data = await loop.run_in_executor(None, self._capture.grab_jpeg, JPEG_QUALITY)
            await self._events.put(Event(EventKind.CAPTURE_OK, {"bytes": data}))
        except Exception as e:
            log.exception("Скриншот не удался: %s", e)
            await self._events.put(Event(EventKind.CAPTURE_ERR, {"error": str(e)}))


class UploadCmd:
    """γ_upload: §2.7 pre-signed URL workflow.
       1) request_upload_url, 2) PUT в blob, 3) send SCREENSHOT_NOTIFY."""

    def __init__(self, relay: RelayClient, blob: HttpBlobClient,
                 nick: str, target: str, data: bytes,
                 events: asyncio.Queue):
        self._relay = relay
        self._blob = blob
        self._nick = nick
        self._target = target
        self._data = data
        self._events = events
        self._caller = ResilientCaller(name="upload", max_attempts=4)

    async def execute(self) -> None:
        log = L("upload")
        if not self._target:
            log.error("Не выбран собеседник, скриншот отменён")
            await self._events.put(Event(EventKind.UPLOAD_ERR,
                                          {"error": "no target"}))
            return
        key = f"{uuid.uuid4()}.jpg"
        try:
            log.info("Запрашиваю presigned URL на upload, key=%s", key)
            upload_url = await self._caller.call(
                self._relay.request_upload_url, self._nick, key)
            log.info("PUT в Object Storage (%.1f KB)...", len(self._data) / 1024)
            await self._caller.call(self._blob.put, upload_url, self._data)
            msg = Message(
                id=str(uuid.uuid4()),
                src=self._nick, dst=self._target,
                type=MsgType.SCREENSHOT_NOTIFY,
                payload={"key": key},
                ts=int(time.time() * 1000),
            )
            log.info("Уведомляю '%s' о новом скриншоте", self._target)
            await self._caller.call(self._relay.send, msg)
            await self._events.put(Event(EventKind.UPLOAD_OK, {"key": key}))
        except Exception as e:
            log.error("Загрузка скриншота не удалась: %s", e)
            await self._events.put(Event(EventKind.UPLOAD_ERR, {"error": str(e)}))


class SendTextCmd:
    """γ_send_text: отправляет TEXT-сообщение выбранному собеседнику."""

    def __init__(self, relay: RelayClient, nick: str, target: str, text: str):
        self._relay = relay
        self._nick = nick
        self._target = target
        self._text = text
        self._caller = ResilientCaller(name="send_text", max_attempts=4)

    async def execute(self) -> None:
        log = L("text")
        msg = Message(
            id=str(uuid.uuid4()),
            src=self._nick, dst=self._target, type=MsgType.TEXT,
            payload={"text": self._text},
            ts=int(time.time() * 1000),
        )
        try:
            await self._caller.call(self._relay.send, msg)
            log.info("→ '%s': %s", self._target, self._text)
        except Exception as e:
            log.error("Сообщение для '%s' не доставлено: %s", self._target, e)


class NotifySysCmd:
    """γ_notify_sys: системная нотификация (для входящего TEXT)."""

    def __init__(self, notifier: SystemNotifier, title: str, body: str):
        self._notifier = notifier
        self._title = title
        self._body = body

    async def execute(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, self._notifier.notify, self._title, self._body)
            L("notify").info("Системное уведомление: '%s' — %s",
                             self._title, self._body[:60])
        except Exception as e:
            L("notify").error("Не удалось показать уведомление: %s", e)


class PrintLinkCmd:
    """γ_print_link: печатает кликабельную ссылку на скриншот.
       Использует OSC 8 escape (поддерживают Windows Terminal, iTerm2,
       Terminal.app, и многие современные эмуляторы).
       На fallback показывает обычный URL текстом — пользователь сможет
       Ctrl/Cmd+клик в большинстве терминалов."""

    def __init__(self, relay: RelayClient, nick: str, key: str, src: str):
        self._relay = relay
        self._nick = nick
        self._key = key
        self._src = src

    async def execute(self) -> None:
        log = L("screenshot")
        try:
            url = await ResilientCaller(name="dl_url", max_attempts=3).call(
                self._relay.request_download_url, self._nick, self._key)
        except Exception as e:
            log.error("Не удалось получить ссылку на скриншот: %s", e)
            print(f"⚠ Скриншот от '{self._src}' получен, но ссылка не доступна: {e}",
                  flush=True)
            return

        # OSC 8 hyperlink
        text = f"🖼  Новый скриншот от {self._src}"
        osc_link = f"\x1b]8;;{url}\x1b\\{text}\x1b]8;;\x1b\\"
        print(f"\n{osc_link}", flush=True)
        # резерв — обычный URL, чтобы можно было скопировать
        print(f"   URL: {url}\n", flush=True)
        log.info("Ссылка на скриншот от '%s' выведена в консоль", self._src)


# ============================================================================
# §J. MEDIATOR / COORDINATOR — ядро FSM
# ============================================================================

class Coordinator:
    """Mediator: единственный владелец state.
       Реализует таблично функцию δ × λ из §1.6."""

    def __init__(self, cfg: Config,
                 relay: RelayClient, blob: HttpBlobClient,
                 capture: ScreenCapture, notifier: SystemNotifier,
                 events: asyncio.Queue,
                 peers: PeersRegistry, target: TargetHolder):
        self._cfg = cfg
        self._relay = relay
        self._blob = blob
        self._capture = capture
        self._notifier = notifier
        self._events = events
        self._peers = peers
        self._target = target
        self._state = State.INIT

        # Таблица переходов — буквально δ из §1.6.
        # Ключ: (State, EventKind) → (NextState, фабрика_команды).
        # Фабрика возвращает либо Command, либо None (если действия нет).
        Co = Coordinator
        self._transitions: dict[
            tuple[State, EventKind],
            tuple[State, Callable[["Coordinator", Event], Optional[Command]]]
        ] = {
            (State.INIT, EventKind.NICK_ENTERED): (State.REG, Co._noop),
            (State.REG, EventKind.HEARTBEAT_OK): (State.RUN, Co._on_first_hb),
            (State.REG, EventKind.HEARTBEAT_ERR): (State.REG, Co._noop),

            (State.RUN, EventKind.HEARTBEAT_OK): (State.RUN, Co._noop),
            (State.RUN, EventKind.HEARTBEAT_ERR): (State.RUN, Co._noop),
            (State.RUN, EventKind.HOTKEY): (State.CAP, Co._start_capture),
            (State.RUN, EventKind.MSG_RECEIVED): (State.RUN, Co._dispatch_msg),
            (State.RUN, EventKind.TEXT_INPUT): (State.RUN, Co._on_text_input),

            (State.CAP, EventKind.CAPTURE_OK): (State.UPL, Co._start_upload),
            (State.CAP, EventKind.CAPTURE_ERR): (State.RUN, Co._noop),
            # игнорируем посторонние события в CAP/UPL чтобы не путать FSM
            (State.CAP, EventKind.MSG_RECEIVED): (State.CAP, Co._dispatch_msg),
            (State.CAP, EventKind.HEARTBEAT_OK): (State.CAP, Co._noop),
            (State.CAP, EventKind.HEARTBEAT_ERR): (State.CAP, Co._noop),

            (State.UPL, EventKind.UPLOAD_OK): (State.RUN, Co._noop),
            (State.UPL, EventKind.UPLOAD_ERR): (State.RUN, Co._noop),
            (State.UPL, EventKind.MSG_RECEIVED): (State.UPL, Co._dispatch_msg),
            (State.UPL, EventKind.HEARTBEAT_OK): (State.UPL, Co._noop),
            (State.UPL, EventKind.HEARTBEAT_ERR): (State.UPL, Co._noop),
        }

    # ---- фабрики команд (методы класса, потому что нужны self) ----

    def _noop(self, ev: Event) -> Optional[Command]:
        return None

    def _on_first_hb(self, ev: Event) -> Optional[Command]:
        peers = ev.data.get("peers", [])
        L("coord").info("Регистрация прошла. Онлайн пиров: %d", len(peers))
        if peers and not self._target.get():
            if len(peers) == 1:
                self._target.set(peers[0])
                print(f"(авто) собеседник: {peers[0]}", flush=True)
        return None

    def _start_capture(self, ev: Event) -> Optional[Command]:
        L("coord").info("HOTKEY → CAPTURE")
        return CaptureCmd(self._capture, self._events)

    def _start_upload(self, ev: Event) -> Optional[Command]:
        target = self._target.get()
        if not target:
            peers = self._peers.get()
            if len(peers) == 1:
                target = peers[0]
                self._target.set(target)
        if not target:
            L("coord").error(
                "Нет выбранного собеседника, скриншот не отправляется. "
                "Используйте /to <nick>")
            # сразу эмитим UPLOAD_ERR чтобы FSM вернулся в RUN
            asyncio.create_task(self._events.put(
                Event(EventKind.UPLOAD_ERR, {"error": "no target"})))
            return None
        L("coord").info("CAPTURE_OK → UPLOAD к '%s'", target)
        return UploadCmd(self._relay, self._blob,
                         self._cfg.nick, target, ev.data["bytes"], self._events)

    def _on_text_input(self, ev: Event) -> Optional[Command]:
        target = ev.data.get("to") or self._target.get()
        if not target:
            L("coord").warning("Нет собеседника для отправки текста")
            return None
        return SendTextCmd(self._relay, self._cfg.nick, target, ev.data["text"])

    def _dispatch_msg(self, ev: Event) -> Optional[Command]:
        msg: Message = ev.data["msg"]
        if msg.type == MsgType.TEXT:
            text = msg.payload.get("text", "")
            return NotifySysCmd(self._notifier,
                                title=f"Сообщение от {msg.src}",
                                body=text)
        if msg.type == MsgType.SCREENSHOT_NOTIFY:
            key = msg.payload.get("key", "")
            return PrintLinkCmd(self._relay, self._cfg.nick, key, msg.src)
        L("coord").warning("Неизвестный тип сообщения: %s", msg.type)
        return None

    # ---- главный цикл ----

    async def run(self) -> None:
        log = L("coord")
        log.info("Coordinator стартовал в состоянии %s", self._state.value)
        # сразу переход INIT → REG (никнейм уже введён к моменту запуска)
        await self._events.put(Event(EventKind.NICK_ENTERED))

        while self._state != State.HALT:
            ev = await self._events.get()

            if ev.kind == EventKind.QUIT:
                log.info("Получен QUIT — завершаюсь")
                self._state = State.HALT
                break

            key = (self._state, ev.kind)
            if key not in self._transitions:
                log.debug("Игнор: event=%s в state=%s",
                          ev.kind.name, self._state.value)
                continue

            next_state, factory = self._transitions[key]
            old_state = self._state
            # переход состояния ПЕРЕД execute — Command может породить новое событие
            self._state = next_state
            if old_state != next_state:
                log.debug("Переход: %s → %s по %s",
                          old_state.value, next_state.value, ev.kind.name)

            try:
                cmd = factory(self, ev)
            except Exception as e:
                log.exception("Ошибка в фабрике команды: %s", e)
                continue

            if cmd is None:
                continue
            # выполняем команду асинхронно, чтобы не блокировать FSM
            asyncio.create_task(self._exec_safe(cmd))

    async def _exec_safe(self, cmd: Command) -> None:
        try:
            await cmd.execute()
        except Exception as e:
            L("coord").exception("Command.execute() упал: %s", e)


# ============================================================================
# §K. BOOTSTRAP / main()
# ============================================================================

BANNER = r"""
╔══════════════════════════════════════════════════════════════╗
║  peer.py  •  P2P screen+chat (YCF + YDB + Object Storage)    ║
║  Логи: stderr   |   ввод/вывод: stdout                       ║
╚══════════════════════════════════════════════════════════════╝
"""


async def amain() -> int:
    cfg = Config.from_env_and_prompt()
    setup_logging(cfg.log_level)

    log = L("main")
    print(BANNER)
    log.info("Никнейм: '%s'   Хоткей: %s   Relay: %s",
             cfg.nick, "+".join(cfg.hotkey), cfg.relay_url)

    # Abstract Factory
    try:
        capture, notifier = PlatformFactory.create()
    except Exception as e:
        log.critical("Не удалось инициализировать ОС-зависимые компоненты: %s", e)
        return 1

    # HTTP-сессия для обоих Adapter-ов
    try:
        import aiohttp
    except ImportError:
        log.critical("Не установлен aiohttp. Установите: pip install aiohttp")
        return 1

    http = aiohttp.ClientSession()
    relay = YCFRelay(cfg.relay_url, cfg.api_key, http)
    blob = HttpBlobClient(http)

    events: asyncio.Queue[Event] = asyncio.Queue()
    dedup = DedupCache()
    peers = PeersRegistry()
    target = TargetHolder()

    loop = asyncio.get_running_loop()

    producers = [
        HeartbeatProducer(cfg.nick, relay, events, peers),
        PollerProducer(cfg.nick, relay, events, dedup),
        HotkeyProducer(cfg.hotkey, events, loop),
        ConsoleProducer(events, peers, target),
    ]
    coord = Coordinator(cfg, relay, blob, capture, notifier,
                        events, peers, target)

    # обработчик Ctrl+C — кладём QUIT в очередь
    def _sigint_handler():
        log.info("Ctrl+C — инициирую завершение")
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(events.put(Event(EventKind.QUIT))))

    if platform.system() != "Windows":
        # на Windows asyncio не поддерживает add_signal_handler
        try:
            loop.add_signal_handler(signal.SIGINT, _sigint_handler)
            loop.add_signal_handler(signal.SIGTERM, _sigint_handler)
        except NotImplementedError:
            pass

    tasks = [asyncio.create_task(p.run(), name=type(p).__name__) for p in producers]
    coord_task = asyncio.create_task(coord.run(), name="Coordinator")

    try:
        # ждём пока Coordinator не закончит (по QUIT)
        await coord_task
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await http.close()
        log.info("Все задачи остановлены. До свидания.")
    return 0


def main() -> None:
    try:
        sys.exit(asyncio.run(amain()))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()