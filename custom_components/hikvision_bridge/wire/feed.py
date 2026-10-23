"""Reader for the device event stream (multipart alert stream)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import json
import re
import time
from typing import Any

from .faults import BadCredentials, BridgeFault, LockedOut
from .link import IsapiLink
from .scrub import scrubbed_logger
from .xmlview import XmlError, XmlNode, decode, parse

_LOGGER = scrubbed_logger(__name__)

STREAM_PATH = "/ISAPI/Event/notification/alertStream"
MAX_PART_BYTES = 2 * 1024 * 1024
SILENCE_LIMIT = 60.0
BACKOFF_FIRST = 5.0
BACKOFF_MAX = 300.0
BACKLOG_TOLERANCE = timedelta(seconds=5)


@dataclass(slots=True)
class Part:
    headers: dict[str, str]
    body: bytes

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "").split(";")[0].strip().lower()


class MultipartSplitter:
    """Incrementally cuts a multipart byte stream into parts.

    Uses ``Content-Length`` when present and the boundary otherwise. Oversized parts are
    skipped without ending the stream.
    """

    def __init__(self, boundary: str | None) -> None:
        self._boundary = ("--" + boundary.strip('"')).encode() if boundary else None
        self._buffer = bytearray()
        self._skip = 0

    def push(self, chunk: bytes) -> Iterator[Part]:
        self._buffer.extend(chunk)
        while True:
            if self._skip:
                dropped = min(self._skip, len(self._buffer))
                del self._buffer[:dropped]
                self._skip -= dropped
                if self._skip:
                    return
            part = self._next()
            if part is None:
                return
            yield part

    def _boundary_at_start(self) -> int | None:
        """Strip leading blank lines and a boundary line; return header start or None."""
        buf = self._buffer
        while buf[:2] == b"\r\n" or buf[:1] == b"\n":
            del buf[: 2 if buf[:2] == b"\r\n" else 1]
        if not buf.startswith(b"--"):
            # Some firmwares omit the boundary before the first headers.
            return 0 if re.match(rb"[A-Za-z-]+:", bytes(buf[:40])) else None
        line_end = buf.find(b"\n")
        if line_end == -1:
            return None
        return line_end + 1

    def _next(self) -> Part | None:
        start = self._boundary_at_start()
        if start is None:
            if len(self._buffer) > MAX_PART_BYTES:
                self._buffer.clear()
            return None
        header_end = self._buffer.find(b"\r\n\r\n", start)
        sep = 4
        if header_end == -1:
            header_end = self._buffer.find(b"\n\n", start)
            sep = 2
        if header_end == -1:
            return None
        headers: dict[str, str] = {}
        for line in decode(bytes(self._buffer[start:header_end])).splitlines():
            key, _, value = line.partition(":")
            if value:
                headers[key.strip().lower()] = value.strip()
        body_start = header_end + sep
        length = headers.get("content-length")
        if length and length.isdigit():
            size = int(length)
            if size > MAX_PART_BYTES:
                available = len(self._buffer) - body_start
                del self._buffer[:body_start]
                self._skip = size - min(size, available)
                del self._buffer[: min(size, available)]
                return Part(headers, b"")
            if len(self._buffer) - body_start < size:
                return None
            body = bytes(self._buffer[body_start : body_start + size])
            del self._buffer[: body_start + size]
            return Part(headers, body)
        if self._boundary is None:
            return None
        end = self._buffer.find(self._boundary, body_start)
        if end == -1:
            return None
        body = bytes(self._buffer[body_start:end]).rstrip(b"\r\n")
        del self._buffer[:end]
        return Part(headers, body)


@dataclass(slots=True)
class FeedFrame:
    """One normalised notification from a device."""

    raw_type: str
    state: str
    channel_id: str | None = None
    port_id: str | None = None
    device_time: datetime | None = None
    received: datetime = field(default_factory=lambda: datetime.now(UTC))
    targets: tuple[str, ...] = ()
    region_id: str | None = None
    description: str | None = None
    channel_name: str | None = None
    mac: str | None = None
    picture: bytes | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.state == "active"

    @property
    def heartbeat(self) -> bool:
        """Devices send inactive video-loss notices as keep-alive."""
        return self.raw_type.lower() == "videoloss" and not self.is_active


def _device_clock(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _non_empty(value: str | None) -> str | None:
    return value if value not in (None, "") else None


def _is_duration(raw_type: str) -> bool:
    """Duration alerts name the actual event in DurationList/Duration/relationEvent."""
    return raw_type.strip().lower() == "duration"


def frame_from_xml(node: XmlNode) -> FeedFrame | None:
    raw_type = node.text("eventType")
    if not raw_type:
        return None
    extra: dict[str, Any] = {"active_count": node.numeric("activePostCount")}
    if _is_duration(raw_type):
        related = next((n.value for n in node.walk("relationEvent") if n.value), None)
        if related:
            raw_type, extra["via"] = related, raw_type
    targets: list[str] = []
    for name in ("detectionTarget", "targetType", "TargetType"):
        for found in node.walk(name):
            if found.value:
                targets.extend(t.strip().lower() for t in found.value.split(",") if t.strip())
    region = next((n.value for n in node.walk("regionID") if n.value), None)
    port = (
        node.text("inputIOPortID")
        or node.text("dynInputIOPortID")
        or node.text("IOPortID")
        or next((n.value for n in node.walk("inputIOPortID") if n.value), None)
    )
    channel = _non_empty(node.text("channelID")) or _non_empty(node.text("dynChannelID"))
    return FeedFrame(
        raw_type=raw_type,
        state=(node.text("eventState") or "active").lower(),
        channel_id=channel,
        port_id=_non_empty(port),
        device_time=_device_clock(node.text("dateTime")),
        targets=tuple(dict.fromkeys(targets)),
        region_id=region,
        description=node.text("eventDescription"),
        channel_name=node.text("channelName"),
        mac=node.text("macAddress"),
        extra=extra,
    )


def _json_find(data: Any, key: str) -> Any:
    wanted = key.lower()
    if isinstance(data, dict):
        for k, v in data.items():
            if k.lower() == wanted:
                return v
        for v in data.values():
            found = _json_find(v, key)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _json_find(item, key)
            if found is not None:
                return found
    return None


def frame_from_json(data: Any) -> FeedFrame | None:
    if not isinstance(data, dict):
        return None
    raw_type = _json_find(data, "eventType")
    if not raw_type:
        return None
    extra: dict[str, Any] = {}
    if _is_duration(str(raw_type)) and (related := _json_find(data, "relationEvent")):
        raw_type, extra["via"] = related, str(raw_type)
    channel = _json_find(data, "channelID") or _json_find(data, "dynChannelID")
    target = _json_find(data, "targetType") or _json_find(data, "detectionTarget")
    targets = tuple(t.strip().lower() for t in str(target).split(",")) if target else ()
    port = _json_find(data, "inputIOPortID") or _json_find(data, "ioPortID")
    region = _json_find(data, "regionID")
    return FeedFrame(
        raw_type=str(raw_type),
        state=str(_json_find(data, "eventState") or "active").lower(),
        channel_id=str(channel) if channel not in (None, "") else None,
        port_id=str(port) if port not in (None, "") else None,
        device_time=_device_clock(_json_find(data, "dateTime")),
        targets=targets,
        region_id=str(region) if region not in (None, "") else None,
        description=_json_find(data, "eventDescription"),
        channel_name=_json_find(data, "channelName"),
        mac=_json_find(data, "macAddress"),
        extra=extra,
    )


def frame_from_part(part: Part) -> FeedFrame | bytes | None:
    """Turn a part into a frame, a picture (bytes) or nothing."""
    ctype = part.content_type
    if ctype.startswith("image/"):
        return part.body or None
    text = decode(part.body).strip()
    if not text:
        return None
    if "json" in ctype or text.startswith("{"):
        try:
            return frame_from_json(json.loads(text))
        except ValueError:
            return None
    try:
        return frame_from_xml(parse(text))
    except XmlError:
        _LOGGER.debug("Skipping unparsable event part (%d bytes)", len(part.body))
        return None


def boundary_of(content_type: str) -> str | None:
    match = re.search(r'boundary="?([^";]+)"?', content_type, flags=re.IGNORECASE)
    return match.group(1) if match else None


FrameHandler = Callable[[FeedFrame], None]
StateHandler = Callable[[bool, BridgeFault | None], Awaitable[None] | None]


@dataclass(slots=True)
class FeedStats:
    connects: int = 0
    frames: int = 0
    heartbeats: int = 0
    backlog_dropped: int = 0
    pictures: int = 0
    last_frame: float | None = None
    connected_since: datetime | None = None
    last_error: str | None = None


class AlarmFeed:
    """Keeps the event stream of one device open and delivers frames."""

    def __init__(
        self,
        link: IsapiLink,
        on_frame: FrameHandler,
        on_state: StateHandler,
        path: str = STREAM_PATH,
    ) -> None:
        self._link = link
        self._on_frame = on_frame
        self._on_state = on_state
        self._path = path
        self._task: asyncio.Task[None] | None = None
        self._connected = False
        self._clock_offset = timedelta(0)
        self.stats = FeedStats()

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def clock_offset(self) -> timedelta:
        """Device clock minus local clock, learnt from heartbeats."""
        return self._clock_offset

    def start(self, create_task: Callable[..., asyncio.Task[None]]) -> None:
        if self._task is None:
            self._task = create_task(
                self._run(), f"hikvision_bridge feed {self._link.endpoint.host}"
            )

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._set_state(False, None)

    async def _set_state(self, connected: bool, fault: BridgeFault | None) -> None:
        if connected == self._connected and fault is None:
            return
        self._connected = connected
        result = self._on_state(connected, fault)
        if asyncio.iscoroutine(result):
            await result

    async def _run(self) -> None:
        delay = BACKOFF_FIRST
        while True:
            try:
                await self._session()
                delay = BACKOFF_FIRST
                await self._set_state(False, None)
            except (BadCredentials, LockedOut) as err:
                self.stats.last_error = type(err).__name__
                await self._set_state(False, err)
                return
            except BridgeFault as err:
                self.stats.last_error = type(err).__name__
                await self._set_state(False, err)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.stats.last_error = type(err).__name__
                _LOGGER.exception(
                    "Event stream of %s failed unexpectedly", self._link.endpoint.host
                )
                await self._set_state(False, None)
            await asyncio.sleep(delay)
            delay = min(delay * 2, BACKOFF_MAX)

    async def _session(self) -> None:
        async with self._link.open_stream(self._path) as response:
            connected_at = datetime.now(UTC)
            self.stats.connects += 1
            self.stats.connected_since = connected_at
            splitter = MultipartSplitter(boundary_of(response.headers.get("Content-Type", "")))
            await self._set_state(True, None)
            pending: FeedFrame | None = None
            while True:
                try:
                    async with asyncio.timeout(SILENCE_LIMIT):
                        chunk = await response.content.readany()
                except TimeoutError:
                    _LOGGER.debug(
                        "Event stream of %s went silent; reconnecting", self._link.endpoint.host
                    )
                    return
                if not chunk:
                    return
                for part in splitter.push(chunk):
                    item = frame_from_part(part)
                    if isinstance(item, bytes):
                        self.stats.pictures += 1
                        if pending is not None:
                            pending.picture = item
                            self._deliver(pending, connected_at)
                            pending = None
                        continue
                    if pending is not None:
                        self._deliver(pending, connected_at)
                        pending = None
                    if item is None:
                        continue
                    self.stats.last_frame = time.monotonic()
                    if item.heartbeat:
                        self.stats.heartbeats += 1
                        if item.device_time is not None:
                            # Heartbeats are "now" on the device clock: track the offset.
                            self._clock_offset = item.device_time - item.received
                        continue
                    # Hold the frame briefly: a picture part may follow it.
                    pending = item
                if pending is not None and splitter_idle(splitter):
                    self._deliver(pending, connected_at)
                    pending = None

    def _deliver(self, frame: FeedFrame, connected_at: datetime) -> None:
        if frame.device_time is not None:
            # Events stamped before the connection existed are replays of a device backlog.
            local_time = frame.device_time - self._clock_offset
            if local_time < connected_at - BACKLOG_TOLERANCE:
                self.stats.backlog_dropped += 1
                return
        self.stats.frames += 1
        try:
            self._on_frame(frame)
        except Exception:
            _LOGGER.exception("Event handler failed")


def splitter_idle(splitter: MultipartSplitter) -> bool:
    """True when no partial part is buffered (the next chunk starts a new part)."""
    return not splitter._buffer.strip(b"\r\n")
