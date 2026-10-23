"""Multipart splitting, frame parsing, heartbeat and backlog handling."""

import asyncio
from datetime import UTC, datetime, timedelta

import aiohttp
from aiohttp.test_utils import TestServer
import pytest

from custom_components.hikvision_bridge.model.vocabulary import Kind, canonical
from custom_components.hikvision_bridge.wire.feed import (
    AlarmFeed,
    MultipartSplitter,
    frame_from_part,
)
from custom_components.hikvision_bridge.wire.link import Endpoint, IsapiLink
from tests.fakedevice import FakeDevice, event_xml, xml_part

pytestmark = pytest.mark.usefixtures("socket_enabled")


def _now_iso(offset_s: float = 0) -> str:
    return (datetime.now(UTC) + timedelta(seconds=offset_s)).isoformat()


def test_splitter_handles_arbitrary_chunking() -> None:
    stream = b"".join(
        [
            xml_part(event_xml("VMD")),
            xml_part(
                event_xml(
                    "videoloss",
                    "inactive",
                    channel=None,
                    namespace="http://www.isapi.org/ver20/XMLSchema",
                )
            ),
            b"--boundary\r\nContent-Type: image/jpeg\r\nContent-Length: 4\r\n\r\n\xff\xd8\xff\xd9\r\n",
        ]
    )
    for size in (1, 7, 64, len(stream)):
        splitter = MultipartSplitter("boundary")
        parts = [
            p for i in range(0, len(stream), size) for p in splitter.push(stream[i : i + size])
        ]
        assert [p.content_type for p in parts] == [
            "application/xml",
            "application/xml",
            "image/jpeg",
        ]
        assert parts[2].body == b"\xff\xd8\xff\xd9"


def test_splitter_without_content_length_uses_boundary() -> None:
    body = event_xml("fielddetection").encode()
    stream = b"--boundary\r\nContent-Type: application/xml\r\n\r\n" + body + b"\r\n--boundary\r\n"
    parts = list(MultipartSplitter("boundary").push(stream))
    assert len(parts) == 1
    assert parts[0].body.strip().endswith(b"</EventNotificationAlert>")


def test_frame_parsing_targets_regions_and_heartbeat() -> None:
    splitter = MultipartSplitter("boundary")
    smart = event_xml(
        "linedetection",
        extra="<DetectionRegionList><DetectionRegionEntry><regionID>2</regionID>"
        "<detectionTarget>human</detectionTarget></DetectionRegionEntry></DetectionRegionList>",
    )
    heartbeat = event_xml(
        "videoloss", "inactive", channel=None, namespace="http://www.isapi.org/ver20/XMLSchema"
    )
    frames = [frame_from_part(p) for p in splitter.push(xml_part(smart) + xml_part(heartbeat))]
    assert frames[0].raw_type == "linedetection"
    assert frames[0].targets == ("human",)
    assert frames[0].region_id == "2"
    assert frames[0].channel_id == "1"
    assert frames[1].heartbeat
    assert frames[1].channel_id is None


def test_duration_alert_takes_the_related_event() -> None:
    duration = event_xml(
        "duration",
        extra="<DurationList><Duration><relationEvent>fielddetection</relationEvent>"
        "</Duration></DurationList>",
    )
    bare = event_xml("duration")
    frames = [
        frame_from_part(p)
        for p in MultipartSplitter("boundary").push(xml_part(duration) + xml_part(bare))
    ]
    assert frames[0].raw_type == "fielddetection"
    assert canonical(frames[0].raw_type) == Kind.INTRUSION
    assert frames[0].is_active
    assert frames[0].channel_id == "1"
    assert frames[0].extra["via"] == "duration"
    assert frames[1].raw_type == "duration"
    assert "via" not in frames[1].extra


def test_json_duration_alert_takes_the_related_event() -> None:
    body = '{"eventType":"duration","eventState":"active","channelID":1,"DurationList":{"Duration":[{"relationEvent":"linedetection"}]}}'
    (part,) = list(MultipartSplitter("boundary").push(xml_part(body, "application/json")))
    frame = frame_from_part(part)
    assert frame.raw_type == "linedetection"
    assert frame.extra["via"] == "duration"


def test_json_frame() -> None:
    splitter = MultipartSplitter("boundary")
    body = '{"eventType":"regionEntrance","eventState":"active","channelID":3,"targetType":"vehicle","dateTime":"2026-10-21T10:00:00Z"}'
    (part,) = list(splitter.push(xml_part(body, "application/json")))
    frame = frame_from_part(part)
    assert frame.channel_id == "3"
    assert frame.targets == ("vehicle",)


async def test_feed_delivers_live_events_and_drops_backlog() -> None:
    device = FakeDevice(profile="camera_g2")
    device.stream_parts = [
        xml_part(event_xml("videoloss", "inactive", channel="1", when=_now_iso())),
        xml_part(event_xml("VMD", when=_now_iso(-3600))),  # replayed backlog
        xml_part(event_xml("VMD", when=_now_iso())),
        xml_part(event_xml("fielddetection", when=_now_iso())),
    ]
    device.stream_hold = 0.2
    server = TestServer(device.app())
    await server.start_server()
    received = []
    states = []
    async with aiohttp.ClientSession() as session:
        link = IsapiLink(
            session, Endpoint(server.host, server.port), device.username, device.password
        )
        feed = AlarmFeed(link, received.append, lambda c, f: states.append(c))
        feed.start(lambda coro, name: asyncio.get_running_loop().create_task(coro, name=name))
        for _ in range(50):
            if len(received) >= 2:
                break
            await asyncio.sleep(0.02)
        await feed.stop()
    await server.close()
    assert [f.raw_type for f in received] == ["VMD", "fielddetection"]
    assert feed.stats.heartbeats == 1
    assert feed.stats.backlog_dropped == 1
    assert states[0] is True
    assert states[-1] is False
