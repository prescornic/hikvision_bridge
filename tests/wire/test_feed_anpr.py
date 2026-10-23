"""Plate notifications (ANPR): plate details and the pictures that come with them."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import aiohttp
from aiohttp.test_utils import TestServer
import pytest

from custom_components.hikvision_bridge.model.vocabulary import Kind, canonical
from custom_components.hikvision_bridge.wire import feed
from custom_components.hikvision_bridge.wire.feed import (
    AlarmFeed,
    MultipartSplitter,
    frame_from_json,
    frame_from_part,
)
from custom_components.hikvision_bridge.wire.link import Endpoint, IsapiLink
from custom_components.hikvision_bridge.wire.xmlview import parse
from tests.fakedevice import FakeDevice, anpr_xml, picture_part, xml_part

pytestmark = pytest.mark.usefixtures("socket_enabled")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def test_xml_plate_details_are_read() -> None:
    frame = feed.frame_from_xml(parse(anpr_xml()))
    assert canonical(frame.raw_type) is Kind.ANPR
    assert frame.pictures_expected == 2
    assert frame.plate.as_dict() == {
        "plate": "B AB 1234",
        "country": "DE",
        "direction": "forward",
        "confidence": 97,
        "plate_type": "unknown",
        "plate_color": "white",
        "vehicle_type": "vehicle",
        "vehicle_color": "blue",
        "list": "allow",
    }


@pytest.mark.parametrize("raw", ["unknown", "noPlate", ""])
def test_unreadable_plate_has_no_number(raw: str) -> None:
    frame = feed.frame_from_xml(parse(anpr_xml(plate=raw)))
    assert frame.plate is not None
    assert frame.plate.number is None


def test_other_events_have_no_plate() -> None:
    frame = feed.frame_from_xml(
        parse(anpr_xml().replace("<ANPR>", "<X>").replace("</ANPR>", "</X>"))
    )
    assert frame.plate is None


def test_json_plate_details_are_read() -> None:
    frame = frame_from_json(
        {
            "eventType": "vehicleDetection",
            "channelID": 2,
            "picNum": 1,
            "ANPR": {
                "licensePlate": "W-12345",
                "confidenceLevel": "88",
                "vehicleListName": "blockList",
                "vehicleInfo": {"color": "red"},
            },
        }
    )
    assert canonical(frame.raw_type) is Kind.ANPR
    assert frame.pictures_expected == 1
    assert frame.plate.number == "W-12345"
    assert frame.plate.confidence == 88
    assert frame.plate.list_name == "block"
    assert frame.plate.vehicle_color == "red"


def test_picture_part_names() -> None:
    stream = picture_part(b"\xff\xd8a\xff\xd9", "licensePlatePicture.jpg") + picture_part(
        b"\xff\xd8b\xff\xd9"
    )
    parts = list(MultipartSplitter("boundary").push(stream))
    assert [p.name for p in parts] == ["licensePlatePicture.jpg", None]
    assert [feed.picture_slot(p.name) for p in parts] == ["plate", "scene"]
    assert isinstance(frame_from_part(parts[0]), bytes)


@pytest.fixture
async def run_feed():
    started: list = []

    async def make(device: FakeDevice):
        server = TestServer(device.app())
        await server.start_server()
        session = aiohttp.ClientSession()
        link = IsapiLink(
            session, Endpoint(server.host, server.port), device.username, device.password
        )
        frames: list = []
        alarm = AlarmFeed(link, frames.append, lambda up, fault: None)
        alarm.start(lambda coro, name: asyncio.get_running_loop().create_task(coro, name=name))
        started.append((alarm, session, server))
        return alarm, frames

    yield make
    for alarm, session, server in started:
        await alarm.stop()
        await session.close()
        await server.close()


async def _until(condition, seconds: float = 3.0) -> None:
    for _ in range(int(seconds / 0.02)):
        if condition():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not reached")


async def test_both_announced_pictures_reach_the_frame(run_feed) -> None:
    plate, scene = b"\xff\xd8plate\xff\xd9", b"\xff\xd8scene\xff\xd9"
    device = FakeDevice(
        stream_parts=[
            xml_part(anpr_xml(when=_now())),
            picture_part(plate, "licensePlatePicture.jpg"),
            picture_part(scene, "detectionPicture.jpg"),
        ],
        stream_hold=1,
        stream_gap=0.1,
    )
    alarm, frames = await run_feed(device)
    await _until(lambda: frames)
    assert frames[0].plate.number == "B AB 1234"
    assert frames[0].pictures == {"plate": plate, "scene": scene}
    assert frames[0].picture == plate
    assert alarm.stats.pictures == 2


async def test_missing_pictures_do_not_hold_the_frame_forever(run_feed, monkeypatch) -> None:
    monkeypatch.setattr(feed, "PICTURE_WAIT", 0.1)
    device = FakeDevice(stream_parts=[xml_part(anpr_xml(when=_now()))], stream_hold=1)
    _, frames = await run_feed(device)
    await _until(lambda: frames, seconds=0.8)
    assert frames[0].pictures == {}
