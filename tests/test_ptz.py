"""PTZ buttons and preset select of a camera."""

from __future__ import annotations

from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from tests.conftest import device_entry
from tests.fakedevice import FakeDevice
from tests.model.test_ptz import CAPS, PARK, PRESETS
from tests.test_status_entities import _entity


async def test_ptz_entities(hass: HomeAssistant, serve) -> None:
    device = FakeDevice("camera_g3")
    device.documents.update(
        {
            "/ISAPI/PTZCtrl/channels/1/capabilities": CAPS,
            "/ISAPI/PTZCtrl/channels/1/presets": PRESETS,
        }
    )
    server = await serve(device)
    entry = device_entry(server, device)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    preset = _entity(hass, entry, "select", "_ptz_preset")
    state = hass.states.get(preset.entity_id)
    assert state.attributes["options"] == ["Gate", "Gate (2)", "Preset 3"]
    device.writes.clear()
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": preset.entity_id, "option": "Gate (2)"},
        blocking=True,
    )
    assert device.writes == [("PUT", "/ISAPI/PTZCtrl/channels/1/presets/2/goto", "")]

    left = _entity(hass, entry, "button", "_ptz_pan_left")
    device.writes.clear()
    await hass.services.async_call("button", "press", {"entity_id": left.entity_id}, blocking=True)
    assert [(m, p) for m, p, _ in device.writes] == [
        ("PUT", "/ISAPI/PTZCtrl/channels/1/continuous"),
        ("PUT", "/ISAPI/PTZCtrl/channels/1/continuous"),
    ]
    assert "<pan>-50</pan>" in device.writes[0][2]
    assert "<pan>0</pan><tilt>0</tilt><zoom>0</zoom>" in device.writes[1][2]

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_fixed_camera_has_no_ptz(hass: HomeAssistant, serve) -> None:
    device = FakeDevice("camera_g3")
    server = await serve(device)
    entry = device_entry(server, device)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    registry = er.async_get(hass)
    assert not [
        e
        for e in er.async_entries_for_config_entry(registry, entry.entry_id)
        if "_ptz_" in e.unique_id
    ]
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_ptz_buttons_go_when_ptz_is_off(hass: HomeAssistant, serve) -> None:
    """Buttons made from a switched-off serial PTZ are removed once that is recognised."""
    device = FakeDevice("camera_g3")
    device.documents["/ISAPI/PTZCtrl/channels/1/capabilities"] = CAPS
    server = await serve(device)
    entry = device_entry(server, device)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    registry = er.async_get(hass)

    def ptz_buttons() -> list[er.RegistryEntry]:
        return [
            e
            for e in er.async_entries_for_config_entry(registry, entry.entry_id)
            if e.domain == "button" and "_ptz_" in e.unique_id
        ]

    assert len(ptz_buttons()) == 6

    device.documents["/ISAPI/PTZCtrl/channels/1/capabilities"] = CAPS.replace(
        "<ContinuousPanTiltSpace>", "<enabled>false</enabled><ContinuousPanTiltSpace>", 1
    )
    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert ptz_buttons() == []
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_park_action_entities(hass: HomeAssistant, serve) -> None:
    device = FakeDevice("camera_g3")
    device.documents["/ISAPI/PTZCtrl/channels/1/parkaction"] = PARK
    server = await serve(device)
    entry = device_entry(server, device)
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    switch = _entity(hass, entry, "switch", "_image_park_action")
    assert hass.states.get(switch.entity_id).state == "off"
    kind = _entity(hass, entry, "select", "_image_park_action_type")
    state = hass.states.get(kind.entity_id)
    assert state.state == "preset"
    assert "patrol" in state.attributes["options"]
    number = _entity(hass, entry, "number", "_image_park_time")
    assert float(hass.states.get(number.entity_id).state) == 30

    device.writes.clear()
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": kind.entity_id, "option": "patrol"},
        blocking=True,
    )
    method, path, body = device.writes[0]
    assert (method, path) == ("PUT", "/ISAPI/PTZCtrl/channels/1/parkaction")
    assert "<ActionType>patrol</ActionType>" in body
    assert "<ActionNum>1</ActionNum>" in body
    assert "<Parktime>30</Parktime>" in body

    action_number = _entity(hass, entry, "select", "_image_park_action_number")
    state = hass.states.get(action_number.entity_id)
    assert state.state == "1"
    assert state.attributes["options"] == [str(n) for n in range(1, 9)]

    # A scan takes no number: the device stores 0 and the select is unavailable.
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": kind.entity_id, "option": "autoscan"},
        blocking=True,
    )
    path = "/ISAPI/PTZCtrl/channels/1/parkaction"
    device.documents[path] = device.documents[path].replace(
        "<ActionNum>1</ActionNum>", "<ActionNum>0</ActionNum>"
    )
    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert hass.states.get(action_number.entity_id).state == "unavailable"

    # Back to a preset: a valid number is sent along instead of the 0 the scan left.
    device.writes.clear()
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": kind.entity_id, "option": "preset"},
        blocking=True,
    )
    body = device.writes[0][2]
    assert "<ActionType>preset</ActionType>" in body
    assert "<ActionNum>1</ActionNum>" in body

    device.writes.clear()
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": action_number.entity_id, "option": "7"},
        blocking=True,
    )
    body = device.writes[0][2]
    assert "<ActionType>preset</ActionType>" in body
    assert "<ActionNum>7</ActionNum>" in body

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
