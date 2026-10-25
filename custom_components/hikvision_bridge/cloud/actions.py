"""Read and act through a logged-in Hik-Connect account."""

from __future__ import annotations

import asyncio

from .account import CloudAccount, CloudError
from .catalog import (
    MAX_PAGES,
    PAGE_PATH,
    CloudArea,
    CloudDevice,
    page_params,
    parse_areas,
    parse_page,
)

# Not yet verified against a real door station.
UNLOCK_PATH = "/v3/devconfig/v1/call/{serial}/{channel}/remote/unlock"
AREA_LIST_PATH = "/v3/devices/group/{serial}/list"
AREA_MEMBER_PATH = "/v3/devices/group/{serial}/{group_id}"
AREA_MODE_PATH = "/v3/devices/group/{serial}/switchDefenceMode"
AREA_CREATE_PATH = "/v3/devices/group/{serial}"


async def fetch_areas(account: CloudAccount, serial: str) -> list[CloudArea]:
    """Fetch all security areas configured for a device serial."""
    try:
        payload = await account.request("GET", AREA_LIST_PATH.format(serial=serial))
    except CloudError:
        return []
    base_areas = parse_areas(payload, serial)
    if not base_areas:
        return []

    async def _fill(area: CloudArea) -> CloudArea:
        try:
            mem_payload = await account.request(
                "GET", AREA_MEMBER_PATH.format(serial=serial, group_id=area.group_id)
            )
            raw_mems = mem_payload.get("list") or []
            cam_ids = tuple(
                str(m["memberId"]) for m in raw_mems if isinstance(m, dict) and m.get("memberId")
            )
            return CloudArea(
                group_id=area.group_id,
                device_serial=area.device_serial,
                name=area.name,
                mode=area.mode,
                camera_ids=cam_ids,
            )
        except CloudError:
            return area

    return list(await asyncio.gather(*(_fill(a) for a in base_areas)))


async def set_area_defence_mode(
    account: CloudAccount, serial: str, group_id: int, mode: int
) -> None:
    """Set area defence mode (0: disarmed, 1: armed, 2: armed-silent)."""
    await account.request(
        "POST",
        AREA_MODE_PATH.format(serial=serial),
        json_data={"groupId": group_id, "mode": mode},
    )


async def create_area(
    account: CloudAccount, serial: str, name: str, resource_ids: list[str]
) -> int:
    """Create a new area on a device, returning the new group_id."""
    payload = await account.request(
        "POST",
        AREA_CREATE_PATH.format(serial=serial),
        json_data={"groupName": name, "resourceIds": resource_ids},
    )
    info = payload.get("groupInfo") or {}
    if not isinstance(info, dict) or "groupId" not in info:
        raise CloudError("cloud_rejected", "missing groupInfo in create response")
    return int(info["groupId"])


async def delete_area(account: CloudAccount, serial: str, group_id: int) -> None:
    """Delete an area on a device."""
    await account.request("DELETE", AREA_MEMBER_PATH.format(serial=serial, group_id=group_id))


async def update_area(
    account: CloudAccount, serial: str, group_id: int, name: str, resource_ids: list[str]
) -> int:
    """Update an area by recreating it with the new members/name."""
    await delete_area(account, serial, group_id)
    return await create_area(account, serial, name, resource_ids)


async def fetch_devices(account: CloudAccount) -> dict[str, CloudDevice]:
    """All devices of the account, keyed by their normalised serial."""
    found: dict[str, CloudDevice] = {}
    offset = 0
    for _page in range(MAX_PAGES):
        payload = await account.request("GET", PAGE_PATH, params=page_params(offset))
        devices, more = parse_page(payload)
        for device in devices:
            found[device.key] = device
        if not more or not devices:
            break
        offset += len(devices)

    for key, device in list(found.items()):
        if not device.is_intercom:
            areas = await fetch_areas(account, device.serial)
            if areas:
                found[key] = device.with_areas(tuple(areas))

    return found


async def open_door_relay(account: CloudAccount, device: CloudDevice, lock_id: int = 0) -> None:
    """Open the door relay of an intercom through the cloud (one request, no retry)."""
    await account.request(
        "PUT",
        UNLOCK_PATH.format(serial=device.serial, channel=1),
        params={"srcId": "1", "lockId": str(lock_id), "userType": "0"},
    )
