"""Describe Music Assistant Archiver logbook events."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from homeassistant.components.logbook import (
    DOMAIN as LOGBOOK_DOMAIN,
    LOGBOOK_ENTRY_CONTEXT_ID,
    LOGBOOK_ENTRY_ENTITY_ID,
    LOGBOOK_ENTRY_ICON,
    LOGBOOK_ENTRY_MESSAGE,
    LOGBOOK_ENTRY_NAME,
    LazyEventPartialState,
)
from homeassistant.const import ATTR_ENTITY_ID, ATTR_NAME
from homeassistant.core import HomeAssistant, callback

from .const import DOMAIN, EVENT_MA_ARCHIVER_ACTIVITY

OPERATION_ICONS: dict[str, str] = {
    "archived": "mdi:music-box-multiple",
    "relinked": "mdi:link-variant",
    "metadata_updated": "mdi:image-edit",
}


@callback
def _describe_archiver_event(event: LazyEventPartialState) -> dict[str, Any]:
    """Describe a Music Assistant Archiver logbook event."""
    data = event.data
    operation = str(data.get("operation") or "archived")
    return {
        LOGBOOK_ENTRY_NAME: data.get(ATTR_NAME, "MA Archiver"),
        LOGBOOK_ENTRY_MESSAGE: data.get("message", ""),
        LOGBOOK_ENTRY_ICON: OPERATION_ICONS.get(
            operation, "mdi:music-box-multiple"
        ),
        LOGBOOK_ENTRY_ENTITY_ID: data.get(ATTR_ENTITY_ID),
        LOGBOOK_ENTRY_CONTEXT_ID: event.context_id,
    }


@callback
def async_describe_events(
    hass: HomeAssistant,
    async_describe_event: Callable[
        [str, str, Callable[[LazyEventPartialState], dict[str, Any]]], None
    ],
) -> None:
    """Describe Music Assistant Archiver logbook events."""
    async_describe_event(
        DOMAIN, EVENT_MA_ARCHIVER_ACTIVITY, _describe_archiver_event
    )


@callback
def async_register_logbook_if_loaded(hass: HomeAssistant) -> None:
    """Ensure the logbook event descriptor is registered even after a hot reload."""
    logbook_config = hass.data.get(LOGBOOK_DOMAIN)
    external_events = getattr(logbook_config, "external_events", None)
    if isinstance(external_events, dict) and EVENT_MA_ARCHIVER_ACTIVITY not in external_events:
        external_events[EVENT_MA_ARCHIVER_ACTIVITY] = (
            DOMAIN,
            _describe_archiver_event,
        )
