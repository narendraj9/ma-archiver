"""The Music Assistant Archiver integration."""

from __future__ import annotations

import logging

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv

from .archiver import MusicAssistantArchiver
from .const import (
    CONF_ARCHIVE_ON_PLAY,
    CONF_AUDIO_FORMAT,
    CONF_BATCH_SIZE,
    CONF_BITRATE,
    CONF_OUTPUT_DIR,
    CONF_PERIODIC_SCAN,
    CONF_SCAN_INTERVAL_MINUTES,
    CONF_SLIMPROTO_PORT,
    DEFAULT_ARCHIVE_ON_PLAY,
    DEFAULT_AUDIO_FORMAT,
    DEFAULT_BATCH_SIZE,
    DEFAULT_BITRATE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PERIODIC_SCAN,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DEFAULT_SLIMPROTO_PORT,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR, Platform.BUTTON, Platform.SWITCH]

SERVICE_ARCHIVE_NOW = "archive_now"
SERVICE_ARCHIVE_TRACK = "archive_track"
SERVICE_REFRESH_LIBRARY = "refresh_library"
SERVICE_RESET_ARCHIVE = "reset_archive"

SCHEMA_ARCHIVE_NOW = vol.Schema(
    {
        vol.Optional("limit"): vol.All(vol.Coerce(int), vol.Range(min=1, max=500)),
    }
)

SCHEMA_ARCHIVE_TRACK = vol.Schema(
    {
        vol.Required("uri"): cv.string,
    }
)

SCHEMA_REFRESH_LIBRARY = vol.Schema(
    {
        vol.Optional("limit"): vol.All(vol.Coerce(int), vol.Range(min=1, max=5000)),
    }
)

SCHEMA_RESET_ARCHIVE = vol.Schema(
    {
        vol.Optional("delete_files", default=True): cv.boolean,
    }
)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Music Assistant Archiver from a config entry."""
    cfg = {**entry.data, **entry.options}

    archiver = MusicAssistantArchiver(
        hass=hass,
        output_dir=cfg.get(CONF_OUTPUT_DIR, DEFAULT_OUTPUT_DIR),
        audio_format=cfg.get(CONF_AUDIO_FORMAT, DEFAULT_AUDIO_FORMAT),
        bitrate=cfg.get(CONF_BITRATE, DEFAULT_BITRATE),
        archive_on_play=cfg.get(CONF_ARCHIVE_ON_PLAY, DEFAULT_ARCHIVE_ON_PLAY),
        periodic_scan=cfg.get(CONF_PERIODIC_SCAN, DEFAULT_PERIODIC_SCAN),
        scan_interval_minutes=cfg.get(
            CONF_SCAN_INTERVAL_MINUTES, DEFAULT_SCAN_INTERVAL_MINUTES
        ),
        batch_size=cfg.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE),
        slimproto_port=cfg.get(CONF_SLIMPROTO_PORT, DEFAULT_SLIMPROTO_PORT),
    )

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = archiver

    await archiver.async_start()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    def _get_active_archiver() -> MusicAssistantArchiver | None:
        entries = hass.data.get(DOMAIN, {})
        if entry.entry_id in entries:
            return entries[entry.entry_id]
        return next(iter(entries.values()), None)

    async def handle_archive_now(call: ServiceCall) -> None:
        """Handle ma_archiver.archive_now service call."""
        active = _get_active_archiver()
        if active:
            limit = call.data.get("limit")
            active.spawn_task(
                active.async_archive_batch(limit=limit), "ma_archiver_batch"
            )

    async def handle_archive_track(call: ServiceCall) -> None:
        """Handle ma_archiver.archive_track service call."""
        active = _get_active_archiver()
        if active:
            uri = call.data["uri"]
            active.enqueue_track(uri, delay=0.0)

    async def handle_refresh_library(call: ServiceCall) -> None:
        """Handle ma_archiver.refresh_library service call."""
        active = _get_active_archiver()
        if active:
            limit = call.data.get("limit")
            active.spawn_task(
                active.async_refresh_library_items(limit=limit), "ma_archiver_refresh"
            )

    async def handle_reset_archive(call: ServiceCall) -> None:
        """Handle ma_archiver.reset_archive service call."""
        active = _get_active_archiver()
        if active:
            delete_files = call.data.get("delete_files", True)
            await active.async_reset_archive(delete_files=delete_files)

    hass.services.async_register(
        DOMAIN,
        SERVICE_ARCHIVE_NOW,
        handle_archive_now,
        schema=SCHEMA_ARCHIVE_NOW,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_ARCHIVE_TRACK,
        handle_archive_track,
        schema=SCHEMA_ARCHIVE_TRACK,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_REFRESH_LIBRARY,
        handle_refresh_library,
        schema=SCHEMA_REFRESH_LIBRARY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_RESET_ARCHIVE,
        handle_reset_archive,
        schema=SCHEMA_RESET_ARCHIVE,
    )

    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Update or reload integration when options are updated."""
    archiver: MusicAssistantArchiver | None = hass.data.get(DOMAIN, {}).get(
        entry.entry_id
    )
    cfg = {**entry.data, **entry.options}
    if archiver:
        new_output_dir = cfg.get(CONF_OUTPUT_DIR, DEFAULT_OUTPUT_DIR)
        new_format = cfg.get(CONF_AUDIO_FORMAT, DEFAULT_AUDIO_FORMAT)
        new_bitrate = cfg.get(CONF_BITRATE, DEFAULT_BITRATE)
        new_periodic = cfg.get(CONF_PERIODIC_SCAN, DEFAULT_PERIODIC_SCAN)
        new_interval = cfg.get(
            CONF_SCAN_INTERVAL_MINUTES, DEFAULT_SCAN_INTERVAL_MINUTES
        )
        new_batch = cfg.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE)
        if (
            archiver.output_dir == new_output_dir
            and archiver.audio_format == new_format
            and archiver.bitrate == new_bitrate
            and archiver.periodic_scan == new_periodic
            and archiver.scan_interval_minutes == new_interval
            and archiver.batch_size == new_batch
        ):
            archiver.archive_on_play = cfg.get(
                CONF_ARCHIVE_ON_PLAY, DEFAULT_ARCHIVE_ON_PLAY
            )
            return
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        archiver: MusicAssistantArchiver | None = hass.data.get(DOMAIN, {}).pop(
            entry.entry_id, None
        )
        if archiver:
            await archiver.async_stop()
    return unload_ok
