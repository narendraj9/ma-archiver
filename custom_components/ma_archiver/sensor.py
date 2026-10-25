"""Sensor platform for Music Assistant Archiver."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .archiver import MusicAssistantArchiver
from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Music Assistant Archiver sensors from a config entry."""
    archiver: MusicAssistantArchiver = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [
            MAArchiverStatusSensor(archiver, entry.entry_id),
            MAArchiverRemainingSensor(archiver, entry.entry_id),
            MAArchiverArchivedSensor(archiver, entry.entry_id),
        ]
    )


class _MAArchiverBaseSensor(SensorEntity):
    """Base sensor class for Music Assistant Archiver."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize base sensor."""
        self._archiver = archiver
        self._entry_id = entry_id

    async def async_added_to_hass(self) -> None:
        """Subscribe to archiver updates."""
        self.async_on_remove(
            self._archiver.async_add_listener(self._handle_update)
        )

    @callback
    def _handle_update(self) -> None:
        """Write state when archiver updates."""
        self.async_write_ha_state()


class MAArchiverStatusSensor(_MAArchiverBaseSensor):
    """Sensor showing current archiver status and last archived track details."""

    _attr_name = "MA Archiver Status"
    _attr_icon = "mdi:music-box-multiple"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize status sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_status"

    @property
    def native_value(self) -> str:
        """Return the state of the sensor (`idle` or `archiving`)."""
        return self._archiver.status

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {
            "current_track": self._archiver.current_track,
            "last_archived_track": self._archiver.last_archived_track,
            "last_archived_path": self._archiver.last_archived_path,
            "last_error": self._archiver.last_error,
            "archived_this_session": self._archiver.archived_session_count,
            "total_library_tracks": self._archiver.total_library_tracks,
            "archived_library_tracks": self._archiver.archived_library_tracks,
            "unarchived_library_tracks": self._archiver.unarchived_library_tracks,
            "output_dir": self._archiver.output_dir,
            "audio_format": self._archiver.audio_format,
        }


class MAArchiverRemainingSensor(_MAArchiverBaseSensor):
    """Sensor showing number of library tracks not yet archived locally."""

    _attr_name = "MA Archiver Unarchived Tracks"
    _attr_icon = "mdi:cloud-download-outline"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = "tracks"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize remaining sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_unarchived_tracks"

    @property
    def native_value(self) -> int:
        """Return number of unarchived library tracks."""
        return self._archiver.unarchived_library_tracks


class MAArchiverArchivedSensor(_MAArchiverBaseSensor):
    """Sensor showing number of library tracks with a local `/media` copy."""

    _attr_name = "MA Archiver Archived Tracks"
    _attr_icon = "mdi:harddisk"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = "tracks"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize archived sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_archived_tracks"

    @property
    def native_value(self) -> int:
        """Return number of archived library tracks."""
        return self._archiver.archived_library_tracks
