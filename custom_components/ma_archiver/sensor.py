"""Sensor platform for Music Assistant Archiver."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .archiver import MusicAssistantArchiver
from .const import DOMAIN, build_device_info


def _truncate_state(value: str, limit: int = 250) -> str:
    """Truncate a sensor state string so it never exceeds HA's 255-character state limit."""
    return value if len(value) <= limit else f"{value[: limit - 1]}…"


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
            MAArchiverCurrentTrackSensor(archiver, entry.entry_id),
            MAArchiverCurrentAlbumSensor(archiver, entry.entry_id),
            MAArchiverRunProgressSensor(archiver, entry.entry_id),
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
        self._attr_device_info = build_device_info(entry_id)

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

    _attr_name = "Status"
    _attr_icon = "mdi:music-box-multiple"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize status sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_status"

    @property
    def native_value(self) -> str:
        """Return the state of the sensor (`idle`, `archiving`, `refreshing_library`, `refreshing_metadata`)."""
        return self._archiver.status

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {
            "operation": self._archiver.current_operation,
            "current_track": self._archiver.current_track,
            "current_title": self._archiver.current_title,
            "current_artist": self._archiver.current_artist,
            "current_album": self._archiver.current_album,
            "current_year": self._archiver.current_year,
            "current_uri": self._archiver.current_uri,
            "current_source_provider": self._archiver.current_source_provider,
            "current_target_path": self._archiver.current_target_path,
            "current_cover_url": self._archiver.current_cover_url,
            "progress_completed": self._archiver.progress_completed,
            "progress_total": self._archiver.progress_total,
            "last_archived_track": self._archiver.last_archived_track,
            "last_archived_title": self._archiver.last_archived_title,
            "last_archived_artist": self._archiver.last_archived_artist,
            "last_archived_album": self._archiver.last_archived_album,
            "last_archived_path": self._archiver.last_archived_path,
            "last_archived_cover_url": self._archiver.last_archived_cover_url,
            "last_error": self._archiver.last_error,
            "archived_this_session": self._archiver.archived_session_count,
            "refreshed_this_session": self._archiver.refreshed_session_count,
            "metadata_updated_this_session": self._archiver.metadata_updated_session_count,
            "total_library_tracks": self._archiver.total_library_tracks,
            "archived_library_tracks": self._archiver.archived_library_tracks,
            "unarchived_library_tracks": self._archiver.unarchived_library_tracks,
            "output_dir": self._archiver.output_dir,
            "audio_format": self._archiver.audio_format,
        }


class MAArchiverCurrentTrackSensor(_MAArchiverBaseSensor):
    """Sensor showing the track currently being archived or refreshed (with album art thumbnail)."""

    _attr_name = "Current Track"
    _attr_icon = "mdi:music-note"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize current track sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_current_track"

    @property
    def native_value(self) -> str:
        """Return `<Title> — <Artist>` while active, or `Idle`."""
        if self._archiver.current_title:
            artist = self._archiver.current_artist or "Unknown Artist"
            return _truncate_state(f"{self._archiver.current_title} — {artist}")
        return "Idle"

    @property
    def entity_picture(self) -> str | None:
        """Return the current track's album cover thumbnail URL while active."""
        if self._archiver.current_title and self._archiver.current_cover_url:
            return self._archiver.current_cover_url
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return rich structured attributes for the active and last archived track."""
        total = self._archiver.progress_total
        completed = self._archiver.progress_completed
        return {
            "operation": self._archiver.current_operation,
            "title": self._archiver.current_title,
            "artist": self._archiver.current_artist,
            "album": self._archiver.current_album,
            "year": self._archiver.current_year,
            "uri": self._archiver.current_uri,
            "source_provider": self._archiver.current_source_provider,
            "target_path": self._archiver.current_target_path,
            "cover_url": self._archiver.current_cover_url,
            "progress": f"{completed} / {total}" if total > 0 else None,
            "last_archived_title": self._archiver.last_archived_title,
            "last_archived_artist": self._archiver.last_archived_artist,
            "last_archived_album": self._archiver.last_archived_album,
            "last_archived_path": self._archiver.last_archived_path,
            "last_archived_cover_url": self._archiver.last_archived_cover_url,
        }


class MAArchiverCurrentAlbumSensor(_MAArchiverBaseSensor):
    """Sensor showing the album of the track currently being archived or refreshed."""

    _attr_name = "Current Album"
    _attr_icon = "mdi:album"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize current album sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_current_album"

    @property
    def native_value(self) -> str:
        """Return `<Album> (<Year>)` while a track is active, or `Idle`."""
        if not self._archiver.current_title:
            return "Idle"
        album = self._archiver.current_album
        if not album:
            return "Unknown Album"
        if self._archiver.current_year:
            return _truncate_state(f"{album} ({self._archiver.current_year})")
        return _truncate_state(album)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return album attributes."""
        return {
            "album": self._archiver.current_album,
            "year": self._archiver.current_year,
            "artist": self._archiver.current_artist,
            "last_archived_album": self._archiver.last_archived_album,
        }


class MAArchiverRunProgressSensor(_MAArchiverBaseSensor):
    """Sensor showing percentage progress of the active archive or refresh run."""

    _attr_name = "Run Progress"
    _attr_icon = "mdi:progress-download"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = "%"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize run progress sensor."""
        super().__init__(archiver, entry_id)
        self._attr_unique_id = f"{entry_id}_run_progress"

    @property
    def native_value(self) -> float:
        """Return percentage completion of the active run (`0.0` when idle)."""
        total = self._archiver.progress_total
        if total <= 0:
            return 0.0
        return round(min(100.0, (self._archiver.progress_completed / total) * 100.0), 1)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return completed and total track counts for the active run."""
        total = self._archiver.progress_total
        completed = self._archiver.progress_completed
        return {
            "operation": self._archiver.current_operation,
            "completed": completed,
            "total": total,
            "progress": f"{completed} / {total}" if total > 0 else "Idle",
        }


class MAArchiverRemainingSensor(_MAArchiverBaseSensor):
    """Sensor showing number of library tracks not yet archived locally."""

    _attr_name = "Unarchived Tracks"
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

    _attr_name = "Archived Tracks"
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
