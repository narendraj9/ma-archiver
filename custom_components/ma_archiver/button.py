"""Button platform for Music Assistant Archiver."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .archiver import MusicAssistantArchiver
from .const import DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Music Assistant Archiver buttons from a config entry."""
    archiver: MusicAssistantArchiver = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [
            MAArchiverBatchButton(archiver, entry.entry_id),
            MAArchiverRefreshLibraryButton(archiver, entry.entry_id),
        ]
    )


class MAArchiverBatchButton(ButtonEntity):
    """Button to trigger an immediate archive batch run."""

    _attr_has_entity_name = True
    _attr_name = "MA Archiver Archive Batch Now"
    _attr_icon = "mdi:download-multiple"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize the button."""
        self._archiver = archiver
        self._attr_unique_id = f"{entry_id}_archive_batch_now"

    async def async_press(self) -> None:
        """Handle button press."""
        self._archiver.spawn_task(
            self._archiver.async_archive_batch(), "ma_archiver_batch"
        )


class MAArchiverRefreshLibraryButton(ButtonEntity):
    """Button to search active providers and link substitutes for unplayable library tracks."""

    _attr_has_entity_name = True
    _attr_name = "MA Archiver Refresh Library Items"
    _attr_icon = "mdi:refresh-auto"

    def __init__(self, archiver: MusicAssistantArchiver, entry_id: str) -> None:
        """Initialize the button."""
        self._archiver = archiver
        self._attr_unique_id = f"{entry_id}_refresh_library_items"

    async def async_press(self) -> None:
        """Handle button press."""
        self._archiver.spawn_task(
            self._archiver.async_refresh_library_items(), "ma_archiver_refresh"
        )
