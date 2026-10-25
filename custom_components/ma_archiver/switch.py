"""Switch platform for Music Assistant Archiver."""

from __future__ import annotations

from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .archiver import MusicAssistantArchiver
from .const import CONF_ARCHIVE_ON_PLAY, DOMAIN


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Music Assistant Archiver switch from a config entry."""
    archiver: MusicAssistantArchiver = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([MAArchiverOnPlaySwitch(hass, entry, archiver)])


class MAArchiverOnPlaySwitch(SwitchEntity):
    """Switch to toggle archiving of songs when played on any speaker."""

    _attr_has_entity_name = True
    _attr_name = "MA Archiver Archive Played Songs"
    _attr_icon = "mdi:play-box-lock-outline"

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        archiver: MusicAssistantArchiver,
    ) -> None:
        """Initialize the switch."""
        self.hass = hass
        self._entry = entry
        self._archiver = archiver
        self._attr_unique_id = f"{entry.entry_id}_archive_on_play"

    @property
    def is_on(self) -> bool:
        """Return True if archive-on-play is enabled."""
        return self._archiver.archive_on_play

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on archive-on-play."""
        self._archiver.archive_on_play = True
        self.async_write_ha_state()
        self.hass.config_entries.async_update_entry(
            self._entry,
            options={**self._entry.options, CONF_ARCHIVE_ON_PLAY: True},
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off archive-on-play (library additions are still always archived)."""
        self._archiver.archive_on_play = False
        self.async_write_ha_state()
        self.hass.config_entries.async_update_entry(
            self._entry,
            options={**self._entry.options, CONF_ARCHIVE_ON_PLAY: False},
        )
