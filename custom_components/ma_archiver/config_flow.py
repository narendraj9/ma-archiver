"""Config flow for the Music Assistant Archiver integration."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, OptionsFlow
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    AUDIO_FORMATS,
    CONF_ARCHIVE_ON_PLAY,
    CONF_AUDIO_FORMAT,
    CONF_BATCH_SIZE,
    CONF_BITRATE,
    CONF_OUTPUT_DIR,
    CONF_PERIODIC_SCAN,
    CONF_SCAN_INTERVAL_MINUTES,
    DEFAULT_ARCHIVE_ON_PLAY,
    DEFAULT_AUDIO_FORMAT,
    DEFAULT_BATCH_SIZE,
    DEFAULT_BITRATE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PERIODIC_SCAN,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DOMAIN,
)


def _get_schema(defaults: dict[str, Any]) -> vol.Schema:
    """Build the voluptuous schema for user/options forms."""
    return vol.Schema(
        {
            vol.Required(
                CONF_OUTPUT_DIR,
                default=defaults.get(CONF_OUTPUT_DIR, DEFAULT_OUTPUT_DIR),
            ): str,
            vol.Required(
                CONF_AUDIO_FORMAT,
                default=defaults.get(CONF_AUDIO_FORMAT, DEFAULT_AUDIO_FORMAT),
            ): vol.In(AUDIO_FORMATS),
            vol.Required(
                CONF_BITRATE,
                default=defaults.get(CONF_BITRATE, DEFAULT_BITRATE),
            ): str,
            vol.Required(
                CONF_ARCHIVE_ON_PLAY,
                default=defaults.get(CONF_ARCHIVE_ON_PLAY, DEFAULT_ARCHIVE_ON_PLAY),
            ): bool,
            vol.Required(
                CONF_PERIODIC_SCAN,
                default=defaults.get(CONF_PERIODIC_SCAN, DEFAULT_PERIODIC_SCAN),
            ): bool,
            vol.Required(
                CONF_SCAN_INTERVAL_MINUTES,
                default=defaults.get(
                    CONF_SCAN_INTERVAL_MINUTES, DEFAULT_SCAN_INTERVAL_MINUTES
                ),
            ): vol.All(vol.Coerce(int), vol.Range(min=1, max=1440)),
            vol.Required(
                CONF_BATCH_SIZE,
                default=defaults.get(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE),
            ): vol.All(vol.Coerce(int), vol.Range(min=1, max=100)),
        }
    )


class MAArchiverConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Music Assistant Archiver."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()

        if user_input is not None:
            return self.async_create_entry(
                title="Music Assistant Archiver",
                data=user_input,
            )

        return self.async_show_form(
            step_id="user",
            data_schema=_get_schema({}),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Return the options flow handler."""
        return MAArchiverOptionsFlow(config_entry)


class MAArchiverOptionsFlow(OptionsFlow):
    """Handle options flow for Music Assistant Archiver."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize options flow."""
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current = {**self._config_entry.data, **self._config_entry.options}
        return self.async_show_form(
            step_id="init",
            data_schema=_get_schema(current),
        )
