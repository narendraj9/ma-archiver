"""Constants for the Music Assistant Archiver integration."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo

DOMAIN = "ma_archiver"

CONF_OUTPUT_DIR = "output_dir"
CONF_AUDIO_FORMAT = "audio_format"
CONF_BITRATE = "bitrate"
CONF_ARCHIVE_ON_PLAY = "archive_on_play"
CONF_PERIODIC_SCAN = "periodic_scan"
CONF_SCAN_INTERVAL_MINUTES = "scan_interval_minutes"
CONF_SLIMPROTO_PORT = "slimproto_port"

DEFAULT_OUTPUT_DIR = "/media/archive"
DEFAULT_AUDIO_FORMAT = "flac"
DEFAULT_BITRATE = "192k"
DEFAULT_ARCHIVE_ON_PLAY = False
DEFAULT_PERIODIC_SCAN = True
DEFAULT_SCAN_INTERVAL_MINUTES = 5
DEFAULT_SLIMPROTO_PORT = 3483

AUDIO_FORMATS = ["flac", "opus", "m4a"]

# Dedicated virtual Squeezelite player MAC/ID used solely by the archiver
ARCHIVER_PLAYER_ID = "02:00:00:aa:bb:cc"
ARCHIVER_PLAYER_NAME = "MA Archiver"

# Event fired for Home Assistant Logbook / Device Activity feed
EVENT_MA_ARCHIVER_ACTIVITY = "ma_archiver_activity"


def build_device_info(entry_id: str) -> DeviceInfo:
    """Return the unified DeviceInfo for Music Assistant Archiver entities."""
    return DeviceInfo(
        identifiers={(DOMAIN, entry_id)},
        name="MA Archiver",
        manufacturer="Music Assistant",
        model="Library & Playback Archiver",
        entry_type=DeviceEntryType.SERVICE,
    )


