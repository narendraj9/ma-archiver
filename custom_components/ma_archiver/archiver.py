"""Core archiver coordinator for Music Assistant tracks."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlparse

import aiohttp

from music_assistant_models.enums import EventType, MediaType, PlaybackState

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import ATTR_DEVICE_ID, ATTR_ENTITY_ID, ATTR_NAME
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers import (
    aiohttp_client,
    device_registry as dr,
    entity_registry as er,
)
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    ARCHIVER_PLAYER_ID,
    DEFAULT_ARCHIVE_ON_PLAY,
    DEFAULT_AUDIO_FORMAT,
    DEFAULT_BITRATE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PERIODIC_SCAN,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DEFAULT_SLIMPROTO_PORT,
    DOMAIN,
    EVENT_MA_ARCHIVER_ACTIVITY,
)
from .slimproto_client import VirtualSlimprotoPlayer

if TYPE_CHECKING:
    from music_assistant_client import MusicAssistantClient
    from music_assistant_models.event import MassEvent

_LOGGER = logging.getLogger(__name__)

# Delay before archiving an actively playing track so the real speaker's AudioBuffer
# finishes downloading from the streaming provider first.
ON_PLAY_DELAY_SECONDS = 15.0

# Cooldown before retrying a track that failed to resolve or stream (6 hours)
FAILED_TRACK_COOLDOWN_SECONDS = 6 * 3600

def _sanitize_path_component(value: str, fallback: str = "Unknown") -> str:
    """Sanitize a string for safe use as a directory or file name in /media."""
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", (value or "").strip())
    # Avoid leading dots or underscores because Music Assistant's filesystem_local
    # recursive_iter skips entries starting with '.' or '_'
    cleaned = cleaned.lstrip("._ ").rstrip(". ")
    return cleaned or fallback


def _read_flac_duration(path: Path) -> float | None:
    """Parse FLAC STREAMINFO header (first 42 bytes) and return duration in seconds."""
    info = _inspect_flac_file_sync(path, parse_blocks=False)
    return info["duration"] if info else None


def _inspect_flac_file_sync(
    path: Path, parse_blocks: bool = True
) -> dict[str, Any] | None:
    """
    Parse a FLAC file's STREAMINFO, VORBIS_COMMENT tags, and PICTURE block in pure Python.

    Returns a dict with:
      - `duration`: float | None
      - `tags`: dict[str, str] (lowercase keys)
      - `has_cover_art`: bool
    """
    try:
        with path.open("rb") as f:
            header = f.read(42)
            if len(header) < 42 or header[:4] != b"fLaC":
                return None
            val = int.from_bytes(header[18:26], "big")
            sample_rate = (val >> 44) & 0xFFFFF
            total_samples = val & 0xFFFFFFFFF
            duration = (
                total_samples / sample_rate
                if sample_rate > 0 and total_samples > 0
                else None
            )
            tags: dict[str, str] = {}
            has_cover_art = False

            if not parse_blocks:
                return {
                    "duration": duration,
                    "tags": tags,
                    "has_cover_art": has_cover_art,
                }

            # StreamInfo is the first metadata block (header[4] has is_last and type=0)
            is_last = (header[4] & 0x80) != 0
            first_len = int.from_bytes(header[5:8], "big")
            f.seek(8 + first_len)

            while not is_last:
                bhdr = f.read(4)
                if len(bhdr) < 4:
                    break
                is_last = (bhdr[0] & 0x80) != 0
                btype = bhdr[0] & 0x7F
                blen = int.from_bytes(bhdr[1:4], "big")
                if btype == 4 and 0 < blen <= 1024 * 1024:  # VORBIS_COMMENT
                    body = f.read(blen)
                    if len(body) == blen and blen >= 8:
                        vlen = int.from_bytes(body[:4], "little")
                        pos = 4 + vlen
                        if pos + 4 <= blen:
                            cnt = int.from_bytes(body[pos : pos + 4], "little")
                            pos += 4
                            for _ in range(min(cnt, 500)):
                                if pos + 4 > blen:
                                    break
                                clen = int.from_bytes(body[pos : pos + 4], "little")
                                pos += 4
                                if pos + clen > blen:
                                    break
                                raw_comment = body[pos : pos + clen].decode(
                                    "utf-8", errors="replace"
                                )
                                pos += clen
                                if "=" in raw_comment:
                                    k, v = raw_comment.split("=", 1)
                                    k_low = k.strip().lower()
                                    if k_low in tags and k_low == "artist":
                                        tags[k_low] = f"{tags[k_low]};{v}"
                                    elif k_low not in tags:
                                        tags[k_low] = v
                elif btype == 6 and blen > 32:  # PICTURE
                    has_cover_art = True
                    f.seek(blen, os.SEEK_CUR)
                else:
                    f.seek(blen, os.SEEK_CUR)

            return {
                "duration": duration,
                "tags": tags,
                "has_cover_art": has_cover_art,
            }
    except OSError:
        return None


class MusicAssistantArchiver:
    """
    Coordinates on-play archiving and periodic background library archiving.

    1. **On-Play Archiving**: Listens to Music Assistant queue/player events across ALL
       providers. Whenever any track plays on any speaker and does not yet have a local
       ``filesystem_local`` mapping, queues it for archiving.
    2. **Periodic Library Archiving**: Periodically scans all tracks in the Music Assistant
       library (`music/tracks/library_items`). For tracks missing a ``filesystem_local``
       mapping:
       - Uses the Same Fallback Logic as Music Assistant's "Refresh Item"
         (`MusicController.refresh_item`): checks available provider mappings by priority,
         and if only dead mappings (e.g. disconnected Spotify/Qobuz) exist, searches all
         available providers via `music/search` to find a playable substitute.
       - Streams the audio via the virtual Slimproto player (`02:00:00:aa:bb:cc`),
         transcodes & tags it with the original library track's metadata, saves it to
         `/media/archive/...`, and registers it via `music/library/add_item`.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str | None = None,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        audio_format: str = DEFAULT_AUDIO_FORMAT,
        bitrate: str = DEFAULT_BITRATE,
        archive_on_play: bool = DEFAULT_ARCHIVE_ON_PLAY,
        periodic_scan: bool = DEFAULT_PERIODIC_SCAN,
        scan_interval_minutes: int = DEFAULT_SCAN_INTERVAL_MINUTES,
        slimproto_port: int = DEFAULT_SLIMPROTO_PORT,
    ) -> None:
        """Initialize the Music Assistant Archiver."""
        self.hass = hass
        self.entry_id = entry_id
        self.output_dir = output_dir
        self.audio_format = audio_format
        self.bitrate = bitrate
        self.archive_on_play = archive_on_play
        self.periodic_scan = periodic_scan
        self.scan_interval_minutes = scan_interval_minutes
        self.slimproto_port = slimproto_port

        self._slimproto: VirtualSlimprotoPlayer | None = None
        self._unsub_mass_events: CALLBACK_TYPE | None = None
        self._unsub_timer: CALLBACK_TYPE | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        self._stopped: bool = False
        self._queue: asyncio.Queue[tuple[str, float]] = asyncio.Queue()
        self._queued_uris: set[str] = set()
        self._failed_tracks: dict[str, float] = {}
        # Serializes every individual write to disk (archive or metadata update)
        self._archive_lock = asyncio.Lock()
        # Guards full library runs so a new run never starts while one is active
        self._batch_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._listeners: list[CALLBACK_TYPE] = []

        # Status / telemetry exposed to HA sensors
        self.status: str = "idle"
        self.current_operation: str = "idle"
        self.current_track: str | None = None
        self.current_title: str | None = None
        self.current_artist: str | None = None
        self.current_album: str | None = None
        self.current_year: int | None = None
        self.current_uri: str | None = None
        self.current_source_provider: str | None = None
        self.current_target_path: str | None = None
        self.current_cover_url: str | None = None
        self.progress_completed: int = 0
        self.progress_total: int = 0
        self.last_archived_track: str | None = None
        self.last_archived_title: str | None = None
        self.last_archived_artist: str | None = None
        self.last_archived_album: str | None = None
        self.last_archived_cover_url: str | None = None
        self.last_archived_path: str | None = None
        self.last_error: str | None = None
        self.archived_session_count: int = 0
        self.refreshed_session_count: int = 0
        self.metadata_updated_session_count: int = 0
        self.total_library_tracks: int = 0
        self.archived_library_tracks: int = 0
        self.unarchived_library_tracks: int = 0
        self._filesystem_instance_id: str | None = None
        self._filesystem_base_path: str = "/media"
        self._mass_url: str | None = None

    def spawn_task(self, coro: Any, name: str = "ma_archiver_task") -> asyncio.Task[Any]:
        """Spawn and track a background task so it is cleanly cancelled on reload/stop."""
        task = self.hass.async_create_background_task(coro, name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    @callback
    def async_add_listener(self, update_callback: CALLBACK_TYPE) -> CALLBACK_TYPE:
        """Register a state listener for sensor entities."""
        self._listeners.append(update_callback)

        @callback
        def _remove() -> None:
            if update_callback in self._listeners:
                self._listeners.remove(update_callback)

        return _remove

    @callback
    def _notify_listeners(self) -> None:
        """Notify all registered sensor entities of state changes."""
        for listener in list(self._listeners):
            listener()

    def _get_cover_url(self, track: dict[str, Any]) -> str | None:
        """Return a browser-accessible cover art URL for `entity_picture` and attributes."""
        candidates = self._image_candidates(track)
        # 1. Prefer directly accessible HTTPS/HTTP URLs so browsers and mobile apps
        #    outside the internal Docker network can load the thumbnail directly.
        for img in candidates:
            if img.get("type") not in (None, "thumb"):
                continue
            path = str(img.get("path") or "")
            if path.startswith(("https://", "http://")) and img.get(
                "remotely_accessible", True
            ):
                return path
        # 2. Fall back to Music Assistant's imageproxy endpoint if available.
        if self._mass_url:
            for img in candidates:
                if img.get("type") not in (None, "thumb"):
                    continue
                path = str(img.get("path") or "")
                provider = str(img.get("provider") or "")
                image_id = img.get("proxy_id") or (
                    hashlib.sha256(f"{provider}/{path}".encode()).hexdigest()
                    if provider
                    else None
                )
                if image_id:
                    return (
                        f"{self._mass_url}/imageproxy/{quote(str(image_id))}"
                        "?size=256&fmt=jpeg"
                    )
                if provider:
                    return (
                        f"{self._mass_url}/imageproxy?path={quote(path, safe='')}"
                        f"&provider={quote(provider, safe='')}&size=256&fmt=jpeg"
                    )
        return None

    def _extract_track_display_info(self, track: dict[str, Any]) -> dict[str, Any]:
        """Extract human-friendly title, artist, album, year, and cover URL from a track dict."""
        title = str(track.get("name") or "Unknown Track").strip() or "Unknown Track"
        version = str(track.get("version") or "").strip()
        display_title = f"{title} ({version})" if version else title

        artists = [
            str(a.get("name")).strip()
            for a in (track.get("artists") or [])
            if isinstance(a, dict) and a.get("name")
        ]
        artist = ", ".join(artists) if artists else "Unknown Artist"

        album_obj = track.get("album") if isinstance(track.get("album"), dict) else {}
        album = str(album_obj.get("name") or "").strip() or None
        year_val = album_obj.get("year")
        year: int | None = (
            int(year_val)
            if isinstance(year_val, int)
            or (isinstance(year_val, str) and year_val.isdigit())
            else None
        )
        return {
            "title": display_title,
            "artist": artist,
            "album": album,
            "year": year,
            "cover_url": self._get_cover_url(track),
        }

    def _set_active_track_info(
        self,
        track: dict[str, Any],
        *,
        operation: str,
        uri: str | None = None,
        source_provider: str | None = None,
        target_path: str | None = None,
    ) -> None:
        """Populate live current-track telemetry fields from `track`."""
        info = self._extract_track_display_info(track)
        self.current_operation = operation
        self.current_title = info["title"]
        self.current_artist = info["artist"]
        self.current_album = info["album"]
        self.current_year = info["year"]
        self.current_uri = uri or track.get("uri")
        if source_provider is not None:
            self.current_source_provider = source_provider
        if target_path is not None:
            self.current_target_path = target_path
        if info["cover_url"]:
            self.current_cover_url = info["cover_url"]

    def _clear_active_track_info(self, *, reset_progress: bool = False) -> None:
        """Reset live current-track telemetry fields back to idle."""
        self.current_operation = "idle"
        self.current_track = None
        self.current_title = None
        self.current_artist = None
        self.current_album = None
        self.current_year = None
        self.current_uri = None
        self.current_source_provider = None
        self.current_target_path = None
        self.current_cover_url = None
        if reset_progress:
            self.progress_completed = 0
            self.progress_total = 0

    def _record_activity(
        self,
        operation: str,
        track: dict[str, Any],
        *,
        extra_detail: str | None = None,
    ) -> None:
        """Fire a Logbook / Device Activity event for a completed track action."""
        info = self._extract_track_display_info(track)
        title = info["title"]
        artist = info["artist"]
        album = info["album"]
        album_suffix = f" ({album})" if album else ""

        if operation == "archived":
            message = f"archived '{title}' by {artist}{album_suffix}"
        elif operation == "relinked":
            via = f" via {extra_detail}" if extra_detail else ""
            message = f"relinked '{title}' by {artist}{album_suffix}{via}"
        elif operation == "metadata_updated":
            message = f"updated tags & cover art for '{title}' by {artist}{album_suffix}"
        else:
            message = f"{operation} '{title}' by {artist}{album_suffix}"

        event_data: dict[str, Any] = {
            ATTR_NAME: "MA Archiver",
            "operation": operation,
            "message": message,
            "title": title,
            "artist": artist,
            "album": album,
        }
        if extra_detail:
            event_data["detail"] = extra_detail

        if self.entry_id:
            ent_reg = er.async_get(self.hass)
            entity_id = ent_reg.async_get_entity_id(
                "sensor", DOMAIN, f"{self.entry_id}_current_track"
            ) or ent_reg.async_get_entity_id(
                "sensor", DOMAIN, f"{self.entry_id}_status"
            )
            if entity_id:
                event_data[ATTR_ENTITY_ID] = entity_id
            dev_reg = dr.async_get(self.hass)
            device = dev_reg.async_get_device(identifiers={(DOMAIN, self.entry_id)})
            if device:
                event_data[ATTR_DEVICE_ID] = device.id

        self.hass.bus.async_fire(EVENT_MA_ARCHIVER_ACTIVITY, event_data)

    def _get_mass_client(self) -> tuple[MusicAssistantClient | None, str | None]:
        """Return the active MusicAssistantClient and server URL from Home Assistant."""
        for entry in self.hass.config_entries.async_entries("music_assistant"):
            if entry.state is ConfigEntryState.LOADED and hasattr(entry, "runtime_data"):
                mass = getattr(entry.runtime_data, "mass", None)
                if mass is not None:
                    return mass, entry.data.get("url")
        return None, None

    async def async_start(self) -> None:
        """Start the virtual player, event subscriptions, worker loop, and periodic scan."""
        self._stopped = False
        self._worker_task = self.hass.async_create_background_task(
            self._queue_worker(), "ma_archiver_worker"
        )
        await self._ensure_connected_and_subscribed()

        if self.periodic_scan:
            self._unsub_timer = async_track_time_interval(
                self.hass,
                self._async_periodic_tick,
                timedelta(minutes=max(1, self.scan_interval_minutes)),
            )

        # Schedule an initial library stats refresh after startup
        self.spawn_task(self.async_refresh_library_stats(), "ma_archiver_stats")

    async def async_stop(self) -> None:
        """Stop timers, event listeners, worker task, and virtual Slimproto player."""
        self._stopped = True
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None
        if self._unsub_mass_events:
            self._unsub_mass_events()
            self._unsub_mass_events = None
        tasks_to_cancel = list(self._bg_tasks)
        if self._worker_task:
            tasks_to_cancel.append(self._worker_task)
            self._worker_task = None
        for task in tasks_to_cancel:
            task.cancel()
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        self._bg_tasks.clear()
        if self._slimproto:
            await self._slimproto.stop()
            self._slimproto = None

    async def _ensure_connected_and_subscribed(self) -> MusicAssistantClient | None:
        """Ensure we have a connected MA client, Slimproto virtual player, and event subscription."""
        if self._stopped:
            return None
        mass, mass_url = self._get_mass_client()
        if not mass or not mass_url:
            _LOGGER.debug("Music Assistant integration is not loaded yet")
            return None

        self._mass_url = mass_url.rstrip("/")
        parsed = urlparse(mass_url)
        host = parsed.hostname or "127.0.0.1"

        # Start or reconnect virtual Slimproto player
        if self._slimproto is None:
            self._slimproto = VirtualSlimprotoPlayer(
                host=host,
                port=self.slimproto_port,
                player_id=ARCHIVER_PLAYER_ID,
            )
            await self._slimproto.start()
            if await self._slimproto.wait_connected(timeout=10.0):
                await self._configure_virtual_player(mass)

        # Subscribe to Music Assistant events:
        # - MEDIA_ITEM_ADDED: ALWAYS archive any track newly added to the library
        # - QUEUE_UPDATED / PLAYER_UPDATED: archive on play when enabled
        if self._unsub_mass_events is None:
            event_types = tuple(
                getattr(EventType, name)
                for name in (
                    "MEDIA_ITEM_ADDED",
                    "QUEUE_UPDATED",
                    "QUEUE_ITEMS_UPDATED",
                    "PLAYER_UPDATED",
                )
                if hasattr(EventType, name)
            )
            self._unsub_mass_events = mass.subscribe(
                self._on_mass_event,
                event_types,
            )
            _LOGGER.info(
                "Subscribed to Music Assistant library-add and playback events"
            )

        # Discover filesystem_local provider instance ID if not yet cached
        if not self._filesystem_instance_id:
            await self._discover_filesystem_provider(mass)

        return mass

    async def _configure_virtual_player(self, mass: MusicAssistantClient) -> None:
        """Hide the virtual Slimproto player in Music Assistant UI and Home Assistant."""
        try:
            await asyncio.sleep(1.0)
            await mass.send_command(
                "config/players/save",
                player_id=ARCHIVER_PLAYER_ID,
                values={
                    "hide_in_ui": True,
                    "expose_to_ha": False,
                    "http_profile": "chunked",
                    "output_codec": "flac",
                },
            )
            _LOGGER.debug(
                "Configured virtual player %s (hide_in_ui=True, expose_to_ha=False)",
                ARCHIVER_PLAYER_ID,
            )
        except Exception as err:
            _LOGGER.debug("Could not save virtual player config yet: %s", err)

    async def _discover_filesystem_provider(self, mass: MusicAssistantClient) -> None:
        """Discover the active filesystem_local provider instance in Music Assistant."""
        try:
            providers = await mass.send_command("providers")
            for prov in providers:
                if prov.get("domain") == "filesystem_local" and prov.get("available"):
                    self._filesystem_instance_id = prov["instance_id"]
                    _LOGGER.info(
                        "Discovered Music Assistant filesystem_local provider: %s",
                        self._filesystem_instance_id,
                    )
                    return
        except Exception as err:
            _LOGGER.debug("Failed to query providers from Music Assistant: %s", err)

    @callback
    def _on_mass_event(self, event: MassEvent) -> None:
        """Handle library-add and playback events from Music Assistant."""
        if event.object_id == ARCHIVER_PLAYER_ID:
            return

        data = event.data
        if not data:
            return

        # 1. Always archive any track newly added to the library (regardless of archive_on_play)
        if event.event == EventType.MEDIA_ITEM_ADDED:
            media_type = getattr(data, "media_type", None) or (
                data.get("media_type") if isinstance(data, dict) else None
            )
            if str(media_type).lower() not in ("track", MediaType.TRACK.value):
                return
            mappings = getattr(data, "provider_mappings", None) or (
                data.get("provider_mappings") if isinstance(data, dict) else []
            )
            for pm in mappings or []:
                domain = getattr(pm, "provider_domain", None) or (
                    pm.get("provider_domain") if isinstance(pm, dict) else None
                )
                available = getattr(pm, "available", True) if not isinstance(pm, dict) else pm.get("available", True)
                if domain == "filesystem_local" and available:
                    return
            uri = (
                getattr(data, "uri", None)
                or (data.get("uri") if isinstance(data, dict) else None)
                or (event.object_id if event.object_id and "://track/" in event.object_id else None)
            )
            if uri and "://track/" in uri:
                _LOGGER.info("Track added/updated in library without local copy, queueing archive: %s", uri)
                self.enqueue_track(uri, delay=1.0)
            return

        # 2. Archive on play (only when archive_on_play is enabled)
        if not self.archive_on_play:
            return

        # Extract item URI from queue, queue_item, or player update
        uri: str | None = None

        # Case 1: EventType.QUEUE_ITEM_UPDATED (data is a QueueItem directly)
        media_item_direct = getattr(data, "media_item", None) or (
            data.get("media_item") if isinstance(data, dict) else None
        )
        streamdetails = getattr(data, "streamdetails", None) or (
            data.get("streamdetails") if isinstance(data, dict) else None
        )
        if media_item_direct and streamdetails:
            media_type = getattr(media_item_direct, "media_type", None) or (
                media_item_direct.get("media_type")
                if isinstance(media_item_direct, dict)
                else None
            )
            if str(media_type).lower() in ("track", MediaType.TRACK.value):
                uri = getattr(media_item_direct, "uri", None) or (
                    media_item_direct.get("uri")
                    if isinstance(media_item_direct, dict)
                    else None
                )

        # Case 2: EventType.PLAYER_QUEUE_UPDATED / EventType.PLAYER_UPDATED
        if not uri:
            state = getattr(data, "state", None) or (
                data.get("state") if isinstance(data, dict) else None
            )
            playback_state = getattr(data, "playback_state", None) or (
                data.get("playback_state") if isinstance(data, dict) else None
            )
            active_state = state or playback_state
            if str(active_state).lower() not in (
                PlaybackState.PLAYING.value,
                "playing",
            ):
                return

            current_item = getattr(data, "current_item", None) or (
                data.get("current_item") if isinstance(data, dict) else None
            )
            current_media = getattr(data, "current_media", None) or (
                data.get("current_media") if isinstance(data, dict) else None
            )

            if current_item:
                media_item = getattr(current_item, "media_item", None) or (
                    current_item.get("media_item") if isinstance(current_item, dict) else None
                )
                if media_item:
                    media_type = getattr(media_item, "media_type", None) or (
                        media_item.get("media_type") if isinstance(media_item, dict) else None
                    )
                    if str(media_type).lower() not in ("track", MediaType.TRACK.value):
                        return
                    uri = getattr(media_item, "uri", None) or (
                        media_item.get("uri") if isinstance(media_item, dict) else None
                    )
            elif current_media:
                uri = getattr(current_media, "uri", None) or (
                    current_media.get("uri") if isinstance(current_media, dict) else None
                )

        if not uri or "://track/" not in uri:
            return
        if uri.startswith("filesystem_local"):
            return

        self.enqueue_track(uri, delay=ON_PLAY_DELAY_SECONDS)

    @callback
    def enqueue_track(self, uri: str, delay: float = 0.0) -> None:
        """Queue a track URI for archiving if not already queued."""
        if uri in self._queued_uris:
            return
        self._queued_uris.add(uri)
        ready_at = time.monotonic() + delay
        self._queue.put_nowait((uri, ready_at))
        _LOGGER.debug("Queued track %s for archiving (delay=%.1fs)", uri, delay)

    async def _queue_worker(self) -> None:
        """Background worker that processes queued track URIs sequentially."""
        while True:
            uri, ready_at = await self._queue.get()
            try:
                now = time.monotonic()
                if ready_at > now:
                    await asyncio.sleep(ready_at - now)
                await self.async_archive_uri(uri)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                _LOGGER.warning("Error archiving queued URI %s: %s", uri, err)
            finally:
                self._queued_uris.discard(uri)
                self._queue.task_done()

    async def _async_periodic_tick(self, _now: datetime | None = None) -> None:
        """Periodic timer callback to archive all remaining unarchived library tracks."""
        if not self.periodic_scan or self._stopped:
            return
        if self._batch_lock.locked():
            _LOGGER.debug("Archive run still in progress; skipping periodic tick")
            return
        self.spawn_task(self.async_archive_batch(), "ma_archiver_periodic_batch")

    async def async_refresh_library_stats(self) -> list[ dict[str, Any] ]:
        """Fetch library tracks and update archived/unarchived counts."""
        mass = await self._ensure_connected_and_subscribed()
        if not mass:
            return []

        all_tracks: list[dict[str, Any]] = []
        offset = 0
        page_size = 500
        while True:
            batch = await mass.send_command(
                "music/tracks/library_items",
                limit=page_size,
                offset=offset,
            )
            if not batch:
                break
            all_tracks.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        archived = 0
        unarchived_tracks: list[dict[str, Any]] = []
        for track in all_tracks:
            if await self._verify_or_heal_local_mapping(mass, track):
                archived += 1
            else:
                unarchived_tracks.append(track)

        self.total_library_tracks = len(all_tracks)
        self.archived_library_tracks = archived
        self.unarchived_library_tracks = len(unarchived_tracks)
        self._notify_listeners()
        return unarchived_tracks

    def _check_local_file_sync(
        self, rel_item_id: str, expected_duration: int
    ) -> tuple[bool, float | None]:
        """
        Check on disk whether `/media/<rel_item_id>` exists and is not truncated.

        Returns `(is_valid, actual_duration_if_truncated)`. If a truncated file is
        found on disk, deletes it so it can be cleanly re-archived.
        """
        if not rel_item_id:
            return False, None
        abs_file = Path(self._filesystem_base_path) / rel_item_id
        try:
            if not abs_file.is_file() or abs_file.stat().st_size <= 16384:
                return False, None
        except OSError:
            return False, None

        if abs_file.suffix.lower() == ".flac" and expected_duration > 15:
            actual_dur = _read_flac_duration(abs_file)
            min_dur = max(15.0, float(expected_duration) * 0.80)
            if actual_dur is not None and actual_dur < min_dur:
                with contextlib.suppress(OSError):
                    abs_file.unlink(missing_ok=True)
                return False, actual_dur

        return True, None

    def _has_local_mapping(self, track: dict[str, Any]) -> bool:
        """Return True if the track dict has a valid, non-truncated filesystem_local file."""
        expected_duration = int(track.get("duration") or 0)
        for pm in track.get("provider_mappings", []):
            if pm.get("provider_domain") == "filesystem_local" and pm.get("available", True):
                valid, _ = self._check_local_file_sync(
                    pm.get("item_id") or "", expected_duration
                )
                if valid:
                    return True
        return False

    async def _verify_or_heal_local_mapping(
        self, mass: MusicAssistantClient, track: dict[str, Any]
    ) -> bool:
        """
        Verify that a track's `filesystem_local` mapping exists on disk and is not truncated.

        If a mapping points to a missing or truncated file, deletes the corrupted file
        and removes the stale `filesystem_local` provider mapping in Music Assistant.
        """
        expected_duration = int(track.get("duration") or 0)
        has_valid = False
        for pm in list(track.get("provider_mappings", [])):
            if pm.get("provider_domain") != "filesystem_local":
                continue
            rel_item_id = pm.get("item_id") or ""
            valid, trunc_dur = await asyncio.to_thread(
                self._check_local_file_sync, rel_item_id, expected_duration
            )
            if valid and pm.get("available", True):
                has_valid = True
            else:
                if trunc_dur is not None:
                    _LOGGER.warning(
                        "Removed truncated archive file /media/%s (%.1fs vs expected %ds) for '%s'",
                        rel_item_id,
                        trunc_dur,
                        expected_duration,
                        track.get("name"),
                    )
                if track.get("item_id"):
                    with contextlib.suppress(Exception):
                        await mass.send_command(
                            "music/remove_provider_mapping",
                            media_type="track",
                            db_id=str(track["item_id"]),
                            mapping=pm,
                        )
        return has_valid

    async def async_archive_batch(self) -> int:
        """
        Archive ALL remaining unarchived tracks from the Music Assistant library.

        Keeps going until every eligible track has been attempted once in this run
        (re-scanning the library after each pass to pick up tracks added meanwhile).
        Only one run can be active at a time; additional triggers are ignored.
        """
        if self._stopped:
            return 0
        if self._batch_lock.locked():
            _LOGGER.info("Archive run already in progress; ignoring duplicate trigger")
            return 0

        async with self._batch_lock:
            archived_count = 0
            attempted: set[str] = set()
            self.progress_completed = 0
            self.progress_total = 0
            try:
                while not self._stopped:
                    unarchived = await self.async_refresh_library_stats()
                    now = time.monotonic()
                    pending: list[str] = []
                    for track in unarchived:
                        uri = track.get("uri")
                        if not uri or uri in attempted:
                            continue
                        failed_at = self._failed_tracks.get(uri)
                        if failed_at and (now - failed_at) < FAILED_TRACK_COOLDOWN_SECONDS:
                            continue
                        pending.append(uri)

                    if not pending:
                        break

                    self.progress_total = self.progress_completed + len(pending)
                    self._notify_listeners()
                    _LOGGER.info("Archiving %d remaining library tracks", len(pending))
                    for uri in pending:
                        if self._stopped:
                            break
                        attempted.add(uri)
                        if await self.async_archive_uri(uri, reset_to_idle=False):
                            archived_count += 1
                        self.progress_completed += 1
                        self._notify_listeners()
            finally:
                self.status = "idle"
                self._clear_active_track_info(reset_progress=True)
                if not self._stopped:
                    await self.async_refresh_library_stats()
                else:
                    self._notify_listeners()
            _LOGGER.info(
                "Archive run finished: %d tracks archived, %d still unarchived",
                archived_count,
                self.unarchived_library_tracks,
            )
            return archived_count

    async def async_refresh_library_items(self) -> int:
        """
        Refresh library tracks across active providers and refresh archived file metadata.

        1. For any library track that does not currently have an available mapping on a
           loaded provider (e.g. tracks left over from a removed Spotify/Qobuz provider),
           searches all active providers via `music/search`, finds the best matching
           playable substitute, and calls `music/library/add_item` so Music Assistant
           attaches the active provider's mapping to the existing library track.
        2. For every track with a local archived file, re-writes the file's tags and
           embeds cover art when they are missing or outdated (atomic in-place update,
           audio stream copied losslessly).
        """
        if self._stopped:
            return 0
        if self._refresh_lock.locked():
            _LOGGER.info("Library refresh is already in progress; ignoring duplicate trigger")
            return 0

        async with self._refresh_lock:
            mass = await self._ensure_connected_and_subscribed()
            if not mass:
                return 0

            providers_list = await mass.send_command("providers")
            available_instances = {
                p["instance_id"]
                for p in providers_list
                if p.get("available")
                and p.get("type") == "music"
                and p.get("domain") != "filesystem_local"
            }
            all_available_instances = {
                p["instance_id"]
                for p in providers_list
                if p.get("available") and p.get("type") == "music"
            }

            all_tracks: list[dict[str, Any]] = []
            offset = 0
            page_size = 500
            while True:
                batch = await mass.send_command(
                    "music/tracks/library_items",
                    limit=page_size,
                    offset=offset,
                )
                if not batch:
                    break
                all_tracks.extend(batch)
                if len(batch) < page_size:
                    break
                offset += page_size

            dead_tracks: list[dict[str, Any]] = []
            for track in all_tracks:
                mappings = track.get("provider_mappings", [])
                has_active_mapping = any(
                    pm.get("provider_instance") in all_available_instances
                    and pm.get("available", True)
                    for pm in mappings
                )
                if not has_active_mapping:
                    dead_tracks.append(track)

            total_dead = len(dead_tracks)
            refreshed_count = 0
            metadata_updated = 0
            self.status = "refreshing_library"
            self.current_operation = "relinking"
            self.progress_completed = 0
            self.progress_total = total_dead
            self.current_track = f"Refreshing (0/{total_dead} unplayable tracks)"
            self._notify_listeners()

            try:
                for idx, track in enumerate(dead_tracks, start=1):
                    if self._stopped:
                        break

                    db_id = str(track.get("item_id") or "")
                    if not db_id:
                        self.progress_completed = idx
                        continue

                    self.status = "refreshing_library"
                    self._set_active_track_info(
                        track,
                        operation="relinking",
                        uri=track.get("uri") or f"library://track/{db_id}",
                    )
                    self.current_track = (
                        f"Refreshing ({idx}/{total_dead}, {refreshed_count} linked): "
                        f"{self.current_artist} - {self.current_title}"
                    )
                    self._notify_listeners()

                    playable_uri, resolved_track = await self._resolve_playable_source(
                        mass, track, available_instances=available_instances
                    )
                    if not playable_uri or not isinstance(resolved_track, dict):
                        self.progress_completed = idx
                        continue

                    resolved_mappings = [
                        pm
                        for pm in resolved_track.get("provider_mappings", [])
                        if pm.get("provider_instance") in available_instances
                    ]
                    if not resolved_mappings:
                        self.progress_completed = idx
                        continue

                    try:
                        await self._attach_mappings_to_library_track(
                            mass, track, resolved_mappings
                        )
                        refreshed_count += 1
                        self.refreshed_session_count += 1
                        provider_prefix = (
                            playable_uri.split("://", 1)[0]
                            if "://" in playable_uri
                            else playable_uri
                        )
                        self._record_activity(
                            "relinked",
                            resolved_track or track,
                            extra_detail=provider_prefix,
                        )
                        _LOGGER.info(
                            "Refreshed library track '%s' (library://track/%s) with %s",
                            self.current_title,
                            db_id,
                            playable_uri,
                        )
                    except Exception as err:
                        _LOGGER.warning(
                            "Could not attach provider mapping to library://track/%s (%s): %s",
                            db_id,
                            playable_uri,
                            err,
                        )
                    self.progress_completed = idx

                # Pass 2: refresh tags + embedded cover art on already-archived files
                archived_tracks: list[tuple[dict[str, Any], str]] = []
                for track in all_tracks:
                    for pm in track.get("provider_mappings", []):
                        if (
                            pm.get("provider_domain") == "filesystem_local"
                            and pm.get("available", True)
                            and pm.get("item_id")
                        ):
                            archived_tracks.append((track, pm["item_id"]))
                            break

                total_archived = len(archived_tracks)
                self._clear_active_track_info(reset_progress=True)
                self.status = "refreshing_metadata"
                self.current_operation = "updating_metadata"
                self.progress_total = total_archived
                last_notify_ts = 0.0
                for idx, (track, rel_item_id) in enumerate(archived_tracks, start=1):
                    if self._stopped:
                        break
                    track_title = track.get("name") or "Unknown Track"
                    self.status = "refreshing_metadata"
                    self.current_operation = "updating_metadata"
                    self.progress_completed = idx - 1
                    self.current_track = (
                        f"Checking metadata ({idx}/{total_archived}, "
                        f"{metadata_updated} updated): {track_title}"
                    )
                    now_ts = time.monotonic()
                    if idx == 1 or idx == total_archived or (now_ts - last_notify_ts) >= 1.0:
                        self._notify_listeners()
                        last_notify_ts = now_ts
                    try:
                        if await self._update_existing_file_metadata_and_art(
                            mass, track, rel_item_id
                        ):
                            metadata_updated += 1
                            self.metadata_updated_session_count += 1
                    except asyncio.CancelledError:
                        raise
                    except Exception as err:
                        _LOGGER.warning(
                            "Could not update metadata for /media/%s: %s",
                            rel_item_id,
                            err,
                        )
                    self.progress_completed = idx
            finally:
                self.status = "idle"
                self._clear_active_track_info(reset_progress=True)
                if not self._stopped:
                    with contextlib.suppress(Exception):
                        await self.async_refresh_library_stats()
                else:
                    self._notify_listeners()

            _LOGGER.info(
                "Refreshed %d/%d unplayable library tracks across active providers; "
                "updated metadata/cover art on %d archived files",
                refreshed_count,
                total_dead,
                metadata_updated,
            )
            return refreshed_count

    async def async_reset_archive(self, delete_files: bool = True) -> None:
        """
        Reset the archive state, optionally deleting archived files in `output_dir`.

        Clears the pending queue and failed-track cooldown cache so archival starts fresh.
        """
        # Drain pending queue
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                uri, _ = self._queue.get_nowait()
                self._queued_uris.discard(uri)
                self._queue.task_done()
        self._queued_uris.clear()
        self._failed_tracks.clear()
        self.last_error = None

        if delete_files:
            out_path = Path(self.output_dir)

            def _clear_dir() -> None:
                if not out_path.is_dir():
                    return
                for child in out_path.iterdir():
                    if child.is_dir():
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        child.unlink(missing_ok=True)

            await asyncio.to_thread(_clear_dir)
            _LOGGER.info("Deleted all archived files under %s", self.output_dir)

        await self.async_refresh_library_stats()

    async def async_archive_uri(self, uri: str, *, reset_to_idle: bool = True) -> bool:
        """
        Archive a single track URI (either `library://track/<id>` or `<provider>://track/<id>`).

        Returns True if the track was archived (or was already archived), False on failure.
        """
        if self._stopped:
            return False
        async with self._archive_lock:
            if self._stopped:
                return False
            mass = await self._ensure_connected_and_subscribed()
            if not mass or not self._slimproto:
                return False

            try:
                # 1. Fetch the full track object from Music Assistant
                track = await self._fetch_track(mass, uri)
                if not track:
                    self._failed_tracks[uri] = time.monotonic()
                    return False

                # Check if already archived locally (and verify on-disk file is intact & full duration)
                if await self._verify_or_heal_local_mapping(mass, track):
                    _LOGGER.debug("Track %s (%s) is already archived locally", track.get("name"), uri)
                    return True

                if reset_to_idle:
                    self.progress_completed = 0
                    self.progress_total = 1
                self.status = "archiving"
                self._set_active_track_info(track, operation="archiving", uri=uri)
                self.current_track = f"{self.current_artist} - {self.current_title}"
                self._notify_listeners()

                # 2. Resolve playable URI:
                #    - First let Music Assistant play the track's own URI if it has an active
                #      provider mapping (MA picks the highest quality mapping automatically).
                #    - If it has only dead mappings (or if starting the stream on the existing
                #      mapping fails because the remote item was removed), fall back to
                #      cross-provider search (`music/search`).
                playable_uri, resolved_track = await self._resolve_playable_source(mass, track)
                if not playable_uri:
                    msg = f"No playable source found across any provider for {self.current_track}"
                    _LOGGER.warning(msg)
                    self.last_error = msg
                    self._failed_tracks[uri] = time.monotonic()
                    return False

                try:
                    stream_url = await self._start_virtual_stream(mass, playable_uri)
                except Exception as stream_err:
                    if playable_uri == track.get("uri"):
                        _LOGGER.debug(
                            "Direct playback of %s failed (%s); falling back to cross-provider search",
                            playable_uri,
                            stream_err,
                        )
                        playable_uri, resolved_track = await self._resolve_playable_source(
                            mass, track, force_search=True
                        )
                        if not playable_uri:
                            raise stream_err
                        stream_url = await self._start_virtual_stream(mass, playable_uri)
                    else:
                        raise

                # Keep the library track's metadata, but inherit cover art from resolved_track
                # if the library stub has no images of its own.
                if track.get("provider") == "library":
                    metadata_track = dict(track)
                    if (
                        isinstance(resolved_track, dict)
                        and not self._image_candidates(metadata_track)
                        and self._image_candidates(resolved_track)
                    ):
                        metadata_track["metadata"] = dict(metadata_track.get("metadata") or {})
                        metadata_track["metadata"]["images"] = self._image_candidates(
                            resolved_track
                        )
                else:
                    metadata_track = resolved_track or track

                # 3. Compute target file path in /media/archive/<Artist>/<Album>/<Track>.<ext>
                abs_target_path, rel_media_path = self._build_target_paths(metadata_track)
                source_provider = (
                    playable_uri.split("://", 1)[0]
                    if "://" in playable_uri
                    else None
                )
                self._set_active_track_info(
                    metadata_track,
                    operation="archiving",
                    uri=uri,
                    source_provider=source_provider,
                    target_path=f"/media/{rel_media_path}",
                )
                self._notify_listeners()

                # 4. Transcode/tag stream via ffmpeg
                stream_duration = int(
                    (resolved_track.get("duration") if isinstance(resolved_track, dict) else 0)
                    or track.get("duration")
                    or 300
                )
                await self._transcode_and_tag(
                    stream_url,
                    abs_target_path,
                    metadata_track,
                    expected_duration=stream_duration,
                )

                # Stop the virtual queue cleanly
                try:
                    await self._slimproto.notify_stream_finished()
                    await mass.send_command("player_queues/stop", queue_id=ARCHIVER_PLAYER_ID)
                except Exception:
                    pass

                # 5. Register the newly created local file in Music Assistant's library
                #    `music/library/add_item` parses the file's tags & ISRC/MBID and automatically
                #    attaches the `filesystem_local` provider mapping to the existing library track!
                if not self._filesystem_instance_id:
                    await self._discover_filesystem_provider(mass)

                if self._filesystem_instance_id:
                    fs_uri = f"{self._filesystem_instance_id}://track/{rel_media_path}"
                    linked_db_id: Any = None
                    if track.get("provider") == "library" and track.get("item_id"):
                        try:
                            fs_item = await mass.send_command("music/item_by_uri", uri=fs_uri)
                            fs_mappings = (
                                fs_item.get("provider_mappings", [])
                                if isinstance(fs_item, dict)
                                else []
                            )
                            if fs_mappings:
                                updated_item = await self._attach_mappings_to_library_track(
                                    mass, track, fs_mappings
                                )
                                linked_db_id = (
                                    updated_item.get("item_id")
                                    if isinstance(updated_item, dict)
                                    else track["item_id"]
                                )
                        except Exception as map_err:
                            _LOGGER.debug(
                                "Direct mapping merge failed for %s, falling back to add_item: %s",
                                fs_uri,
                                map_err,
                            )

                    if linked_db_id is None:
                        added_item = await mass.send_command(
                            "music/library/add_item",
                            item=fs_uri,
                        )
                        linked_db_id = (
                            added_item.get("item_id")
                            if isinstance(added_item, dict)
                            else added_item
                        )

                    _LOGGER.info(
                        "Archived and linked '%s' -> /media/%s (library item_id=%s)",
                        self.current_track,
                        rel_media_path,
                        linked_db_id,
                    )
                else:
                    _LOGGER.info(
                        "Archived '%s' -> /media/%s (filesystem_local provider not found for immediate link)",
                        self.current_track,
                        rel_media_path,
                    )

                self.archived_session_count += 1
                self.last_archived_track = self.current_track
                self.last_archived_title = self.current_title
                self.last_archived_artist = self.current_artist
                self.last_archived_album = self.current_album
                self.last_archived_cover_url = self.current_cover_url
                self.last_archived_path = f"/media/{rel_media_path}"
                self.last_error = None
                self._failed_tracks.pop(uri, None)
                self._record_activity(
                    "archived",
                    metadata_track,
                    extra_detail=f"/media/{rel_media_path}",
                )
                return True

            except Exception as err:
                err_detail = f"{type(err).__name__}: {err}" if str(err) else type(err).__name__
                self.last_error = f"{uri}: {err_detail}"
                self._failed_tracks[uri] = time.monotonic()
                _LOGGER.warning("Failed to archive track %s: %s", uri, err_detail)
                return False
            finally:
                if reset_to_idle:
                    self.status = "idle"
                    self._clear_active_track_info(reset_progress=True)
                    self._notify_listeners()

    async def _attach_mappings_to_library_track(
        self,
        mass: MusicAssistantClient,
        track: dict[str, Any],
        new_mappings: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """
        Attach new provider mappings directly onto an existing library track via `music/library/add_item`.

        Home Assistant's Music Assistant integration connects with a `service` role token that has
        `library.write` scope (not `library.manage`), so `music/add_provider_mapping` is rejected.
        Passing a `Track` dict with `provider="builtin"` and combined `provider_mappings` (the
        library track's existing mapping(s) + `new_mappings`) causes Music Assistant's
        `add_item_to_library` to use `full_item = item` directly, match `track["item_id"]` via
        `get_library_item_by_prov_mappings`, and merge `new_mappings` onto the existing library
        track without creating a duplicate or firing `MEDIA_ITEM_ADDED`.
        """
        merged_track = dict(track)
        merged_track["provider"] = "builtin"
        merged_track["media_type"] = "track"
        merged_track["album"] = None
        existing_mappings = list(track.get("provider_mappings", []))
        merged_track["provider_mappings"] = [*existing_mappings, *new_mappings]
        res = await mass.send_command(
            "music/library/add_item",
            item=merged_track,
            overwrite_existing=False,
        )
        return res if isinstance(res, dict) else None

    async def _fetch_track(
        self, mass: MusicAssistantClient, uri: str
    ) -> dict[str, Any] | None:
        """Fetch full track dict by URI."""
        try:
            if uri.startswith("library://track/"):
                item_id = uri.split("library://track/", 1)[1]
                res = await mass.send_command(
                    "music/tracks/get",
                    item_id=item_id,
                    provider_instance_id_or_domain="library",
                )
                return res if isinstance(res, dict) else None
            res = await mass.send_command("music/item_by_uri", uri=uri)
            return res if isinstance(res, dict) else None
        except Exception as err:
            _LOGGER.debug("Could not fetch track for URI %s: %s", uri, err)
            return None

    async def _resolve_playable_source(
        self,
        mass: MusicAssistantClient,
        track: dict[str, Any],
        available_instances: set[str] | None = None,
        force_search: bool = False,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        Resolve a playable URI for a track, mirroring `MusicController.refresh_item`.

        1. If the track has an active provider mapping (and `force_search` is False),
           returns the track's own URI so Music Assistant selects the highest-quality
           mapping automatically.
        2. Otherwise, searches all active providers via `music/search` using
           `Artist - Title` and Title fallback, and picks the best matching playable track.
        """
        if available_instances is None:
            providers_list = await mass.send_command("providers")
            available_instances = {
                p["instance_id"]
                for p in providers_list
                if p.get("available")
                and p.get("type") == "music"
                and p.get("domain") != "filesystem_local"
            }

        # Step 1: If the track has any mapping on an active provider, play the track's own
        # URI (e.g. `library://track/<id>`) and let Music Assistant pick the source. MA's
        # stream resolver orders mappings by `ProviderMapping.quality` (lossless/sample
        # rate/bit depth/bitrate + provider priority) and falls through on failures, so
        # the highest quality available version is archived automatically.
        if not force_search:
            has_active_mapping = any(
                pm.get("provider_instance") in available_instances
                and pm.get("available", True)
                and pm.get("item_id")
                for pm in track.get("provider_mappings", [])
            )
            if has_active_mapping and track.get("uri"):
                return track["uri"], track

        # Step 2: Fallback to Music Assistant's server-side track lookup & search
        track_name = (track.get("name") or "").strip()
        if not track_name:
            return None, None
        artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
        album_obj = track.get("album") if isinstance(track.get("album"), dict) else None
        album_name = album_obj.get("name") if album_obj else None
        track_version = track.get("version") or None

        def _is_usable(cand: Any) -> bool:
            if not isinstance(cand, dict) or not cand.get("uri"):
                return False
            if not cand.get("available", True):
                return False
            return any(
                cm.get("provider_instance") in available_instances
                and cm.get("available", True)
                for cm in cand.get("provider_mappings", [])
            )

        # 2a. Try Music Assistant's built-in `music/track_by_name` matcher first
        for artist_name in (artists or [None]):
            try:
                matched = await mass.send_command(
                    "music/track_by_name",
                    track_name=track_name,
                    artist_name=artist_name,
                    album_name=album_name,
                    track_version=track_version,
                )
            except Exception as err:
                _LOGGER.debug(
                    "music/track_by_name failed for '%s' (%s): %s",
                    track_name,
                    artist_name,
                    err,
                )
                matched = None
            if _is_usable(matched):
                assert isinstance(matched, dict)
                _LOGGER.info(
                    "Found cross-provider substitute via track_by_name for '%s' (%s): %s",
                    track_name,
                    track.get("uri"),
                    matched.get("uri"),
                )
                return matched["uri"], matched

        # 2b. Mirror `MusicController.refresh_item`: search active providers and take the
        #     first available non-library track returned by the providers' search engines.
        queries: list[str] = []
        for artist_name in artists:
            queries.append(f"{artist_name} - {track_name}")
        queries.append(track_name)
        for splitter in (" - ", " (", " [", " • ", " | "):
            if splitter in track_name:
                short_title = track_name.split(splitter, 1)[0].strip()
                if short_title:
                    if artists:
                        queries.append(f"{artists[0]} - {short_title}")
                    queries.append(short_title)

        for query in dict.fromkeys(queries):
            try:
                search_res = await mass.send_command(
                    "music/search",
                    search_query=query,
                    media_types=["track"],
                    limit=20,
                    library_only=False,
                )
            except Exception as err:
                _LOGGER.debug("Search failed for query '%s': %s", query, err)
                continue

            candidates = (
                search_res.get("tracks", []) if isinstance(search_res, dict) else []
            )
            for cand in candidates:
                if (
                    isinstance(cand, dict)
                    and cand.get("provider") != "library"
                    and _is_usable(cand)
                ):
                    _LOGGER.info(
                        "Found cross-provider substitute for '%s' (%s): %s",
                        track_name,
                        track.get("uri"),
                        cand.get("uri"),
                    )
                    return cand["uri"], cand

        return None, None

    def _build_target_paths(self, track: dict[str, Any]) -> tuple[Path, str]:
        """
        Return `(absolute_target_path, relative_path_under_media)` for a track.

        Organizes files as `/media/archive/<Artist>/<Album>/<TrackNumber - ><Title>.<ext>`
        so Music Assistant's `filesystem_local` provider parses clean folder hierarchy.
        """
        artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
        primary_artist = _sanitize_path_component(
            artists[0] if artists else "Unknown Artist", "Unknown Artist"
        )
        album_obj = track.get("album")
        album_name = _sanitize_path_component(
            album_obj.get("name") if isinstance(album_obj, dict) else "Unknown Album",
            "Unknown Album",
        )
        title = track.get("name") or f"Track {track.get('item_id', '')}"
        version = track.get("version") or ""
        if version and version.lower() not in title.lower():
            title_with_version = f"{title} ({version})"
        else:
            title_with_version = title

        safe_title = _sanitize_path_component(title_with_version, "Track")
        track_num = track.get("track_number") or 0
        if isinstance(track_num, int) and track_num > 0:
            filename = f"{track_num:02d} - {safe_title}.{self.audio_format}"
        else:
            filename = f"{safe_title}.{self.audio_format}"

        target_dir = Path(self.output_dir) / primary_artist / album_name
        abs_path = target_dir / filename

        # Compute path relative to /media (the base_path of filesystem_local)
        try:
            rel_path = abs_path.relative_to(self._filesystem_base_path).as_posix()
        except ValueError:
            rel_path = abs_path.as_posix().lstrip("/")

        return abs_path, rel_path

    async def _start_virtual_stream(
        self, mass: MusicAssistantClient, playable_uri: str
    ) -> str:
        """Trigger playback on the virtual Slimproto player and capture the stream URL."""
        assert self._slimproto is not None
        if not self._slimproto.is_connected:
            await self._slimproto.wait_connected(timeout=10.0)

        stream_fut = self._slimproto.prepare_stream_capture()
        await mass.send_command(
            "player_queues/play_media",
            queue_id=ARCHIVER_PLAYER_ID,
            media=playable_uri,
        )
        return await asyncio.wait_for(stream_fut, timeout=25.0)

    @staticmethod
    def _build_metadata_dict(track: dict[str, Any]) -> dict[str, str]:
        """Build the canonical tag dict (lowercase Vorbis-comment keys) for a track."""
        artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
        primary_artist = artists[0] if artists else "Unknown Artist"
        album_obj = track.get("album") if isinstance(track.get("album"), dict) else None
        album_name = (album_obj.get("name") if album_obj else None) or "Unknown Album"

        title = track.get("name") or "Unknown Track"
        version = track.get("version") or ""
        if version and version.lower() not in title.lower():
            title = f"{title} ({version})"

        meta: dict[str, str] = {
            "title": title,
            "artist": ";".join(artists) if artists else primary_artist,
            "album": album_name,
            "album_artist": primary_artist,
        }
        if version:
            meta["version"] = str(version)
        if track.get("track_number"):
            meta["track"] = str(track["track_number"])
        if track.get("disc_number"):
            meta["disc"] = str(track["disc_number"])
        if album_obj and album_obj.get("year"):
            meta["date"] = str(album_obj["year"])
        if track.get("mbid"):
            meta["musicbrainz_trackid"] = str(track["mbid"])

        for ext_entry in track.get("external_ids", []) or []:
            if isinstance(ext_entry, (list, tuple)) and len(ext_entry) == 2:
                ext_type, ext_val = str(ext_entry[0]).lower(), str(ext_entry[1])
                if not ext_val:
                    continue
                if ext_type == "isrc":
                    meta.setdefault("isrc", ext_val)
                elif "recording" in ext_type:
                    meta.setdefault("musicbrainz_trackid", ext_val)
                elif "acoustid" in ext_type:
                    meta.setdefault("acoustid_id", ext_val)
        return meta

    @staticmethod
    def _build_ffmpeg_meta_args(meta: dict[str, str]) -> list[str]:
        """Convert a canonical tag dict into ffmpeg `-metadata` arguments."""
        key_map = {
            "isrc": "ISRC",
            "musicbrainz_trackid": "MUSICBRAINZ_TRACKID",
            "acoustid_id": "ACOUSTID_ID",
        }
        args: list[str] = []
        for key, value in meta.items():
            args += ["-metadata", f"{key_map.get(key, key)}={value}"]
        return args

    @staticmethod
    def _image_candidates(track: dict[str, Any]) -> list[dict[str, Any]]:
        """Collect image dicts for a track, preferring the album's front cover."""
        sources: list[Any] = []
        album_obj = track.get("album") if isinstance(track.get("album"), dict) else {}
        for container in (album_obj, track):
            if not isinstance(container, dict):
                continue
            meta = container.get("metadata") if isinstance(container.get("metadata"), dict) else {}
            sources.extend(meta.get("images") or [])
            sources.extend(container.get("images") or [])
            if container.get("image") and isinstance(container["image"], dict):
                sources.append(container["image"])
        audio_exts = (".flac", ".m4a", ".opus", ".mp3", ".ogg", ".wav", ".aac")
        images: list[dict[str, Any]] = []
        for img in sources:
            if not isinstance(img, dict) or not img.get("path"):
                continue
            prov = str(img.get("provider") or "")
            path = str(img["path"]).lower()
            # Skip self-referential embedded-art pointers to local audio files
            if prov.startswith("filesystem_local") and path.endswith(audio_exts):
                continue
            images.append(img)
        # Front cover ("thumb") first, non-filesystem_local providers before local
        return sorted(
            images,
            key=lambda img: (
                img.get("type") not in (None, "thumb"),
                str(img.get("provider") or "").startswith("filesystem_local"),
            ),
        )

    async def _download_cover_art(
        self, track: dict[str, Any], target_dir: Path
    ) -> Path | None:
        """
        Download the track's cover art into a hidden temp file in `target_dir`.

        Tries Music Assistant's `/imageproxy` first (both the modern `/imageproxy/<id>`
        endpoint and the legacy `/imageproxy?path=...&provider=...` endpoint so it works
        across all Music Assistant versions), then falls back to the remote URL directly.
        Returns None when no image could be fetched. Caller is responsible for deleting
        the returned file.
        """
        urls: list[str] = []
        for img in self._image_candidates(track):
            if img.get("type") not in (None, "thumb"):
                continue
            path = str(img["path"])
            provider = str(img.get("provider") or "")
            if self._mass_url:
                image_id = img.get("proxy_id") or (
                    hashlib.sha256(f"{provider}/{path}".encode()).hexdigest()
                    if provider
                    else None
                )
                if image_id:
                    urls.append(
                        f"{self._mass_url}/imageproxy/{quote(str(image_id))}?size=0&fmt=jpeg"
                    )
                if provider:
                    urls.append(
                        f"{self._mass_url}/imageproxy?path={quote(path, safe='')}"
                        f"&provider={quote(provider, safe='')}&size=0&fmt=jpeg"
                    )
            if path.startswith(("http://", "https://")) and img.get(
                "remotely_accessible", True
            ):
                urls.append(path)
        if not urls:
            return None

        session = aiohttp_client.async_get_clientsession(self.hass)
        for url in dict.fromkeys(urls):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.read()
            except Exception as err:
                _LOGGER.debug("Cover art download failed from %s: %s", url, err)
                continue
            # Accept JPEG, PNG, or WebP (ffmpeg converts WebP to MJPEG when embedding)
            is_jpeg = data[:3] == b"\xff\xd8\xff"
            is_png = data[:8] == b"\x89PNG\r\n\x1a\n"
            is_webp = len(data) > 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP"
            if len(data) < 128 or not (is_jpeg or is_png or is_webp):
                continue
            ext = "png" if is_png else ("webp" if is_webp else "jpg")
            cover_path = target_dir / f".cover_{os.getpid()}_{time.monotonic_ns()}.{ext}"

            def _write(p: Path = cover_path, d: bytes = data) -> None:
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(d)

            await asyncio.to_thread(_write)
            return cover_path
        return None

    def _cover_art_args(self, cover_path: Path | None, audio_format: str) -> tuple[list[str], list[str]]:
        """Return `(extra_input_args, mapping_and_codec_args)` for embedding cover art."""
        if cover_path is None or audio_format not in ("flac", "m4a"):
            return [], ["-map", "0:a"]
        return (
            ["-i", str(cover_path)],
            [
                "-map",
                "0:a",
                "-map",
                "1:v",
                "-c:v",
                "png" if cover_path.suffix == ".png" else "mjpeg",
                "-disposition:v",
                "attached_pic",
                "-metadata:s:v",
                "title=Album cover",
                "-metadata:s:v",
                "comment=Cover (front)",
            ],
        )

    async def _run_ffmpeg(self, cmd: list[str], timeout: float, part_path: Path) -> bytes:
        """Run ffmpeg writing to `part_path`; remove the part file on any failure."""
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
            with contextlib.suppress(OSError):
                await asyncio.to_thread(part_path.unlink, missing_ok=True)
            raise

        exists_and_valid = await asyncio.to_thread(
            lambda: proc.returncode == 0
            and part_path.is_file()
            and part_path.stat().st_size > 16384
        )
        if not exists_and_valid:
            err_text = stderr.decode("utf-8", errors="replace").strip()
            with contextlib.suppress(OSError):
                await asyncio.to_thread(part_path.unlink, missing_ok=True)
            raise RuntimeError(
                f"ffmpeg failed (rc={proc.returncode}): {err_text or 'output file empty'}"
            )
        return stderr

    async def _transcode_and_tag(
        self,
        stream_url: str,
        abs_target_path: Path,
        track: dict[str, Any],
        expected_duration: int = 300,
    ) -> None:
        """
        Stream audio from `stream_url` through `ffmpeg`, write tags and embed cover art.

        Writes to a temporary `.part` file in the same directory first and atomically
        replaces `abs_target_path` (`os.replace`) so neither Music Assistant nor anybody
        else ever observes a partially written or corrupt file.
        """
        await asyncio.to_thread(abs_target_path.parent.mkdir, parents=True, exist_ok=True)
        part_path = abs_target_path.with_name(
            f".{abs_target_path.stem}.part.{self.audio_format}"
        )

        # Build ffmpeg metadata arguments so Music Assistant's `_get_library_item_by_match`
        # matches the local file directly onto the existing library track!
        meta_args = self._build_ffmpeg_meta_args(self._build_metadata_dict(track))

        if self.audio_format == "opus":
            codec_args = ["-c:a", "libopus", "-b:a", self.bitrate]
        elif self.audio_format == "m4a":
            codec_args = ["-c:a", "aac", "-b:a", self.bitrate]
        else:
            # Re-mux/encode lossless FLAC with valid streaminfo MD5 & total_samples header
            codec_args = ["-c:a", "flac"]

        cover_path = await self._download_cover_art(track, abs_target_path.parent)
        try:
            cover_inputs, map_args = self._cover_art_args(cover_path, self.audio_format)
            cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                stream_url,
                *cover_inputs,
                *map_args,
                *codec_args,
                *meta_args,
                str(part_path),
            ]

            # Music Assistant paces output streams at 1.1x playback speed after an initial
            # 60s burst (`PacingProfile.DEFAULT`), so allow `max(300, duration * 1.5 + 120)` seconds.
            stream_timeout = max(300.0, float(expected_duration) * 1.5 + 120.0)
            await self._run_ffmpeg(cmd, stream_timeout, part_path)
        finally:
            if cover_path is not None:
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(cover_path.unlink, missing_ok=True)

        # Verify actual audio duration is not truncated (e.g. from a mid-stream drop)
        actual_duration = await self._probe_audio_duration(part_path)
        if (
            expected_duration > 15
            and actual_duration is not None
            and actual_duration < max(15.0, float(expected_duration) * 0.85)
        ):
            with contextlib.suppress(OSError):
                await asyncio.to_thread(part_path.unlink, missing_ok=True)
            raise RuntimeError(
                f"Truncated audio stream: recorded {actual_duration:.1f}s, expected ~{expected_duration}s"
            )

        await asyncio.to_thread(os.replace, part_path, abs_target_path)

    async def _probe_tags_and_cover(self, path: Path) -> tuple[dict[str, str], bool] | None:
        """Return `(lowercase_tags, has_cover_art)` for an audio file."""
        if path.suffix.lower() == ".flac":
            info = await asyncio.to_thread(_inspect_flac_file_sync, path, True)
            if info is not None:
                return info["tags"], info["has_cover_art"]
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format_tags:stream=codec_type:stream_tags:stream_disposition=attached_pic",
                "-of",
                "json",
                str(path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15.0)
            if proc.returncode != 0:
                return None
            data = json.loads(stdout.decode("utf-8", errors="replace") or "{}")
        except Exception:
            return None
        tags = {
            str(k).lower(): str(v)
            for k, v in ((data.get("format") or {}).get("tags") or {}).items()
        }
        # Ogg/Opus stores comments on the audio stream rather than the container
        for stream in data.get("streams") or []:
            if stream.get("codec_type") == "audio":
                for k, v in (stream.get("tags") or {}).items():
                    tags.setdefault(str(k).lower(), str(v))
        has_cover = any(
            s.get("codec_type") == "video"
            and (s.get("disposition") or {}).get("attached_pic") == 1
            for s in data.get("streams") or []
        )
        return tags, has_cover

    async def _update_existing_file_metadata_and_art(
        self,
        mass: MusicAssistantClient,
        track: dict[str, Any],
        rel_item_id: str,
    ) -> bool:
        """
        Re-tag an already archived file and embed cover art if missing/outdated.

        The audio stream is copied losslessly (`-c:a copy`), output goes to a `.part`
        file in the same directory, is verified, and then atomically replaces the
        original via `os.replace`. Returns True if the file was rewritten.
        """
        abs_file = Path(self._filesystem_base_path) / rel_item_id
        # Only touch files this integration manages
        try:
            abs_file.resolve().relative_to(Path(self.output_dir).resolve())
        except (ValueError, OSError):
            return False
        audio_format = abs_file.suffix.lower().lstrip(".")
        if audio_format not in ("flac", "m4a", "opus"):
            return False
        if not await asyncio.to_thread(abs_file.is_file):
            return False

        probed = await self._probe_tags_and_cover(abs_file)
        if probed is None:
            return False
        current_tags, has_cover = probed

        aliases = {
            "albumartist": "album_artist",
            "album artist": "album_artist",
            "tracknumber": "track",
            "discnumber": "disc",
        }
        normalized_current: dict[str, str] = {}
        for key, value in current_tags.items():
            normalized_current.setdefault(aliases.get(key, key), value)

        def _norm(key: str, value: str) -> str:
            value = value.strip()
            if key in ("track", "disc"):
                value = value.split("/", 1)[0].lstrip("0") or "0"
            return value

        def _are_tags_outdated(candidate_track: dict[str, Any]) -> tuple[bool, dict[str, str]]:
            wanted_dict = self._build_metadata_dict(candidate_track)
            # MP4 can't hold the custom Vorbis-style IDs without extra flags; only compare
            # the standard keys there so we don't rewrite the file on every refresh.
            compare_keys = (
                wanted_dict.keys()
                if audio_format in ("flac", "opus")
                else {"title", "artist", "album", "album_artist", "track", "disc", "date"}
                & wanted_dict.keys()
            )
            outdated = any(
                _norm(k, normalized_current.get(k, "")) != _norm(k, wanted_dict[k])
                for k in compare_keys
            )
            return outdated, wanted_dict

        can_embed_art = audio_format in ("flac", "m4a")
        needs_art = can_embed_art and not has_cover

        # Fast-path skip without extra API calls if cover art is already present and
        # summary tags (plus external IDs when present) already match.
        prelim_outdated, wanted = _are_tags_outdated(track)
        has_ext_ids_in_track = "external_ids" in track
        has_ext_tags_in_file = bool(
            normalized_current.get("isrc") or normalized_current.get("musicbrainz_trackid")
        )
        if not needs_art and not prelim_outdated and (has_ext_ids_in_track or has_ext_tags_in_file):
            return False

        # Hydrate the full library track so album/images/external ids are complete
        full_track = track
        if track.get("item_id"):
            fetched = await self._fetch_track(mass, f"library://track/{track['item_id']}")
            if fetched:
                full_track = fetched

        tags_outdated, wanted = _are_tags_outdated(full_track)
        if not tags_outdated and not needs_art:
            return False

        async with self._archive_lock:
            if self._stopped:
                return False
            cover_path: Path | None = None
            if needs_art:
                # If the library track row has no image candidates, check its library album
                # or first active streaming provider mapping.
                if not self._image_candidates(full_track):
                    album_obj = full_track.get("album")
                    if isinstance(album_obj, dict) and album_obj.get("item_id"):
                        with contextlib.suppress(Exception):
                            alb = await mass.send_command(
                                "music/albums/get",
                                item_id=str(album_obj["item_id"]),
                                provider_instance_id_or_domain=album_obj.get(
                                    "provider", "library"
                                ),
                            )
                            if isinstance(alb, dict) and self._image_candidates(alb):
                                full_track = {**full_track, "album": alb}
                cover_path = await self._download_cover_art(full_track, abs_file.parent)
            if not tags_outdated and cover_path is None:
                # Only missing art, and none available: nothing to do
                return False

            # File is actually being updated: expose full track info + cover art on sensors
            self._set_active_track_info(
                full_track,
                operation="updating_metadata",
                target_path=f"/media/{rel_item_id}",
            )
            self._notify_listeners()

            part_path = abs_file.with_name(f".{abs_file.stem}.part.{audio_format}")
            try:
                if cover_path is not None:
                    cover_inputs, map_args = self._cover_art_args(cover_path, audio_format)
                elif has_cover and can_embed_art:
                    # Keep the existing embedded picture
                    cover_inputs, map_args = [], ["-map", "0:a", "-map", "0:v?", "-c:v", "copy"]
                else:
                    cover_inputs, map_args = [], ["-map", "0:a"]
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-i",
                    str(abs_file),
                    *cover_inputs,
                    *map_args,
                    "-c:a",
                    "copy",
                    # Drop old global tags so stale values never survive
                    "-map_metadata",
                    "-1",
                    *self._build_ffmpeg_meta_args(wanted),
                    str(part_path),
                ]
                await self._run_ffmpeg(cmd, 120.0, part_path)
            finally:
                if cover_path is not None:
                    with contextlib.suppress(OSError):
                        await asyncio.to_thread(cover_path.unlink, missing_ok=True)

            # Verify the rewritten file still holds the full audio before swapping it in
            old_dur = await self._probe_audio_duration(abs_file)
            new_dur = await self._probe_audio_duration(part_path)
            if old_dur is not None and (new_dur is None or abs(new_dur - old_dur) > 1.0):
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(part_path.unlink, missing_ok=True)
                raise RuntimeError(
                    f"Duration mismatch after re-tag ({new_dur} vs {old_dur}); kept original"
                )

            await asyncio.to_thread(os.replace, part_path, abs_file)
            self._record_activity(
                "metadata_updated",
                full_track,
                extra_detail=f"/media/{rel_item_id}",
            )
            _LOGGER.info(
                "Updated metadata%s on /media/%s",
                " and cover art" if cover_path is not None else "",
                rel_item_id,
            )
            return True

    async def _probe_audio_duration(self, path: Path) -> float | None:
        """Return actual audio duration in seconds from FLAC header or ffprobe."""
        if path.suffix.lower() == ".flac":
            dur = await asyncio.to_thread(_read_flac_duration, path)
            if dur is not None:
                return dur
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            if proc.returncode == 0 and stdout:
                return float(stdout.decode("utf-8", errors="replace").strip())
        except Exception:
            pass
        return None
