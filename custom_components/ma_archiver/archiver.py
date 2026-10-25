"""Core archiver coordinator for Music Assistant tracks."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import re
import shutil
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from music_assistant_models.enums import EventType, MediaType, PlaybackState

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    ARCHIVER_PLAYER_ID,
    DEFAULT_ARCHIVE_ON_PLAY,
    DEFAULT_AUDIO_FORMAT,
    DEFAULT_BATCH_SIZE,
    DEFAULT_BITRATE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PERIODIC_SCAN,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DEFAULT_SLIMPROTO_PORT,
)
from .slimproto_client import VirtualSlimprotoPlayer

if TYPE_CHECKING:
    from music_assistant_client import MusicAssistantClient
    from music_assistant_models.event import MassEvent
    from music_assistant_models.media_items import Track

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


def _normalize_text(text: str) -> str:
    """Normalize text for loose title/artist comparison."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _read_flac_duration(path: Path) -> float | None:
    """Parse FLAC STREAMINFO header (first 42 bytes) and return duration in seconds."""
    try:
        with path.open("rb") as f:
            header = f.read(42)
        if len(header) < 42 or header[:4] != b"fLaC":
            return None
        val = int.from_bytes(header[18:26], "big")
        sample_rate = (val >> 44) & 0xFFFFF
        total_samples = val & 0xFFFFFFFFF
        if sample_rate <= 0 or total_samples <= 0:
            return None
        return total_samples / sample_rate
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
        output_dir: str = DEFAULT_OUTPUT_DIR,
        audio_format: str = DEFAULT_AUDIO_FORMAT,
        bitrate: str = DEFAULT_BITRATE,
        archive_on_play: bool = DEFAULT_ARCHIVE_ON_PLAY,
        periodic_scan: bool = DEFAULT_PERIODIC_SCAN,
        scan_interval_minutes: int = DEFAULT_SCAN_INTERVAL_MINUTES,
        batch_size: int = DEFAULT_BATCH_SIZE,
        slimproto_port: int = DEFAULT_SLIMPROTO_PORT,
    ) -> None:
        """Initialize the Music Assistant Archiver."""
        self.hass = hass
        self.output_dir = output_dir
        self.audio_format = audio_format
        self.bitrate = bitrate
        self.archive_on_play = archive_on_play
        self.periodic_scan = periodic_scan
        self.scan_interval_minutes = scan_interval_minutes
        self.batch_size = batch_size
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
        self._archive_lock = asyncio.Lock()
        self._listeners: list[CALLBACK_TYPE] = []

        # Status / telemetry exposed to HA sensors
        self.status: str = "idle"
        self.current_track: str | None = None
        self.last_archived_track: str | None = None
        self.last_archived_path: str | None = None
        self.last_error: str | None = None
        self.archived_session_count: int = 0
        self.total_library_tracks: int = 0
        self.archived_library_tracks: int = 0
        self.unarchived_library_tracks: int = 0
        self._filesystem_instance_id: str | None = None
        self._filesystem_base_path: str = "/media"

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
        # - MEDIA_ITEM_ADDED / MEDIA_ITEM_UPDATED: ALWAYS archive any track added to the library
        # - QUEUE_UPDATED / PLAYER_UPDATED: archive on play when enabled
        if self._unsub_mass_events is None:
            event_types = tuple(
                getattr(EventType, name)
                for name in (
                    "MEDIA_ITEM_ADDED",
                    "MEDIA_ITEM_UPDATED",
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

        # 1. Always archive any track added to the library (regardless of archive_on_play)
        if event.event in (EventType.MEDIA_ITEM_ADDED, EventType.MEDIA_ITEM_UPDATED):
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
        """Periodic timer callback to archive a batch of unarchived library tracks."""
        if not self.periodic_scan or self._stopped:
            return
        self.spawn_task(
            self.async_archive_batch(limit=self.batch_size), "ma_archiver_periodic_batch"
        )

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

    async def async_archive_batch(self, limit: int | None = None) -> int:
        """Archive up to `limit` unarchived tracks from the Music Assistant library."""
        if self._stopped:
            return 0
        batch_limit = limit or self.batch_size
        unarchived = await self.async_refresh_library_stats()
        if not unarchived:
            _LOGGER.info("All %d library tracks are already archived locally!", self.total_library_tracks)
            return 0

        now = time.monotonic()
        archived_count = 0
        for track in unarchived:
            if self._stopped or archived_count >= batch_limit:
                break
            uri = track.get("uri")
            if not uri:
                continue
            failed_at = self._failed_tracks.get(uri)
            if failed_at and (now - failed_at) < FAILED_TRACK_COOLDOWN_SECONDS:
                continue
            success = await self.async_archive_uri(uri)
            if success:
                archived_count += 1

        if not self._stopped:
            await self.async_refresh_library_stats()
        return archived_count

    async def async_refresh_library_items(self, limit: int | None = None) -> int:
        """
        Refresh unplayable/dead library tracks across active providers.

        For any library track that does not currently have an available mapping on a
        loaded provider (e.g. tracks left over from a removed Spotify/Qobuz provider),
        searches all active providers via `music/search`, finds the best matching
        playable substitute, and calls `music/library/add_item` so Music Assistant
        attaches the active provider's mapping to the existing library track.
        """
        if self._stopped:
            return 0
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

        refreshed_count = 0
        self.status = "refreshing_library"
        self._notify_listeners()

        try:
            for track in all_tracks:
                if limit is not None and refreshed_count >= limit:
                    break
                mappings = track.get("provider_mappings", [])
                has_active_mapping = any(
                    pm.get("provider_instance") in available_instances
                    and pm.get("available", True)
                    for pm in mappings
                )
                if has_active_mapping:
                    continue

                track_title = track.get("name") or "Unknown Track"
                artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
                artist_display = ", ".join(artists) if artists else "Unknown Artist"
                self.current_track = f"Refreshing: {artist_display} - {track_title}"
                self._notify_listeners()

                playable_uri, _resolved = await self._resolve_playable_source(mass, track)
                if not playable_uri:
                    continue

                try:
                    await mass.send_command(
                        "music/library/add_item",
                        item=playable_uri,
                        overwrite_existing=True,
                    )
                    refreshed_count += 1
                    _LOGGER.info(
                        "Refreshed library track '%s' (%s) with %s",
                        track_title,
                        track.get("uri"),
                        playable_uri,
                    )
                except Exception as err:
                    _LOGGER.debug(
                        "Could not refresh library track %s with %s: %s",
                        track.get("uri"),
                        playable_uri,
                        err,
                    )
        finally:
            self.status = "idle"
            self.current_track = None
            await self.async_refresh_library_stats()

        _LOGGER.info("Refreshed %d unplayable library tracks across active providers", refreshed_count)
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

    async def async_archive_uri(self, uri: str) -> bool:
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

                track_title = track.get("name") or "Unknown Track"
                artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
                artist_display = ", ".join(artists) if artists else "Unknown Artist"
                self.status = "archiving"
                self.current_track = f"{artist_display} - {track_title}"
                self._notify_listeners()

                # 2. Resolve playable URI using the exact "Refresh Item" fallback strategy:
                #    - First check if the track has an active provider mapping on a loaded provider.
                #    - If not (e.g. only dead Spotify/Qobuz mappings), search across all available
                #      providers (`music/search`) to find the best playable substitute.
                playable_uri, resolved_track = await self._resolve_playable_source(mass, track)
                if not playable_uri:
                    msg = f"No playable source found across any provider for {self.current_track}"
                    _LOGGER.warning(msg)
                    self.last_error = msg
                    self._failed_tracks[uri] = time.monotonic()
                    return False

                # If the track was played from a non-library URI (e.g. direct search/radio play),
                # check if it matches an existing library track or keep its metadata.
                metadata_track = track if track.get("provider") == "library" else (resolved_track or track)

                # 3. Compute target file path in /media/archive/<Artist>/<Album>/<Track>.<ext>
                abs_target_path, rel_media_path = self._build_target_paths(metadata_track)

                # 4. Stream via virtual Slimproto player and transcode/tag via ffmpeg
                stream_url = await self._start_virtual_stream(mass, playable_uri)
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
                    added_item = await mass.send_command(
                        "music/library/add_item",
                        item=fs_uri,
                    )
                    _LOGGER.info(
                        "Archived and linked '%s' -> /media/%s (library item_id=%s)",
                        self.current_track,
                        rel_media_path,
                        added_item.get("item_id") if isinstance(added_item, dict) else added_item,
                    )
                else:
                    _LOGGER.info(
                        "Archived '%s' -> /media/%s (filesystem_local provider not found for immediate link)",
                        self.current_track,
                        rel_media_path,
                    )

                self.archived_session_count += 1
                self.last_archived_track = self.current_track
                self.last_archived_path = f"/media/{rel_media_path}"
                self.last_error = None
                self._failed_tracks.pop(uri, None)
                return True

            except Exception as err:
                err_detail = f"{type(err).__name__}: {err}" if str(err) else type(err).__name__
                self.last_error = f"{uri}: {err_detail}"
                self._failed_tracks[uri] = time.monotonic()
                _LOGGER.warning("Failed to archive track %s: %s", uri, err_detail)
                return False
            finally:
                self.status = "idle"
                self.current_track = None
                self._notify_listeners()

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
        self, mass: MusicAssistantClient, track: dict[str, Any]
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        Resolve a playable URI for a track, mirroring `MusicController.refresh_item`.

        1. Checks all active/available provider mappings on the track (sorted by priority/quality).
        2. If none of the track's existing mappings belong to a currently available provider
           (for example, tracks imported from a previous Spotify or Qobuz account), searches
           all active providers via `music/search` using `Artist - Title` and Title fallback,
           and picks the best matching playable track.
        """
        providers_list = await mass.send_command("providers")
        available_instances = {
            p["instance_id"]
            for p in providers_list
            if p.get("available")
            and p.get("type") == "music"
            and p.get("domain") != "filesystem_local"
        }

        # Step 1: Check existing provider mappings on the track
        mappings = sorted(
            track.get("provider_mappings", []),
            key=lambda m: (m.get("priority", 0), m.get("quality", 0)),
            reverse=True,
        )
        for pm in mappings:
            instance_id = pm.get("provider_instance")
            item_id = pm.get("item_id")
            if (
                instance_id in available_instances
                and pm.get("available", True)
                and item_id
            ):
                # Verify the provider can resolve this item
                try:
                    prov_track = await mass.send_command(
                        "music/tracks/get",
                        item_id=item_id,
                        provider_instance_id_or_domain=instance_id,
                    )
                    if isinstance(prov_track, dict) and prov_track.get("uri"):
                        return prov_track["uri"], prov_track
                except Exception:
                    continue

        # Step 2: Fallback to cross-provider search (just like MusicController.refresh_item)
        track_name = track.get("name") or ""
        artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
        queries: list[str] = []
        if artists:
            queries.append(f"{artists[0]} - {track_name}")
            queries.append(f"{artists[0]} {track_name}")
        if track_name:
            queries.append(track_name)

        target_title_norm = _normalize_text(track_name)
        target_artists_norm = [_normalize_text(a) for a in artists if _normalize_text(a)]
        target_duration = track.get("duration") or 0

        for query in dict.fromkeys(queries):
            try:
                search_res = await mass.send_command(
                    "music/search",
                    search_query=query,
                    media_types=["track"],
                    limit=10,
                    library_only=False,
                )
            except Exception as err:
                _LOGGER.debug("Search failed for query '%s': %s", query, err)
                continue

            candidates = search_res.get("tracks", []) if isinstance(search_res, dict) else []
            best_candidate: dict[str, Any] | None = None
            best_score = -1

            for cand in candidates:
                if not isinstance(cand, dict):
                    continue
                if cand.get("provider") == "library" or not cand.get("available", True):
                    continue
                # Must belong to an active provider
                cand_mappings = cand.get("provider_mappings", [])
                if not any(
                    cm.get("provider_instance") in available_instances
                    for cm in cand_mappings
                ):
                    continue

                cand_title_norm = _normalize_text(cand.get("name") or "")
                if not cand_title_norm:
                    continue

                # Score candidate by title match, artist match, and duration proximity
                score = 0
                if cand_title_norm == target_title_norm:
                    score += 50
                elif (
                    target_title_norm in cand_title_norm
                    or cand_title_norm in target_title_norm
                ):
                    score += 30
                else:
                    continue

                cand_artists_norm = [
                    _normalize_text(a.get("name") or "")
                    for a in cand.get("artists", [])
                ]
                if target_artists_norm and any(
                    ta in ca or ca in ta
                    for ta in target_artists_norm
                    for ca in cand_artists_norm
                    if ca
                ):
                    score += 40
                elif target_artists_norm:
                    # Require at least a partial artist match when the library track has artists
                    # unless the artist name is embedded in the candidate's title
                    if not any(ta in cand_title_norm for ta in target_artists_norm):
                        continue
                    score += 20

                cand_duration = cand.get("duration") or 0
                if target_duration and cand_duration:
                    diff = abs(int(target_duration) - int(cand_duration))
                    if diff <= 5:
                        score += 20
                    elif diff <= 15:
                        score += 10
                    elif diff > 60:
                        score -= 20

                if score > best_score and cand.get("uri"):
                    best_score = score
                    best_candidate = cand

            if best_candidate and best_score >= 50:
                _LOGGER.info(
                    "Found cross-provider substitute for '%s' (%s): %s (score=%d)",
                    track_name,
                    track.get("uri"),
                    best_candidate.get("uri"),
                    best_score,
                )
                return best_candidate["uri"], best_candidate

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

    async def _transcode_and_tag(
        self,
        stream_url: str,
        abs_target_path: Path,
        track: dict[str, Any],
        expected_duration: int = 300,
    ) -> None:
        """
        Stream audio from `stream_url` through `ffmpeg` and write metadata tags.

        Writes to a temporary `.part` file first and atomically renames to `abs_target_path`
        so Music Assistant never observes a partially written file.
        """
        await asyncio.to_thread(abs_target_path.parent.mkdir, parents=True, exist_ok=True)
        part_path = abs_target_path.with_name(
            f".{abs_target_path.stem}.part.{self.audio_format}"
        )

        artists = [a.get("name") for a in track.get("artists", []) if a.get("name")]
        primary_artist = artists[0] if artists else "Unknown Artist"
        album_obj = track.get("album") if isinstance(track.get("album"), dict) else None
        album_name = (album_obj.get("name") if album_obj else None) or "Unknown Album"

        title = track.get("name") or "Unknown Track"
        version = track.get("version") or ""
        if version and version.lower() not in title.lower():
            title = f"{title} ({version})"

        # Build ffmpeg metadata arguments so Music Assistant's `_get_library_item_by_match`
        # matches the local file directly onto the existing library track!
        meta_args: list[str] = [
            "-metadata",
            f"title={title}",
            "-metadata",
            f"artist={';'.join(artists) if artists else primary_artist}",
            "-metadata",
            f"album={album_name}",
            "-metadata",
            f"album_artist={primary_artist}",
        ]
        if version:
            meta_args += ["-metadata", f"version={version}"]

        track_number = track.get("track_number")
        if track_number:
            meta_args += ["-metadata", f"track={track_number}"]

        disc_number = track.get("disc_number")
        if disc_number:
            meta_args += ["-metadata", f"disc={disc_number}"]

        if album_obj and album_obj.get("year"):
            meta_args += ["-metadata", f"date={album_obj['year']}"]

        if track.get("mbid"):
            meta_args += ["-metadata", f"MUSICBRAINZ_TRACKID={track['mbid']}"]

        for ext_entry in track.get("external_ids", []):
            if isinstance(ext_entry, (list, tuple)) and len(ext_entry) == 2:
                ext_type, ext_val = str(ext_entry[0]).lower(), str(ext_entry[1])
                if ext_type == "isrc" and ext_val:
                    meta_args += ["-metadata", f"ISRC={ext_val}"]
                elif "recording" in ext_type and ext_val:
                    meta_args += ["-metadata", f"MUSICBRAINZ_TRACKID={ext_val}"]
                elif "acoustid" in ext_type and ext_val:
                    meta_args += ["-metadata", f"ACOUSTID_ID={ext_val}"]

        if self.audio_format == "opus":
            codec_args = ["-c:a", "libopus", "-b:a", self.bitrate]
        elif self.audio_format == "m4a":
            codec_args = ["-c:a", "aac", "-b:a", self.bitrate]
        else:
            # Re-mux/encode lossless FLAC with valid streaminfo MD5 & total_samples header
            codec_args = ["-c:a", "flac"]

        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            stream_url,
            "-vn",
            *codec_args,
            *meta_args,
            str(part_path),
        ]

        # Music Assistant paces output streams at 1.1x playback speed after an initial
        # 60s burst (`PacingProfile.DEFAULT`), so allow `max(300, duration * 1.5 + 120)` seconds.
        stream_timeout = max(300.0, float(expected_duration) * 1.5 + 120.0)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=stream_timeout
            )
        except Exception:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
            with contextlib.suppress(OSError):
                await asyncio.to_thread(part_path.unlink, missing_ok=True)
            raise

        # Verify output file exists, ffmpeg exited cleanly, and has valid audio size (>16 KB)
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
