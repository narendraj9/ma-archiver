# Music Assistant Archiver (`ma_archiver`)

A Home Assistant custom integration that automatically archives Music Assistant tracks to local storage (`/media/...`) and links them back into your Music Assistant library via the `filesystem_local` provider — ensuring that removing or switching streaming providers (Spotify, Qobuz, Tidal, YouTube Music, OpenSubsonic, etc.) never breaks your library or playlists.

---

## Architecture & Code Structure

```text
custom_components/ma_archiver/
├── __init__.py          # Entry setup/unload, options reload listener, and HA service registration
├── archiver.py          # Core coordinator: event listener, queue worker, source resolver, ffmpeg pipeline
├── slimproto_client.py  # Asyncio Slimproto client registering hidden player `02:00:00:aa:bb:cc`
├── config_flow.py       # UI setup & options flow (output dir, format, scan interval)
├── sensor.py            # Status, Archived Tracks, and Unarchived Tracks sensors
├── button.py            # "Archive All Now" and "Refresh Library Items" buttons
├── switch.py            # "Archive Played Songs" live toggle switch
├── const.py             # Domain constants and default configuration values
├── icons.json           # Service icons for the HA UI
├── brand/               # Integration icon/logo shown in Home Assistant (icon.png, icon@2x.png, logo*.png)
└── services.yaml        # Service schemas for Developer Tools -> Actions
```

---

## How the Archival Pipeline Works

When a track URI (`library://track/<id>` or `<provider>://track/<id>`) is selected for archiving, [`MusicAssistantArchiver.async_archive_uri`](custom_components/ma_archiver/archiver.py) executes a 5-stage pipeline under an `asyncio.Lock` (so only one stream runs at a time):

1. **Fetch & Self-Healing Local Check (`_verify_or_heal_local_mapping`)**:
   * Fetches the full track record from Music Assistant.
   * Checks whether the track already has a `filesystem_local` provider mapping **and** verifies that the file on disk (`/media/<item_id>`) actually exists, is `> 16 KB`, and (for `.flac`) has a `STREAMINFO` header duration at least **80%** of the track's expected duration.
   * If a mapped local file is missing or truncated, `ma_archiver` automatically deletes the broken file, removes the stale `filesystem_local` mapping via `music/remove_provider_mapping`, and proceeds to re-archive the track.

2. **Playable Source Resolution (`_resolve_playable_source`)**:
   * **Active Mapping First**: Checks the track's existing `provider_mappings` (excluding `filesystem_local`) on currently loaded providers. If any exists, the track's own URI (e.g. `library://track/<id>`) is played on the virtual player and **Music Assistant itself picks the source**: its stream resolver orders mappings by `ProviderMapping.quality` (lossless/sample rate/bit depth/bitrate + provider priority) and falls back to the next mapping on failure — exactly like normal playback, so the highest quality available version is archived.
   * **Cross-Provider Search Fallback**: If a library track only has dead mappings from a removed provider (e.g., an old Spotify or Qobuz account), it queries Music Assistant's built-in `music/track_by_name` matcher first, and then falls back to `music/search` across all active providers (mirroring `MusicController.refresh_item`).

3. **Virtual Slimproto Stream Capture (`VirtualSlimprotoPlayer`)**:
   * [`slimproto_client.py`](custom_components/ma_archiver/slimproto_client.py) maintains a lightweight TCP connection to Music Assistant's Slimproto server (`port 3483`) as a hidden Squeezelite player (`MA Archiver`, MAC `02:00:00:aa:bb:cc`, configured with `hide_in_ui=True`, `expose_to_ha=False`, `output_codec="flac"`).
   * Calling `player_queues/play_media` on `02:00:00:aa:bb:cc` causes Music Assistant to send a Slimproto `strm` (`s`) frame containing the internal lossless HTTP stream URL (`http://<ma-host>:8097/single/...`).

4. **Atomic Transcode, Tagging & Duration Verification (`_transcode_and_tag`)**:
   * Streams the audio through `ffmpeg` into a hidden temporary file (`.<Track>.part.flac`) under `<output_dir>/<Artist>/<Album>/<TrackNumber - ><Title>.flac`.
   * Embeds the library track's canonical metadata tags (`title`, `artist`, `album`, `album_artist`, `track`, `disc`, `date`, `ISRC`, `MUSICBRAINZ_TRACKID`, `ACOUSTID_ID`).
   * **Embeds cover art** (`flac`/`m4a`): downloads the album's front cover through Music Assistant's `/imageproxy` (falling back to the remote image URL) and embeds it as an attached picture (FLAC `PICTURE` block / MP4 `covr`).
   * **Duration Guard**: Reads the FLAC `STREAMINFO` total-samples header (or `ffprobe` for `.opus`/`.m4a`) and verifies the recorded audio is at least **85%** of `expected_duration`. If the stream dropped early, the `.part` file is deleted and the track enters a 6-hour retry cooldown (`FAILED_TRACK_COOLDOWN_SECONDS`).
   * Atomically replaces the final path with the `.part` file (`os.replace`, same directory) so nobody ever sees an incomplete or corrupt file.

5. **Immediate Library Linking (`music/library/add_item`)**:
   * Calls `music/library/add_item` with `filesystem_local://<instance_id>/track/<relative_path>`.
   * Music Assistant matches the file's tags/ISRC/MBID to your existing library track and attaches the `filesystem_local` provider mapping immediately — no full disk scan required.

---

## When Tracks Are Archived (3 Triggers)

| Trigger | Default | How It Works |
| :--- | :--- | :--- |
| **1. Library Additions** | **Always ON** | Subscribes to `EventType.MEDIA_ITEM_ADDED`. Any track newly added to your Music Assistant library without a local file is queued immediately. |
| **2. Periodic Background Scan** | **ON** | Every **Scan interval** (`scan_interval_minutes`), scans your library and archives **all** remaining unarchived tracks sequentially, re-scanning until nothing is left. If the previous run is still going, the tick is skipped — runs never overlap. |
| **3. On-Play Archiving** | **OFF** *(Switchable)* | Controlled by `switch.ma_archiver_archive_played_songs`. When enabled, any song played on any speaker is queued after a 15-second delay (`ON_PLAY_DELAY_SECONDS`) so speaker buffering finishes first. |

---

## Configuration Options

Configured under **Settings → Devices & Services → Music Assistant Archiver → Configure**:

* **Archive output directory (`output_dir`)**: Path inside `/media` where files are stored (e.g., `/media/MusicArchive`). Organized as `<output_dir>/<Artist>/<Album>/<Track>.flac`.
* **Audio format (`audio_format`)**: `flac` (lossless, default), `opus`, or `m4a`.
* **Bitrate (`bitrate`)**: Target bitrate when using `opus` or `m4a` (default `192k`; ignored for lossless `flac`).
* **Archive tracks when played (`archive_on_play`)**: Initial state for the on-play switch.
* **Periodically scan and archive entire MA library (`periodic_scan`)**: Enable/disable background library scanning.
* **Scan interval (`scan_interval_minutes`)**: Minutes between background archive runs (`1`–`1440`). Each run archives everything that's left; there is no per-run cap.

---

## Entities & Services

### Entities
* **`sensor.ma_archiver_status`**: `idle`, `archiving`, `refreshing_library`, or `refreshing_metadata`. Attributes show `current_track`, `last_archived_track`, `last_archived_path`, `last_error`, `metadata_updated_this_session`, and library counts.
* **`sensor.ma_archiver_archived_tracks`**: Number of library tracks with a verified local copy on disk.
* **`sensor.ma_archiver_unarchived_tracks`**: Number of library tracks still waiting to be archived locally.
* **`switch.ma_archiver_archive_played_songs`**: Live toggle for archiving tracks when played on any speaker.
* **`button.ma_archiver_archive_all_now`**: Immediately archives all remaining unarchived library tracks (ignored if a run is already active).
* **`button.ma_archiver_refresh_library_items`**: Scans all library tracks whose mappings only point to removed providers, searches active providers (`music/search`), and attaches playable substitute mappings (`~1–2s` per track, without waiting for audio archival). Then re-tags every archived file and embeds cover art where tags are outdated or art is missing (audio copied losslessly with `-c:a copy`, written to `.part`, duration-verified, then atomically swapped in).

### Services (Developer Tools → Actions)
* **`ma_archiver.archive_now`**: Archive all remaining unarchived tracks now.
* **`ma_archiver.archive_track`**: Archive a specific track URI immediately (`uri: "library://track/952"`).
* **`ma_archiver.refresh_library`**: Re-link unplayable library tracks across active providers and refresh tags/cover art on archived files.
* **`ma_archiver.reset_archive`**: Drain the queue, clear cooldowns, optionally delete all files in `output_dir` (`delete_files: true`), and automatically strip stale `filesystem_local` mappings from Music Assistant.
