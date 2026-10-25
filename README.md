# Music Assistant Archiver (`ma_archiver`)

A Home Assistant custom integration that automatically archives Music Assistant tracks to local storage (`/media`) so that switching or losing streaming providers never breaks your library.

## How It Works

1. **Virtual Slimproto Archiver Player (`02:00:00:aa:bb:cc`)**:
   Registers a lightweight, hidden virtual Squeezelite player (`MA Archiver`) with Music Assistant's Slimproto server (`port 3483`, up to 192 kHz / 24-bit lossless). Whenever a track needs to be archived, `ma_archiver` plays it to this hidden player, captures the internal `/single/...` lossless audio stream from Music Assistant's streamserver, and writes a properly tagged **lossless `.flac`** file (or optional `.opus` / `.m4a`) via `ffmpeg`.
2. **Three Archiving Triggers**:
   - **Library Additions (Always Active)**: Subscribes to `MEDIA_ITEM_ADDED` / `MEDIA_ITEM_UPDATED`. Any track added to your Music Assistant library without a local file mapping is immediately queued and archived.
   - **On-Play Archiving (Default OFF, Toggleable via Switch)**: Subscribes to playback events across all speakers and all providers (`ytmusic`, `opensubsonic`, `spotify`, `qobuz`, `tidal`, etc.). Disabled by default so ad-hoc playback doesn't fill your disk; can be toggled on/off anytime via `switch.ma_archiver_archive_played_songs` or integration options.
   - **Periodic Background Library Archiving**: Periodically scans `music/tracks/library_items` for tracks that do not yet have a `filesystem_local` provider mapping. If a track only has dead mappings from a previous provider (e.g., disconnected Spotify or Qobuz), `ma_archiver` searches across all active providers (`music/search`), finds the best matching playable substitute, archives the audio, tags it with the original library track's metadata (including `ISRC` and MusicBrainz IDs), and calls `music/library/add_item` so Music Assistant immediately links the local file to the existing library track.

## Entities Provided

- `switch.ma_archiver_archive_played_songs` — Toggle archiving of songs when played on any speaker (songs added to the library are always archived).
- `sensor.ma_archiver_status` — Current status (`idle` / `archiving` / `refreshing_library`), current track, last archived track/path, and library counts.
- `sensor.ma_archiver_archived_tracks` — Count of library tracks that have a local `filesystem_local` copy.
- `sensor.ma_archiver_unarchived_tracks` — Count of library tracks still waiting to be archived locally.
- `button.ma_archiver_archive_batch_now` — Trigger an immediate background batch archive run.
- `button.ma_archiver_refresh_library_items` — Scan library tracks with dead/removed provider mappings, search active providers for substitutes, and re-link them in Music Assistant.

## Services

- `ma_archiver.archive_now` — Immediately archive a batch of unarchived library tracks (optional `limit`).
- `ma_archiver.archive_track` — Archive a specific Music Assistant track `uri` (e.g., `library://track/952`).
- `ma_archiver.refresh_library` — Refresh unplayable library tracks across active providers without waiting for archival (optional `limit`).
- `ma_archiver.reset_archive` — Clear the archive queue/cooldown cache and optionally delete all archived files (`delete_files: true`) to start fresh.
