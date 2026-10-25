"""Minimal asyncio Slimproto client to register a virtual archiver player in Music Assistant."""

from __future__ import annotations

import asyncio
import logging
import struct

from .const import ARCHIVER_PLAYER_ID, ARCHIVER_PLAYER_NAME, DEFAULT_SLIMPROTO_PORT

_LOGGER = logging.getLogger(__name__)

_CAPABILITIES = (
    b"Model=squeezelite,ModelName=SqueezeLite,AccuratePlayPoints=1,"
    b"HasDigitalOut=1,MaxSampleRate=192000,flc,pcm,mp3,ogg,aac"
)


class VirtualSlimprotoPlayer:
    """
    Lightweight Slimproto client that connects to Music Assistant's Squeezelite provider.

    Registers a hidden virtual player (MAC ``02:00:00:aa:bb:cc``) so Music Assistant's
    streamserver generates ``/single/...`` audio streams on demand when the archiver
    calls ``player_queues/play_media`` on ``02:00:00:aa:bb:cc``.
    """

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_SLIMPROTO_PORT,
        player_id: str = ARCHIVER_PLAYER_ID,
        player_name: str = ARCHIVER_PLAYER_NAME,
    ) -> None:
        """Initialize the virtual Slimproto player."""
        self.host = host
        self.port = port
        self.player_id = player_id
        self.player_name = player_name
        self._mac_bytes = bytes.fromhex(player_id.replace(":", ""))
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._connected = asyncio.Event()
        self._stream_waiter: asyncio.Future[str] | None = None

    @property
    def is_connected(self) -> bool:
        """Return True if currently connected to the Slimproto server."""
        return self._connected.is_set()

    async def start(self) -> None:
        """Start the persistent Slimproto connection loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._connection_loop())

    async def stop(self) -> None:
        """Stop the Slimproto connection loop and close the socket."""
        self._running = False
        self._connected.clear()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._close_writer()

    async def wait_connected(self, timeout: float = 15.0) -> bool:
        """Wait until the virtual player is connected to Music Assistant."""
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    def prepare_stream_capture(self) -> asyncio.Future[str]:
        """
        Create and return a Future that resolves to the next HTTP stream URL.

        Call this immediately before invoking ``player_queues/play_media`` on
        ``self.player_id``, then await the returned Future.
        """
        if self._stream_waiter and not self._stream_waiter.done():
            self._stream_waiter.cancel()
        loop = asyncio.get_running_loop()
        self._stream_waiter = loop.create_future()
        return self._stream_waiter

    async def notify_stream_finished(self) -> None:
        """Notify the Slimproto server that the current stream has completed."""
        await self._send_stat(b"STMd")
        await self._send_stat(b"STMu")

    async def _connection_loop(self) -> None:
        """Maintain connection to the Slimproto server with automatic reconnect."""
        while self._running:
            try:
                _LOGGER.debug(
                    "Connecting virtual Slimproto player %s to %s:%s",
                    self.player_id,
                    self.host,
                    self.port,
                )
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port
                )
                await self._send_helo()
                self._connected.set()
                _LOGGER.info(
                    "Virtual Slimproto player %s (%s) connected to %s:%s",
                    self.player_name,
                    self.player_id,
                    self.host,
                    self.port,
                )
                await self._read_loop()
            except asyncio.CancelledError:
                raise
            except Exception as err:
                _LOGGER.debug("Slimproto connection error: %s", err)
            finally:
                self._connected.clear()
                await self._close_writer()

            if self._running:
                await asyncio.sleep(5.0)

    async def _read_loop(self) -> None:
        """Read incoming Slimproto frames from the server."""
        assert self._reader is not None
        while self._running:
            hdr = await self._reader.readexactly(2)
            length = struct.unpack("!H", hdr)[0]
            data = await self._reader.readexactly(length)
            cmd = data[:4]
            payload = data[4:]

            if cmd == b"setd" and payload == b"\x00":
                # Server requested player display name (id=0)
                name_payload = b"\x00" + self.player_name.encode("utf-8") + b"\x00"
                await self._send_frame(b"SETD", name_payload)
            elif cmd == b"strm" and len(payload) >= 24:
                subcmd = payload[:1]
                if subcmd == b"t":
                    # Heartbeat ping
                    await self._send_stat(b"STMt")
                elif subcmd == b"q":
                    # Stop / flush
                    await self._send_stat(b"STMf")
                elif subcmd == b"s":
                    # Start stream: 24-byte header followed by HTTP GET request string
                    (
                        _command,
                        _autostart,
                        _codec,
                        _threshold,
                        _spdif,
                        _trans_dur,
                        _trans_type,
                        _flags,
                        _out_thresh,
                        _reserved,
                        _replay_gain,
                        server_port,
                        server_ip,
                    ) = struct.unpack("!cc5sBcBcBBBLHL", payload[:24])
                    httpreq = payload[24:].decode("latin1", errors="ignore")
                    first_line = httpreq.splitlines()[0] if httpreq else ""
                    parts = first_line.split(" ")
                    if len(parts) >= 2:
                        path = parts[1]
                        port = server_port or 8097
                        # Determine stream host (use server_ip if non-zero, else self.host)
                        if server_ip:
                            ip_bytes = struct.pack("!I", server_ip)
                            stream_host = ".".join(str(b) for b in ip_bytes)
                        else:
                            stream_host = self.host
                        stream_url = f"http://{stream_host}:{port}{path}"
                        _LOGGER.debug("Captured Slimproto stream URL: %s", stream_url)
                        if self._stream_waiter and not self._stream_waiter.done():
                            self._stream_waiter.set_result(stream_url)
                    # Acknowledge stream connection & start
                    await self._send_stat(b"STMc")
                    await self._send_stat(b"STMs")

    async def _send_helo(self) -> None:
        """Send HELO packet to register the player with Music Assistant."""
        header = struct.pack(
            "!BB6s16sHQ2s",
            12,  # device_id = squeezeplay/squeezelite
            0,  # revision
            self._mac_bytes,
            b"\x00" * 16,
            0x8000,
            0,
            b"EN",
        )
        await self._send_frame(b"HELO", header + _CAPABILITIES)

    async def _send_stat(self, event_code: bytes) -> None:
        """Send a 53-byte STAT frame with the given 4-byte event code."""
        payload = event_code + struct.pack(
            "!3IB2I2QI2HI",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        )
        await self._send_frame(b"STAT", payload)

    async def _send_frame(self, cmd: bytes, payload: bytes) -> None:
        """Write a Slimproto frame to the socket."""
        if not self._writer:
            return
        frame = cmd + struct.pack("!I", len(payload)) + payload
        self._writer.write(frame)
        await self._writer.drain()

    async def _close_writer(self) -> None:
        """Close the active StreamWriter if open."""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None
