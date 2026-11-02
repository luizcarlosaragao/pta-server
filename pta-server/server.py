#!/usr/bin/env python3
"""Concurrent, read-only PTA server. Run with --help for configuration."""

import argparse
import asyncio
from contextlib import suppress
import logging
from pathlib import Path
import signal

from protocol import InvalidRequest, MAX_REQUEST, parse_request, sequence_hint
from storage import FileStore, load_users

LOG = logging.getLogger("pta")
BASE = Path(__file__).resolve().parent
CHUNK_SIZE = 64 * 1024


class Session:
    def __init__(self, reader, writer, users, store, legacy_idle):
        self.reader = reader
        self.writer = writer
        self.users = users
        self.store = store
        self.legacy_idle = legacy_idle
        self.authenticated = False
        self.closing = False
        self.busy = False
        self.operation = None

    async def reply(self, sequence: bytes, result: bytes):
        self.writer.write(sequence + b" " + result)
        await self.writer.drain()

    async def dispatch(self, frame: bytes):
        if self.busy or self.closing:
            return
        try:
            request = parse_request(frame)
        except InvalidRequest as error:
            await self.reply(error.sequence, b"NOK")
            self.closing = not self.authenticated
            return
        seq = request.sequence
        if not self.authenticated:
            if request.command == "CUMP" and request.argument in self.users:
                await self.reply(seq, b"OK")
                self.authenticated = True
            else:
                await self.reply(seq, b"NOK")
                self.closing = True
        elif request.command == "TERM":
            self.closing = True
            await self.reply(seq, b"OK")
        elif request.command in ("LIST", "PEGA"):
            self.busy = True
            self.operation = asyncio.create_task(self.perform(request))
        else:
            await self.reply(seq, b"NOK")

    async def perform(self, request):
        started = False
        try:
            if request.command == "LIST":
                names = self.store.list_files()
                payload = (f"ARQS {len(names)} " + ",".join(names)).encode("ascii")
                started = True
                await self.reply(request.sequence, payload)
            else:
                stream, size = self.store.open_file(request.argument)
                with stream:
                    started = True
                    await self.reply(request.sequence, f"ARQ {size} ".encode("ascii"))
                    remaining = size
                    while remaining:
                        data = stream.read(min(CHUNK_SIZE, remaining))
                        if not data:
                            raise OSError("file truncated during transfer")
                        self.writer.write(data)
                        await self.writer.drain()
                        remaining -= len(data)
                        # drain() need not yield when the socket is writable.
                        await asyncio.sleep(0)
        except (OSError, ValueError, UnicodeError) as error:
            LOG.debug("operation failed: %s", error)
            if started:
                self.closing = True
                self.writer.close()
            else:
                try:
                    await self.reply(request.sequence, b"NOK")
                except (OSError, ConnectionError):
                    self.closing = True
                    self.writer.close()
        finally:
            self.busy = False

    async def run(self):
        pending = bytearray()
        try:
            while not self.closing:
                try:
                    if pending:
                        data = await asyncio.wait_for(self.reader.read(CHUNK_SIZE), self.legacy_idle)
                    else:
                        data = await self.reader.read(CHUNK_SIZE)
                except asyncio.TimeoutError:
                    frame = bytes(pending)
                    pending.clear()
                    await self.dispatch(frame)
                    continue
                if not data:
                    if pending:
                        await self.dispatch(bytes(pending))
                    if self.operation:
                        await self.operation
                    break
                if self.busy:
                    pending.clear()
                    continue
                pending.extend(data)
                while pending and not self.closing:
                    end = pending.find(b"\n")
                    length = end if end >= 0 else len(pending)
                    if length > MAX_REQUEST:
                        await self.reply(sequence_hint(bytes(pending[:MAX_REQUEST])), b"NOK")
                        self.closing = True
                        break
                    if end < 0:
                        break
                    frame = bytes(pending[:end])
                    del pending[:end + 1]
                    if frame.endswith(b"\r"):
                        frame = frame[:-1]
                    await self.dispatch(frame)
                    if self.busy:
                        # Requests already pipelined behind this operation are ignored.
                        pending.clear()
                        break
        except asyncio.CancelledError:
            # A slow peer must not hold shutdown open while buffered data drains.
            self.writer.transport.abort()
            raise
        except (OSError, ConnectionError):
            LOG.debug("client disconnected", exc_info=True)
        finally:
            if self.operation:
                if not self.operation.done():
                    self.operation.cancel()
                with suppress(asyncio.CancelledError, OSError):
                    await self.operation
            self.writer.close()
            with suppress(OSError):
                await self.writer.wait_closed()


async def serve(host, port, users, store, legacy_idle=0.1):
    sessions = set()

    async def connected(reader, writer):
        task = asyncio.current_task()
        sessions.add(task)
        LOG.info("connected: %s", writer.get_extra_info("peername"))
        try:
            await Session(reader, writer, users, store, legacy_idle).run()
        finally:
            sessions.discard(task)
            LOG.info("disconnected: %s", writer.get_extra_info("peername"))

    server = await asyncio.start_server(connected, host, port)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopped.set)
            installed.append(sig)
        except NotImplementedError:
            pass
    try:
        LOG.info("PTA listening on %s", ", ".join(str(s.getsockname()) for s in server.sockets))
        await stopped.wait()
    finally:
        server.close()
        active = list(sessions)
        for task in active:
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
        # Python 3.13+ also waits for accepted transports: close sessions first.
        await server.wait_closed()
        for sig in installed:
            loop.remove_signal_handler(sig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=11550)
    parser.add_argument("--legacy-idle-ms", type=float, default=100.0,
                        help="quiet interval for requests without LF (default: 100 ms)")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be between 0 and 65535")
    if not 0 < args.legacy_idle_ms < float("inf"):
        parser.error("legacy-idle-ms must be finite and positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        users = load_users(BASE / "users.txt")
        store = FileStore(BASE / "files")
        asyncio.run(serve(args.host, args.port, users, store, args.legacy_idle_ms / 1000))
    except (OSError, ValueError) as error:
        parser.exit(1, f"PTA startup failed: {error}\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
