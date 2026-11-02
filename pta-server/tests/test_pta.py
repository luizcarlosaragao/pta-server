"""Protocol and socket-level regression suite; no external dependencies."""

import asyncio
import hashlib
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
ROOT = BASE.parent
sys.path.insert(0, str(BASE))
from protocol import InvalidRequest, MAX_REQUEST, parse_request
from server import Session
from storage import FileStore, load_users


class GrammarTests(unittest.TestCase):
    def test_valid_commands_and_sequence_preservation(self):
        for raw in (b"00042 LIST", b"42 TERM", b"42 CUMP user1", b"42 PEGA a b"):
            with self.subTest(raw=raw):
                request = parse_request(raw)
                self.assertEqual(request.sequence, raw.split(b" ")[0])
        self.assertEqual(parse_request(b"42 PEGA first second").argument, "first")

    def test_invalid_grammar(self):
        for raw in (b"", b"abc LIST", b"-1 LIST", b"1 list", b"1 TRAP",
                    b"1 CUMP", b"1 CUMP a b", b"1 PEGA", b"1 LIST extra",
                    b"1 TERM extra", b"1  LIST", b" 1 LIST", b"1 LIST ",
                    b"1\tLIST", b"1 CUMP a\x00", b"1 CUMP \xff"):
            with self.subTest(raw=raw), self.assertRaises(InvalidRequest):
                parse_request(raw)

    def test_invalid_users_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "users.txt"
            for data in (b"", b"user1\n\nuser2", b"a b\n", b"\xff"):
                path.write_bytes(data)
                with self.subTest(data=data), self.assertRaises((ValueError, UnicodeError)):
                    load_users(path)
            path.write_bytes(b"User1\r\nuser1\r\n")
            self.assertEqual(load_users(path), {"User1", "user1"})


class SocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.store = FileStore(self.directory)
        self.users = load_users(BASE / "users.txt")
        self.sessions = set()
        self.connections = []

        async def connected(reader, writer):
            task = asyncio.current_task()
            self.sessions.add(task)
            try:
                await Session(reader, writer, self.users, self.store, 0.1).run()
            finally:
                self.sessions.discard(task)

        self.server = await asyncio.start_server(connected, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        for _, writer in self.connections:
            writer.close()
        for _, writer in self.connections:
            try:
                await writer.wait_closed()
            except OSError:
                pass
        self.server.close()
        await self.server.wait_closed()
        tasks = list(self.sessions)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.temp.cleanup()

    async def connect(self, authenticate=True):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        self.connections.append((reader, writer))
        if authenticate:
            await self.expect(reader, writer, b"73 CUMP user1\n", b"73 OK")
        return reader, writer

    async def exact(self, reader, size):
        return await asyncio.wait_for(reader.readexactly(size), 5)

    async def expect(self, reader, writer, request, response):
        writer.write(request)
        await writer.drain()
        self.assertEqual(await self.exact(reader, len(response)), response)

    async def download(self, reader, writer, name, expected, seq=b"90", suffix=b"\n", slow=False):
        writer.write(seq + b" PEGA " + name.encode("ascii") + suffix)
        await writer.drain()
        header = seq + b" ARQ " + str(len(expected)).encode() + b" "
        self.assertEqual(await self.exact(reader, len(header)), header)
        chunks = []
        remaining = len(expected)
        while remaining:
            chunk = await self.exact(reader, min(257 if slow else 8192, remaining))
            chunks.append(chunk)
            remaining -= len(chunk)
            if slow:
                await asyncio.sleep(0.001)
        received = b"".join(chunks)
        self.assertEqual(received, expected)
        self.assertEqual(hashlib.sha256(received).digest(), hashlib.sha256(expected).digest())

    async def test_authentication_rejections_close_connection(self):
        for request in (b"0 CUMP unknown\n", b"0 CUMP User1\n", b"0 LIST\n",
                        b"0 TERM\n", b"0 TRAP\n", b"0 CUMP\n"):
            with self.subTest(request=request):
                reader, writer = await self.connect(False)
                await self.expect(reader, writer, request, b"0 NOK")
                self.assertEqual(await asyncio.wait_for(reader.read(1), 2), b"")

    async def test_each_configured_user(self):
        for user in self.users:
            reader, writer = await self.connect(False)
            await self.expect(reader, writer, b"999 CUMP " + user.encode() + b"\n", b"999 OK")
            await self.expect(reader, writer, b"1000 TERM\n", b"1000 OK")

    async def test_invalid_commands_keep_authenticated_session(self):
        reader, writer = await self.connect()
        for request, response in ((b"81 TRAP\n", b"81 NOK"),
                                  (b"82 CUMP user1\n", b"82 NOK"),
                                  (b"83 LIST extra\n", b"83 NOK"),
                                  (b"84 PEGA\n", b"84 NOK"),
                                  (b"85 CUMP \xff\n", b"85 NOK"),
                                  (b"garbage\n", b"0 NOK"),
                                  (b"\n", b"0 NOK")):
            await self.expect(reader, writer, request, response)
        await self.expect(reader, writer, b"86 TERM\n", b"86 OK")
        self.assertEqual(await asyncio.wait_for(reader.read(1), 2), b"")

    async def test_arbitrary_and_repeated_sequences(self):
        reader, writer = await self.connect()
        for sequence in (b"00091", b"91", b"5", b"100000000000000000000000000"):
            await self.expect(reader, writer, sequence + b" LIST\n", sequence + b" ARQS 0 ")

    async def test_all_original_files_and_listing(self):
        self.store.directory = BASE / "files"
        names = sorted(path.name for path in (BASE / "files").iterdir())
        self.assertEqual(len(names), 16)
        reader, writer = await self.connect()
        listing = (f"80 ARQS {len(names)} " + ",".join(names)).encode()
        await self.expect(reader, writer, b"80 LIST\n", listing)
        for index, name in enumerate(names):
            with self.subTest(name=name):
                await self.download(reader, writer, name, (BASE / "files" / name).read_bytes(),
                                    seq=str(index + 81).encode())
        await self.expect(reader, writer, b"99 TERM\n", b"99 OK")

    async def test_list_over_client_buffer(self):
        names = [f"file-{i:03d}-" + "a" * 80 for i in range(80)]
        for name in names:
            (self.directory / name).touch()
        reader, writer = await self.connect()
        expected = ("1 ARQS 80 " + ",".join(names)).encode()
        self.assertGreater(len(expected), 2048)
        await self.expect(reader, writer, b"1 LIST\n", expected)

    async def test_large_binary_and_slow_reader(self):
        data = bytes(range(256)) * 1024
        (self.directory / "binary").write_bytes(data)
        reader, writer = await self.connect()
        await self.download(reader, writer, "binary", data, slow=True)
        await self.expect(reader, writer, b"91 LIST\n", b"91 ARQS 1 binary")

    async def test_nonexistent_and_unsafe_paths(self):
        (self.directory / "subdir").mkdir()
        outside = self.directory.parent / (self.directory.name + "-secret")
        outside.write_bytes(b"secret")
        try:
            (self.directory / "link").symlink_to(outside)
            reader, writer = await self.connect()
            for name in ("missing", "../users.txt", "/etc/passwd", "subdir", "subdir/file",
                         "link", "..", ".", "a\\b"):
                with self.subTest(name=name):
                    await self.expect(reader, writer, b"4 PEGA " + name.encode() + b"\n", b"4 NOK")
            await self.expect(reader, writer, b"5 LIST\n", b"5 ARQS 0 ")
        finally:
            outside.unlink()

    async def test_extra_file_arguments_ignored(self):
        (self.directory / "first").write_bytes(b"one")
        reader, writer = await self.connect()
        await self.expect(reader, writer, b"4 PEGA first second\n", b"4 ARQ 3 one")

    async def test_fragmented_legacy_and_line_requests(self):
        reader, writer = await self.connect(False)
        for fragment in (b"40 C", b"UMP us", b"er1"):
            writer.write(fragment)
            await writer.drain()
            await asyncio.sleep(0.015)
        self.assertEqual(await self.exact(reader, 5), b"40 OK")
        for fragment in (b"41 L", b"IS", b"T\r", b"\n"):
            writer.write(fragment)
            await writer.drain()
            await asyncio.sleep(0.015)
        self.assertEqual(await self.exact(reader, 10), b"41 ARQS 0 ")
        await self.expect(reader, writer, b"42 LIST", b"42 ARQS 0 ")
        await self.expect(reader, writer, b"43 TERM", b"43 OK")

    async def test_oversized_request_closes(self):
        for authenticated in (False, True):
            reader, writer = await self.connect(authenticated)
            await self.expect(reader, writer, b"123 " + b"X" * MAX_REQUEST, b"123 NOK")
            self.assertEqual(await asyncio.wait_for(reader.read(1), 2), b"")

    async def test_request_at_buffer_limit(self):
        reader, writer = await self.connect()
        frame = b"8 " + b"X" * (MAX_REQUEST - 2)
        await self.expect(reader, writer, frame + b"\n", b"8 NOK")
        await self.expect(reader, writer, b"9 TERM\n", b"9 OK")

    async def test_storage_errors_keep_session_alive(self):
        reader, writer = await self.connect()
        with patch.object(self.store, "list_files", side_effect=PermissionError("denied")):
            await self.expect(reader, writer, b"6 LIST\n", b"6 NOK")
        with patch.object(self.store, "open_file", side_effect=PermissionError("denied")):
            await self.expect(reader, writer, b"7 PEGA no\n", b"7 NOK")
        await self.expect(reader, writer, b"8 LIST\n", b"8 ARQS 0 ")

    async def test_truncated_file_closes_without_nok_in_body(self):
        (self.directory / "short").write_bytes(b"abc")
        original_open = self.store.open_file

        def wrong_size(name):
            stream, size = original_open(name)
            return stream, size + 10

        reader, writer = await self.connect()
        with patch.object(self.store, "open_file", side_effect=wrong_size):
            writer.write(b"9 PEGA short\n")
            await writer.drain()
            data = await asyncio.wait_for(reader.read(), 3)
        self.assertEqual(data, b"9 ARQ 13 abc")

    async def test_busy_commands_are_discarded(self):
        (self.directory / "large").write_bytes(bytes(range(256)) * 65536)
        reader, writer = await self.connect()
        writer.write(b"9 PEGA large\n10 TERM\n11 LIST\n")
        await writer.drain()
        header = b"9 ARQ 16777216 "
        self.assertEqual(await self.exact(reader, len(header)), header)
        # Additional commands while backpressure keeps the transfer active.
        writer.write(b"12 LIST\n13 TERM\n")
        await writer.drain()
        await asyncio.sleep(0.03)
        body = await self.exact(reader, 16777216)
        self.assertEqual(hashlib.sha256(body).digest(),
                         hashlib.sha256(bytes(range(256)) * 65536).digest())
        await self.expect(reader, writer, b"14 LIST\n", b"14 ARQS 1 large")

    async def test_twenty_concurrent_clients(self):
        payload = bytes(range(256)) * 300
        (self.directory / "shared").write_bytes(payload)

        async def client(index):
            reader, writer = await self.connect()
            seq = str(1000 + index).encode()
            await self.download(reader, writer, "shared", payload, seq)
            await self.expect(reader, writer, b"7 TERM\n", b"7 OK")

        await asyncio.gather(*(client(i) for i in range(20)))

    async def test_disconnect_during_download_and_reconnect(self):
        (self.directory / "large").write_bytes(b"x" * (8 * 1024 * 1024))
        reader, writer = await self.connect()
        writer.write(b"1 PEGA large\n")
        await writer.drain()
        await self.exact(reader, len(b"1 ARQ 8388608 "))
        writer.transport.abort()
        reader, writer = await self.connect()
        await self.expect(reader, writer, b"2 LIST\n", b"2 ARQS 1 large")

    async def test_original_client_text_download(self):
        self.store.directory = BASE / "files"
        name = "dummyfile02-with-a-bigger-name-to-test-your-buffer-treatment.txt"
        # Pin the random choice externally; the supplied client stays untouched.
        code = ("import random, runpy, sys; "
                f"random.choice=lambda files: {name!r}; "
                f"sys.argv=[{str(ROOT / 'pta-client.py')!r},'127.0.0.1',{str(self.port)!r},'user1']; "
                f"runpy.run_path({str(ROOT / 'pta-client.py')!r}, run_name='__main__')")
        with tempfile.TemporaryDirectory() as directory:
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-c", code, cwd=directory,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            self.assertEqual(process.returncode, 0, stderr.decode())
            self.assertIn(b"Points: 6/6", stdout)
            self.assertIn(b"TERM is OK!", stdout)
            self.assertEqual((Path(directory) / name).read_bytes(), (BASE / "files" / name).read_bytes())


class EntrypointTests(unittest.TestCase):
    def test_start_from_other_directory_and_shutdown_active_session(self):
        import socket
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.Popen(
                [sys.executable, str(BASE / "server.py"), "--host", "127.0.0.1", "--port", "0"],
                cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                # The first log record reports the actual OS-selected port.
                line = process.stderr.readline()
                import re
                match = re.search(r"\('127\.0\.0\.1', (\d+)\)", line)
                self.assertIsNotNone(match, line)
                with socket.create_connection(("127.0.0.1", int(match[1])), timeout=3) as client:
                    client.sendall(b"2 CUMP user1")
                    self.assertEqual(client.recv(10), b"2 OK")
                    process.send_signal(signal.SIGTERM)
                    self.assertEqual(process.wait(timeout=5), 0)
                    self.assertEqual(client.recv(10), b"")
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)

    def test_invalid_cli_arguments(self):
        for args in (("--port", "65536"), ("--legacy-idle-ms", "0"),
                     ("--legacy-idle-ms", "nan"), ("--legacy-idle-ms", "inf")):
            result = subprocess.run([sys.executable, str(BASE / "server.py"), *args],
                                    capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
