"""Read-only access to the configured PTA users and flat file directory."""

import os
from pathlib import Path
import stat
from typing import BinaryIO


def load_users(path: Path) -> frozenset[str]:
    users = path.read_text(encoding="ascii").splitlines()
    if not users or any(not user or any(ord(c) < 33 or ord(c) > 126 for c in user)
                        for user in users):
        raise ValueError("users.txt must contain one nonempty ASCII username per line")
    return frozenset(users)


def valid_filename(name: str) -> bool:
    return (bool(name) and name not in (".", "..")
            and not any(c in name for c in ("/", "\\", ","))
            and all(33 <= ord(c) <= 126 for c in name))


class FileStore:
    def __init__(self, directory: Path):
        self.directory = directory.resolve(strict=True)
        if not self.directory.is_dir():
            raise NotADirectoryError(directory)

    def list_files(self) -> list[str]:
        with os.scandir(self.directory) as entries:
            return sorted(entry.name for entry in entries
                          if valid_filename(entry.name)
                          and entry.is_file(follow_symlinks=False))

    def open_file(self, name: str) -> tuple[BinaryIO, int]:
        if not valid_filename(name):
            raise FileNotFoundError(name)
        # O_NOFOLLOW prevents symlink substitution between validation and open.
        # O_NONBLOCK also prevents a substituted FIFO from blocking the loop.
        flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0)
        if hasattr(os, "O_NOFOLLOW"):
            fd = os.open(self.directory / name, flags | os.O_NOFOLLOW)
        else:
            # Portable fallback; Unix is the primary supported platform.
            path = self.directory / name
            if path.is_symlink():
                raise FileNotFoundError(name)
            fd = os.open(path, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise FileNotFoundError(name)
            stream = os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise
        return stream, info.st_size
