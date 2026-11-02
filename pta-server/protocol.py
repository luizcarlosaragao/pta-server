"""PTA request grammar. Payloads are never decoded as text."""

from dataclasses import dataclass
import re

MAX_REQUEST = 64 * 1024


def sequence_hint(data: bytes) -> bytes:
    """Recover an ASCII sequence even when the remainder is malformed."""
    token = data.split(b" ", 1)[0].split(b"\t", 1)[0]
    return token if re.fullmatch(rb"[0-9]+", token) else b"0"


class InvalidRequest(ValueError):
    def __init__(self, sequence: bytes):
        self.sequence = sequence


@dataclass(frozen=True)
class Request:
    sequence: bytes
    command: str
    argument: str | None = None


def parse_request(data: bytes) -> Request:
    sequence = sequence_hint(data)
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError:
        raise InvalidRequest(sequence) from None
    # A single ASCII space separates fields. No leading/trailing whitespace.
    fields = text.split(" ")
    if (len(fields) < 2 or not re.fullmatch(r"[0-9]+", fields[0])
            or any(not field or any(ord(c) < 33 or ord(c) > 126 for c in field)
                   for field in fields)):
        raise InvalidRequest(sequence)
    command = fields[1]
    if command in ("LIST", "TERM") and len(fields) == 2:
        return Request(sequence, command)
    if command == "CUMP" and len(fields) == 3:
        return Request(sequence, command, fields[2])
    if command == "PEGA" and len(fields) >= 3:
        # The specification explicitly says to ignore additional file requests.
        return Request(sequence, command, fields[2])
    raise InvalidRequest(sequence)
