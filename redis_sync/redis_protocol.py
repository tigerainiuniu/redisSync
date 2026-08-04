"""Redis replication protocol helpers.

The replication connection must have exactly one read owner.  redis-py may
read ahead into its parser buffer, so replication code uses
``ReplicationStreamReader`` for the handshake, RDB transfer, and command
stream instead of alternating between ``Connection.read_response`` and the
underlying socket.
"""

from __future__ import annotations

import socket
from typing import Callable, List, Optional, Tuple


MAX_REPLICATION_BUFFER_SIZE = 16 * 1024 * 1024
# Match Redis' default proto-max-bulk-len and client-query-buffer-limit.
MAX_REPLICATION_BULK_SIZE = 512 * 1024 * 1024
MAX_REPLICATION_FRAME_SIZE = 1024 * 1024 * 1024
MAX_REPLICATION_PENDING_SIZE = (
    MAX_REPLICATION_FRAME_SIZE + MAX_REPLICATION_BUFFER_SIZE
)
MAX_REPLICATION_ARGUMENTS = 1024 * 1024
_MAX_RESP_HEADER_SIZE = 128


class ReplicationProtocolError(Exception):
    """The peer sent a malformed or unsupported replication frame."""


class ReplicationConnectionClosed(ConnectionError):
    """The replication socket closed before the requested frame completed."""


class SnapshotRequiredError(RuntimeError):
    """A FULLRESYNC snapshot was received without an application callback."""


class _NeedMoreData(Exception):
    pass


def _line_end(data: bytes, start: int) -> int:
    end = data.find(b"\r\n", start)
    if end < 0:
        raise _NeedMoreData
    return end


def _parse_resp_value(data: bytes, pos: int = 0):
    if pos >= len(data):
        raise _NeedMoreData

    prefix = data[pos : pos + 1]
    try:
        line_end = _line_end(data, pos + 1)
    except _NeedMoreData:
        if len(data) - pos > _MAX_RESP_HEADER_SIZE:
            raise ReplicationProtocolError("RESP header exceeds replication limit")
        raise
    if line_end - pos > _MAX_RESP_HEADER_SIZE:
        raise ReplicationProtocolError("RESP header exceeds replication limit")
    token = data[pos + 1 : line_end]
    body_pos = line_end + 2

    if prefix == b"+":
        return token, body_pos
    if prefix == b"-":
        raise ReplicationProtocolError(token.decode("utf-8", errors="replace"))
    if prefix == b":":
        try:
            return int(token), body_pos
        except ValueError as exc:
            raise ReplicationProtocolError("invalid RESP integer") from exc
    if prefix == b"$":
        try:
            length = int(token)
        except ValueError as exc:
            raise ReplicationProtocolError("invalid RESP bulk length") from exc
        if length == -1:
            return None, body_pos
        if length < -1:
            raise ReplicationProtocolError("invalid negative RESP bulk length")
        if length > MAX_REPLICATION_BULK_SIZE:
            raise ReplicationProtocolError("RESP bulk length exceeds replication limit")
        frame_end = body_pos + length + 2
        if frame_end > MAX_REPLICATION_FRAME_SIZE:
            raise ReplicationProtocolError("RESP frame exceeds replication limit")
        if frame_end > len(data):
            raise _NeedMoreData
        if data[body_pos + length : frame_end] != b"\r\n":
            raise ReplicationProtocolError("RESP bulk payload lacks CRLF")
        return data[body_pos : body_pos + length], frame_end
    if prefix == b"*":
        try:
            count = int(token)
        except ValueError as exc:
            raise ReplicationProtocolError("invalid RESP array length") from exc
        if count == -1:
            return None, body_pos
        if count < -1:
            raise ReplicationProtocolError("invalid negative RESP array length")
        if count > MAX_REPLICATION_ARGUMENTS:
            raise ReplicationProtocolError("RESP array length exceeds replication limit")
        values = []
        cursor = body_pos
        for _ in range(count):
            value, cursor = _parse_resp_value(data, cursor)
            values.append(value)
        return values, cursor

    raise ReplicationProtocolError(f"unexpected RESP prefix {prefix!r}")


def parse_resp_array_frame(
    data: bytes,
) -> Tuple[Optional[List[bytes]], int]:
    """Return a complete command and its encoded byte length.

    ``(None, 0)`` means the frame is incomplete.  Malformed data raises
    ``ReplicationProtocolError`` so callers do not retain an ever-growing,
    permanently unparseable buffer.
    """

    try:
        value, consumed = _parse_resp_value(data)
    except _NeedMoreData:
        return None, 0

    if not isinstance(value, list):
        raise ReplicationProtocolError("replication command is not a RESP array")
    if any(not isinstance(item, bytes) for item in value):
        raise ReplicationProtocolError("replication command contains a non-bulk argument")
    return value, consumed


def parse_resp_array_command(data: bytes) -> Tuple[Optional[List[bytes]], bytes]:
    """Parse one RESP array command while preserving the legacy API."""

    try:
        command, consumed = parse_resp_array_frame(data)
    except ReplicationProtocolError:
        return None, data
    if command is None:
        return None, data
    return command, data[consumed:]


def is_replconf_getack(command: List[bytes]) -> bool:
    """Return whether a replication frame requests an immediate offset ACK."""
    return (
        len(command) >= 2
        and command[0].upper() == b"REPLCONF"
        and command[1].upper() == b"GETACK"
    )


class _IncrementalArrayCommandParser:
    """Incrementally parse one RESP array-of-bulk command from a bytearray."""

    def __init__(self):
        self._expected_arguments = None
        self._cursor = 0
        self._argument_spans = []
        self._pending_argument = None
        self._complete = None

    @staticmethod
    def _parse_header(data, cursor, prefix, label):
        if cursor >= len(data):
            return None
        if data[cursor : cursor + 1] != prefix:
            raise ReplicationProtocolError(
                "expected %s prefix %r, got %r"
                % (label, prefix, data[cursor : cursor + 1])
            )
        end = data.find(b"\r\n", cursor + 1)
        if end < 0:
            if len(data) - cursor > _MAX_RESP_HEADER_SIZE:
                raise ReplicationProtocolError(
                    "%s header exceeds replication limit" % label
                )
            return None
        if end - cursor > _MAX_RESP_HEADER_SIZE:
            raise ReplicationProtocolError(
                "%s header exceeds replication limit" % label
            )
        token = bytes(data[cursor + 1 : end])
        try:
            value = int(token)
        except ValueError as exc:
            raise ReplicationProtocolError("invalid %s length" % label) from exc
        return value, end + 2

    def parse(self, data):
        if self._complete is not None:
            return self._complete

        if self._expected_arguments is None:
            parsed = self._parse_header(data, 0, b"*", "RESP array")
            if parsed is None:
                return None
            count, self._cursor = parsed
            if count <= 0:
                raise ReplicationProtocolError(
                    "replication command must contain at least one argument"
                )
            if count > MAX_REPLICATION_ARGUMENTS:
                raise ReplicationProtocolError(
                    "RESP array length exceeds replication limit"
                )
            # Even empty bulk arguments require "$0\r\n\r\n" framing.
            if self._cursor + count * 6 > MAX_REPLICATION_FRAME_SIZE:
                raise ReplicationProtocolError("RESP frame exceeds replication limit")
            self._expected_arguments = count

        while len(self._argument_spans) < self._expected_arguments:
            if self._pending_argument is None:
                parsed = self._parse_header(
                    data, self._cursor, b"$", "RESP bulk"
                )
                if parsed is None:
                    return None
                length, payload_start = parsed
                if length < 0:
                    raise ReplicationProtocolError(
                        "replication command contains a null bulk argument"
                    )
                if length > MAX_REPLICATION_BULK_SIZE:
                    raise ReplicationProtocolError(
                        "RESP bulk length exceeds replication limit"
                    )
                payload_end = payload_start + length
                frame_end = payload_end + 2
                if frame_end > MAX_REPLICATION_FRAME_SIZE:
                    raise ReplicationProtocolError(
                        "RESP frame exceeds replication limit"
                    )
                self._pending_argument = (
                    payload_start,
                    payload_end,
                    frame_end,
                )

            payload_start, payload_end, frame_end = self._pending_argument
            if len(data) < frame_end:
                return None
            if data[payload_end:frame_end] != b"\r\n":
                raise ReplicationProtocolError("RESP bulk payload lacks CRLF")
            self._argument_spans.append((payload_start, payload_end))
            self._cursor = frame_end
            self._pending_argument = None

        data_view = memoryview(data)
        try:
            command = [
                data_view[start:end].tobytes()
                for start, end in self._argument_spans
            ]
        finally:
            data_view.release()
        self._complete = command, self._cursor
        return self._complete


class ReplicationStreamReader:
    """Buffered, socket-only reader for one Redis replication connection."""

    def __init__(
        self,
        sock,
        buffer_size: int = 65536,
        initial_buffer: bytes = b"",
        initial_stream=None,
        on_socket_read: Optional[Callable[[int], None]] = None,
        allow_leading_crlf: bool = False,
        adopt_initial_buffer: bool = False,
    ):
        self.sock = sock
        self.buffer_size = int(buffer_size)
        if not 1 <= self.buffer_size <= MAX_REPLICATION_BUFFER_SIZE:
            raise ValueError(
                "replication buffer_size must be between 1 and "
                f"{MAX_REPLICATION_BUFFER_SIZE} bytes"
            )
        if adopt_initial_buffer:
            if not isinstance(initial_buffer, bytearray):
                raise TypeError(
                    "adopt_initial_buffer requires a bytearray"
                )
            self._buffer = initial_buffer
        else:
            self._buffer = bytearray(initial_buffer)
        if len(self._buffer) > MAX_REPLICATION_PENDING_SIZE:
            raise ReplicationProtocolError(
                "replication pending buffer exceeds replication limit"
            )
        self._initial_stream = initial_stream
        self._on_socket_read = on_socket_read
        self._allow_leading_crlf = allow_leading_crlf
        self._command_parser = _IncrementalArrayCommandParser()

    @property
    def pending_bytes(self) -> bytes:
        return bytes(self._buffer)

    @property
    def pending_size(self) -> int:
        return len(self._buffer)

    def take_buffer(self) -> bytes:
        data = bytes(self._buffer)
        self._buffer.clear()
        self._command_parser = _IncrementalArrayCommandParser()
        return data

    def take_buffer_ownership(self) -> bytearray:
        """Transfer the pending bytearray without copying it."""
        data = self._buffer
        self._buffer = bytearray()
        self._command_parser = _IncrementalArrayCommandParser()
        return data

    def discard_partial_snapshot_terminator(self) -> int:
        """Drop a lone CR proven to be incomplete RDB framing at disconnect."""
        if self._allow_leading_crlf and self._buffer == b"\r":
            self._buffer.clear()
            self._allow_leading_crlf = False
            return 1
        return 0

    def _recv(self) -> None:
        if self._initial_stream is not None:
            data = self._initial_stream.read(self.buffer_size)
            if data:
                self._buffer.extend(data)
                self._check_pending_size()
                return
            self._initial_stream = None
        try:
            data = self.sock.recv(self.buffer_size)
        except socket.timeout:
            raise
        except OSError as exc:
            raise ReplicationConnectionClosed(
                f"replication socket read failed: {exc}"
            ) from exc
        if not data:
            raise ReplicationConnectionClosed("replication connection closed")
        self._buffer.extend(data)
        self._check_pending_size()
        if self._on_socket_read is not None:
            self._on_socket_read(len(data))

    def _check_pending_size(self) -> None:
        if len(self._buffer) > MAX_REPLICATION_PENDING_SIZE:
            raise ReplicationProtocolError(
                "replication pending buffer exceeds replication limit"
            )

    def _readline(self) -> bytes:
        while True:
            end = self._buffer.find(b"\r\n")
            if end >= 0:
                if end > _MAX_RESP_HEADER_SIZE:
                    raise ReplicationProtocolError(
                        "RESP header exceeds replication limit"
                    )
                line = bytes(self._buffer[:end])
                del self._buffer[: end + 2]
                return line
            if len(self._buffer) > _MAX_RESP_HEADER_SIZE:
                raise ReplicationProtocolError(
                    "RESP header exceeds replication limit"
                )
            self._recv()

    def _read_exact(self, size: int) -> bytes:
        while len(self._buffer) < size:
            self._recv()
        value = bytes(self._buffer[:size])
        del self._buffer[:size]
        return value

    def _discard_keepalive_newlines(self) -> None:
        """Discard Redis pre-snapshot keepalives sent outside the RESP stream."""
        while True:
            if not self._buffer:
                self._recv()
            if self._buffer[:1] == b"\n":
                del self._buffer[:1]
                continue
            if self._buffer[:1] == b"\r":
                if len(self._buffer) < 2:
                    self._recv()
                    continue
                if self._buffer[:2] == b"\r\n":
                    del self._buffer[:2]
                    continue
            return

    def read_response(self):
        """Read one ordinary RESP2 response from the replication socket."""

        while True:
            self._discard_keepalive_newlines()
            try:
                value, consumed = _parse_resp_value(bytes(self._buffer))
            except _NeedMoreData:
                self._recv()
                continue
            del self._buffer[:consumed]
            return value

    def read_rdb(
        self,
        *,
        sink: Optional[Callable[[bytes], None]] = None,
        collect: bool = True,
    ) -> bytes:
        """Read a fixed-length or diskless RDB, optionally without materializing it."""

        self._discard_keepalive_newlines()
        header = self._readline()
        if not header.startswith(b"$"):
            raise ReplicationProtocolError(f"invalid RDB header {header!r}")

        descriptor = header[1:]
        if descriptor.startswith(b"EOF:"):
            marker = descriptor[4:]
            if not marker:
                raise ReplicationProtocolError("empty RDB EOF marker")
            chunks = bytearray() if collect else None

            def emit(data: bytes) -> None:
                if not data:
                    return
                if sink is not None:
                    sink(data)
                if chunks is not None:
                    chunks.extend(data)

            while True:
                marker_pos = self._buffer.find(marker)
                if marker_pos >= 0:
                    emit(bytes(self._buffer[:marker_pos]))
                    del self._buffer[: marker_pos + len(marker)]
                    self._allow_leading_crlf = True
                    return bytes(chunks or b"")

                # Keep a possible marker prefix buffered across socket reads.
                keep = min(len(self._buffer), len(marker) - 1)
                flush = len(self._buffer) - keep
                if flush > 0:
                    emit(bytes(self._buffer[:flush]))
                    del self._buffer[:flush]
                self._recv()

        try:
            size = int(descriptor)
        except ValueError as exc:
            raise ReplicationProtocolError(f"invalid RDB length {descriptor!r}") from exc
        if size < 0:
            raise ReplicationProtocolError("negative RDB length")
        remaining = size
        chunks = bytearray() if collect else None
        while remaining:
            if not self._buffer:
                self._recv()
            take = min(remaining, len(self._buffer))
            chunk = bytes(self._buffer[:take])
            del self._buffer[:take]
            if sink is not None:
                sink(chunk)
            if chunks is not None:
                chunks.extend(chunk)
            remaining -= take
        self._allow_leading_crlf = True
        return bytes(chunks or b"")

    def peek_command(self) -> Tuple[List[bytes], int]:
        """Read enough bytes for one command without consuming the frame."""

        while True:
            if self._allow_leading_crlf:
                if self._buffer[:1] == b"\r" and len(self._buffer) < 2:
                    self._recv()
                    continue
                if self._buffer[:2] == b"\r\n":
                    # Some Redis-compatible servers terminate the RDB bulk
                    # payload with CRLF. It is snapshot framing, not part of
                    # the replication backlog, so discard it before offset
                    # accounting begins.
                    del self._buffer[:2]
                    self._allow_leading_crlf = False
                elif self._buffer:
                    self._allow_leading_crlf = False
            parsed = self._command_parser.parse(self._buffer)
            if parsed is not None:
                return parsed
            self._recv()

    def commit(self, consumed: int) -> None:
        if consumed < 0 or consumed > len(self._buffer):
            raise ValueError("invalid replication frame commit")
        del self._buffer[:consumed]
        self._allow_leading_crlf = False
        self._command_parser = _IncrementalArrayCommandParser()
