import select
import socket
import time
from dataclasses import dataclass, field


BUFFER_SIZE = 16 * 1024
MAX_PENDING_BYTES = 256 * 1024
SELECT_TIMEOUT = 1.0
IDLE_TIMEOUT = 300.0


@dataclass
class _Endpoint:
    sock: socket.socket
    name: str
    buffer: bytearray = field(default_factory=bytearray)
    read_open: bool = True
    write_open: bool = True
    shutdown_after_flush: bool = False
    bytes_sent: int = 0

# zamykanie po wysłaniu
def _finish_write_if_ready(endpoint: _Endpoint) -> None:

    if (
        endpoint.shutdown_after_flush
        and endpoint.write_open
        and not endpoint.buffer
    ):
        try:
            endpoint.sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass

        endpoint.write_open = False


def relay_bidirectional(
    operator_socket: socket.socket,
    target_socket: socket.socket
) -> None:

    operator = _Endpoint(operator_socket, "Operator")
    target = _Endpoint(target_socket, "Target")

    endpoints = (operator, target)

    peer = {
        operator_socket: target,
        target_socket: operator
    }

    started = time.monotonic()
    last_activity = started
    ending_reason = "normal EOF"

    try:
        operator_socket.setblocking(False)
        target_socket.setblocking(False)

        while True:

            for endpoint in endpoints:
                _finish_write_if_ready(endpoint)

            if (
                all(not end.read_open for end in endpoints)
                and all(not end.buffer for end in endpoints)
            ):
                break

            readable = [
                end.sock
                for end in endpoints
                if end.read_open
                and len(peer[end.sock].buffer) < MAX_PENDING_BYTES
            ]

            writable = [
                end.sock
                for end in endpoints
                if end.write_open and end.buffer
            ]

            if not readable and not writable:
                ending_reason = "no available socket operations"
                break

            ready_read, ready_write, _ = select.select(
                readable,
                writable,
                [],
                SELECT_TIMEOUT
            )

            now = time.monotonic()

            if (
                IDLE_TIMEOUT > 0
                and now - last_activity >= IDLE_TIMEOUT
            ):
                ending_reason = (
                    f"idle timeout ({IDLE_TIMEOUT:g}s)"
                )
                break

            for source_socket in ready_read:

                if source_socket is operator_socket:
                    source = operator
                else:
                    source = target

                destination = peer[source_socket]

                space = (
                    MAX_PENDING_BYTES
                    - len(destination.buffer)
                )

                if space <= 0:
                    continue

                try:
                    data = source.sock.recv(
                        min(BUFFER_SIZE, space)
                    )
                except BlockingIOError:
                    continue

                if data:
                    destination.buffer.extend(data)
                    last_activity = time.monotonic()

                else:
                    source.read_open = False
                    destination.shutdown_after_flush = True

            for destination_socket in ready_write:

                if destination_socket is operator_socket:
                    destination = operator
                else:
                    destination = target

                if not destination.buffer:
                    continue

                try:
                    sent = destination.sock.send(
                        destination.buffer
                    )
                except BlockingIOError:
                    continue

                if sent == 0:
                    raise ConnectionError(
                        f"{destination.name} connection "
                        "closed during send"
                    )

                del destination.buffer[:sent]

                destination.bytes_sent += sent
                last_activity = time.monotonic()

    except OSError as error:
        ending_reason = f"network error: {error}"

        print(
            f"[!] Relay error: {error}",
            flush=True
        )

    finally:
        for endpoint in endpoints:
            endpoint.sock.close()

        duration = time.monotonic() - started

        print(
            f"[RELAY] Session finished ({ending_reason}); "
            f"Operator -> Target: {target.bytes_sent} bytes, "
            f"Target -> Operator: {operator.bytes_sent} bytes, "
            f"duration: {duration:.2f}s",
            flush=True
        )
