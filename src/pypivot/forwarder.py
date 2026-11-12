import argparse
import ipaddress
import socket
import threading

from relay import relay_bidirectional


# Default Docker topology
LISTEN_HOST = "172.29.10.20"
LISTEN_PORT = 8888
TARGET_HOST = "172.29.20.10"
TARGET_PORT = 9900
MAX_SESSIONS = 10
BACKLOG = 10
CONNECT_TIMEOUT = 5.0


def ipv4_address(value: str) -> str:
    try:
        return str(ipaddress.IPv4Address(value))
    except ipaddress.AddressValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid IPv4 address: {value}") from exc


def tcp_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive integer") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return result


def positive_float(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive number") from exc
    if not 0 < result < float("inf"):
        raise argparse.ArgumentTypeError("value must be a finite number above zero")
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="PyPivot: forward TCP from Operator to Target via Pivot."
    )
    parser.add_argument("--listen-host", type=ipv4_address, default=LISTEN_HOST,
                        help=f"IPv4 address on Pivot (default: {LISTEN_HOST})")
    parser.add_argument("--listen-port", type=tcp_port, default=LISTEN_PORT,
                        help=f"Pivot listening TCP port (default: {LISTEN_PORT})")
    parser.add_argument("--target-host", type=ipv4_address, default=TARGET_HOST,
                        help=f"Target IPv4 address (default: {TARGET_HOST})")
    parser.add_argument("--target-port", type=tcp_port, default=TARGET_PORT,
                        help=f"Target TCP port (default: {TARGET_PORT})")
    parser.add_argument("--max-sessions", type=positive_int, default=MAX_SESSIONS,
                        help=f"Maximum concurrent sessions (default: {MAX_SESSIONS})")
    parser.add_argument("--backlog", type=positive_int, default=BACKLOG,
                        help=f"Listen backlog (default: {BACKLOG})")
    parser.add_argument("--connect-timeout", type=positive_float,
                        default=CONNECT_TIMEOUT,
                        help=f"Target connect timeout in seconds (default: {CONNECT_TIMEOUT:g})")
    return parser.parse_args(argv)


def handle_client(operator_socket, address, config, slots, active, active_lock, stop_event):
    print(f"[+] Operator connected: {address}", flush=True)
    try:
        with operator_socket:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as target_socket:
                target_socket.settimeout(config.connect_timeout)
                target_socket.connect((config.target_host, config.target_port))
                target_socket.settimeout(None)

                with active_lock:
                    active[operator_socket] = target_socket

                if stop_event.is_set():
                    return

                print(f"[+] Connected to Target for {address}", flush=True)
                relay_bidirectional(operator_socket, target_socket)

    except OSError as error:
        print(f"[!] Session {address} failed: {error}", flush=True)
    finally:
        with active_lock:
            active.pop(operator_socket, None)
        slots.release()
        print(f"[-] Session ended: {address}", flush=True)


def interrupt_socket(sock):
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def main(argv=None):
    config = parse_args(argv)
    slots = threading.BoundedSemaphore(config.max_sessions)
    active = {}  # {operator_socket: target_socket or None}
    active_lock = threading.Lock()
    stop_event = threading.Event()
    workers = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((config.listen_host, config.listen_port))
        listener.listen(config.backlog)

        print(f"[+] Pivot listening on {config.listen_host}:{config.listen_port}", flush=True)
        print(f"[+] Forwarding to {config.target_host}:{config.target_port}", flush=True)
        print(f"[+] Max concurrent sessions: {config.max_sessions}", flush=True)

        try:
            while not stop_event.is_set():
                operator_socket, address = listener.accept()

                if not slots.acquire(blocking=False):
                    print(f"[!] Session limit reached; rejecting {address}", flush=True)
                    operator_socket.close()
                    continue

                with active_lock:
                    active[operator_socket] = None

                worker = threading.Thread(
                    target=handle_client,
                    args=(operator_socket, address, config, slots, active,
                          active_lock, stop_event),
                    name=f"operator-{address[0]}:{address[1]}",
                )
                try:
                    worker.start()
                except RuntimeError:
                    with active_lock:
                        active.pop(operator_socket, None)
                    operator_socket.close()
                    slots.release()
                    raise

                workers.append(worker)
                workers = [thread for thread in workers if thread.is_alive()]

        except KeyboardInterrupt:
            print("\n[+] Stopping forwarder...", flush=True)
        finally:
            stop_event.set()
            with active_lock:
                sessions = list(active.items())

            for operator_socket, target_socket in sessions:
                interrupt_socket(operator_socket)
                interrupt_socket(target_socket)

            for worker in workers:
                worker.join()

            print("[+] Forwarder stopped", flush=True)


if __name__ == "__main__":
    main()
