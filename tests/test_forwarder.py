from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import signal
import threading
import socket
import subprocess
import sys
import time

import pytest

import forwarder
from conftest import ROOT, wait_for


def available_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def communicate(port, payload=b"hello", timeout=5):
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        parts = []
        while True:
            data = sock.recv(16384)
            if not data:
                return b"".join(parts)
            parts.append(data)


@contextmanager
def running_forwarder(tmp_path, *, target_port, max_sessions=10):
    listen_port = available_port()
    log_path = tmp_path / f"forwarder_{listen_port}.log"
    args = [
        sys.executable, "-u", str(ROOT / "src/pypivot/forwarder.py"),
        "--listen-host", "127.0.0.1",
        "--listen-port", str(listen_port),
        "--target-host", "127.0.0.1",
        "--target-port", str(target_port),
        "--max-sessions", str(max_sessions),
        "--connect-timeout", "1",
    ]
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    ready = wait_for(
        lambda: "Pivot listening" in log_path.read_text(encoding="utf-8") or process.poll() is not None,
        timeout=5,
    )
    if not ready or process.poll() is not None:
        process.kill()
        process.wait(timeout=3)
        pytest.fail(f"forwarder did not start:\n{log_path.read_text(encoding='utf-8')}")
    try:
        yield process, listen_port, log_path
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=4)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=4)


def test_default_arguments():
    config = forwarder.parse_args([])
    assert config.listen_host == "172.29.10.20"
    assert config.listen_port == 8888
    assert config.target_host == "172.29.20.10"
    assert config.target_port == 9900
    assert config.max_sessions == 10


def test_argument_overrides():
    config = forwarder.parse_args([
        "--listen-host", "127.0.0.1", "--listen-port", "12345",
        "--target-host", "127.0.0.1", "--target-port", "54321",
        "--max-sessions", "3", "--backlog", "4", "--connect-timeout", "1.5",
    ])
    assert (config.listen_host, config.listen_port) == ("127.0.0.1", 12345)
    assert (config.target_host, config.target_port) == ("127.0.0.1", 54321)
    assert (config.max_sessions, config.backlog, config.connect_timeout) == (3, 4, 1.5)


@pytest.mark.parametrize("args", [
    ["--listen-host", "bad-ip"],
    ["--target-host", "999.1.1.1"],
    ["--listen-port", "0"],
    ["--target-port", "65536"],
    ["--max-sessions", "0"],
    ["--backlog", "-1"],
    ["--connect-timeout", "nan"],
    ["--connect-timeout", "inf"],
    ["--connect-timeout", "0"],
])
def test_rejects_invalid_arguments(args):
    with pytest.raises(SystemExit) as exc:
        forwarder.parse_args(args)
    assert exc.value.code == 2


def test_cli_help(capsys):
    with pytest.raises(SystemExit) as exc:
        forwarder.parse_args(["--help"])
    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "--listen-host" in output
    assert "--target-port" in output
    assert "--max-sessions" in output


def test_forwarder_relays_echo(tmp_path, echo_server):
    with running_forwarder(tmp_path, target_port=echo_server.server_address[1]) as (proc, port, log):
        assert communicate(port, b"abc\x00\xffxyz") == b"abc\x00\xffxyz"
        assert proc.poll() is None


def test_five_concurrent_connections(tmp_path, echo_server):
    with running_forwarder(tmp_path, target_port=echo_server.server_address[1]) as (_, port, __):
        def exchange(i):
            payload = bytes([i]) * (16384 + i)
            return communicate(port, payload) == payload
        with ThreadPoolExecutor(max_workers=5) as pool:
            assert all(pool.map(exchange, range(1, 6)))


def test_session_limit_and_slot_release(tmp_path, echo_server):
    with running_forwarder(tmp_path, target_port=echo_server.server_address[1], max_sessions=2) as (proc, port, log):
        first = socket.create_connection(("127.0.0.1", port), timeout=3)
        second = socket.create_connection(("127.0.0.1", port), timeout=3)
        third = None
        try:
            assert wait_for(lambda: echo_server.accepted_count >= 2, timeout=4)
            third = socket.create_connection(("127.0.0.1", port), timeout=3)
            third.settimeout(3)
            try:
                rejected = third.recv(1) == b""
            except ConnectionResetError:
                rejected = True
            assert rejected, "third session must be rejected when the limit is 2"
            assert wait_for(lambda: "Session limit reached" in log.read_text(encoding="utf-8"))
            first.close()
            # Semaphore slot
            assert wait_for(lambda: "Session ended" in log.read_text(encoding="utf-8"))
            assert communicate(port, b"slot-free") == b"slot-free"
            assert proc.poll() is None
        finally:
            if third is not None:
                third.close()
            first.close()
            second.close()


def test_unavailable_target_does_not_stop_forwarder(tmp_path):
    target_port = available_port()
    with running_forwarder(tmp_path, target_port=target_port) as (proc, port, log):
        for _ in range(2):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as client:
                client.settimeout(3)
                try:
                    assert client.recv(1) == b""
                except ConnectionResetError:
                    pass
        assert wait_for(lambda: "Session ended" in log.read_text(encoding="utf-8"))
        assert proc.poll() is None


def test_ctrl_c_closes_active_sessions(tmp_path, echo_server):
    with running_forwarder(tmp_path, target_port=echo_server.server_address[1]) as (proc, port, log):
        client = socket.create_connection(("127.0.0.1", port), timeout=3)
        try:
            assert wait_for(lambda: echo_server.accepted_count >= 1)
            proc.send_signal(signal.SIGINT)
            assert wait_for(lambda: proc.poll() is not None, timeout=5)
            output = log.read_text(encoding="utf-8")
            assert "Forwarder stopped" in output, output
        finally:
            client.close()


def test_operator_reset_does_not_stop_forwarder(tmp_path, echo_server):
    import struct

    with running_forwarder(tmp_path, target_port=echo_server.server_address[1]) as (proc, port, log):
        client = socket.create_connection(("127.0.0.1", port), timeout=3)
        client.sendall(b"Nagly reset")
        client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        client.close()
        assert wait_for(lambda: "Session ended" in log.read_text(encoding="utf-8"))
        assert proc.poll() is None
        assert communicate(port, b"nas-tp") == b"nas-tp"


def test_target_reset_does_not_stop_forwarder(tmp_path):
    import socketserver
    import struct

    class ResetHandler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.sendall(b"A" * 4096)
            time.sleep(0.2)
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.request.close()

    class ResetServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    with ResetServer(("127.0.0.1", 0), ResetHandler) as target:
        target_worker = threading.Thread(target=target.serve_forever, daemon=True)
        target_worker.start()
        try:
            with running_forwarder(tmp_path, target_port=target.server_address[1]) as (proc, port, log):
                with socket.create_connection(("127.0.0.1", port), timeout=3) as client:
                    client.settimeout(4)
                    chunks = []
                    try:
                        while (data := client.recv(8192)):
                            chunks.append(data)
                    except ConnectionResetError:
                        pass
                assert b"".join(chunks) == b"A" * 4096
                assert wait_for(lambda: "Session ended" in log.read_text(encoding="utf-8"))
                assert proc.poll() is None
        finally:
            target.shutdown()
            target_worker.join(timeout=3)
