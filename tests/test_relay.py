import hashlib
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import relay


@contextmanager
def bridge():
    operator_client, operator_relay = socket.socketpair()
    target_relay, target_server = socket.socketpair()
    worker_errors = []
    for sock in (operator_client, target_server):
        sock.settimeout(7)

    def run_relay():
        try:
            relay.relay_bidirectional(operator_relay, target_relay)
        except BaseException as exc:
            worker_errors.append(exc)

    worker = threading.Thread(target=run_relay, daemon=True)
    worker.start()
    try:
        yield operator_client, target_server, worker
    finally:
        for sock in (operator_client, target_server):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        worker.join(timeout=4)
        assert not worker.is_alive(), "relay thread did not finish"
        assert not worker_errors, f"unexpected exception: {worker_errors}"


def read_to_eof(sock, *, chunk=8192):
    blocks = []
    while True:
        data = sock.recv(chunk)
        if not data:
            return b"".join(blocks)
        blocks.append(data)


def send_and_finish(sock, data):
    sock.sendall(data)
    sock.shutdown(socket.SHUT_WR)


def test_forwards_data_in_both_directions():
    with bridge() as (operator, target, worker):
        operator.sendall(b"Hello")
        assert target.recv(5) == b"Hello"
        target.sendall(b"Response")
        assert operator.recv(9) == b"Response"
        operator.shutdown(socket.SHUT_WR)
        target.shutdown(socket.SHUT_WR)
        assert read_to_eof(operator) == b""
        assert read_to_eof(target) == b""
        worker.join(timeout=3)
        assert not worker.is_alive()


def test_half_close_allows_reply():
    with bridge() as (operator, target, worker):
        send_and_finish(operator, b"Request")
        assert read_to_eof(target) == b"Request"
        send_and_finish(target, b"Reply after EOF")
        assert read_to_eof(operator) == b"Reply after EOF"
        worker.join(timeout=3)
        assert not worker.is_alive()


def test_binary_one_mib_transfer_in_both_directions():
    request = bytes(range(256)) * 4096
    reply = request[::-1]
    with bridge() as (operator, target, worker):
        with ThreadPoolExecutor(max_workers=2) as pool:
            writer = pool.submit(send_and_finish, operator, request)
            received_request = read_to_eof(target)
            writer.result(timeout=6)
            writer = pool.submit(send_and_finish, target, reply)
            received_reply = read_to_eof(operator)
            writer.result(timeout=6)
        assert len(received_request) == 1_048_576
        assert hashlib.sha256(received_request).digest() == hashlib.sha256(request).digest()
        assert received_request == request
        assert received_reply == reply
        worker.join(timeout=3)
        assert not worker.is_alive()


def test_slow_receiver_and_small_buffer_preserve_data(monkeypatch):
    monkeypatch.setattr(relay, "MAX_PENDING_BYTES", 4096)
    monkeypatch.setattr(relay, "BUFFER_SIZE", 1024)
    payload = bytes(range(256)) * 2048  # 512 KiB
    with bridge() as (operator, target, worker):
        with ThreadPoolExecutor(max_workers=1) as pool:
            writer = pool.submit(send_and_finish, operator, payload)
            blocks = []
            while True:
                data = target.recv(2048)
                if not data:
                    break
                blocks.append(data)
                time.sleep(0.0002)
            writer.result(timeout=6)
        assert b"".join(blocks) == payload
        send_and_finish(target, b"OK")
        assert read_to_eof(operator) == b"OK"
        worker.join(timeout=3)
        assert not worker.is_alive()


def test_idle_timeout_closes_session(monkeypatch):
    monkeypatch.setattr(relay, "IDLE_TIMEOUT", 0.25)
    monkeypatch.setattr(relay, "SELECT_TIMEOUT", 0.05)
    with bridge() as (operator, target, worker):
        worker.join(timeout=3)
        assert not worker.is_alive()
        assert operator.recv(1) == b""
        assert target.recv(1) == b""
