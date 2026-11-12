from pathlib import Path
import socketserver
import sys
import threading
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "pypivot"
if str(SOURCE) not in sys.path:
    sys.path.insert(0, str(SOURCE))


def pytest_addoption(parser):
    parser.addoption(
        "--run-docker", action="store_true", default=False,
        help="also run the Docker Compose integration test",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-docker"):
        return
    skip = pytest.mark.skip(reason="test Docker: add --run-docker")
    for item in items:
        if "docker" in item.keywords:
            item.add_marker(skip)


class _EchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        server = self.server
        with server.lock:
            server.accepted_count += 1
        while True:
            data = self.request.recv(16384)
            if not data:
                break
            self.request.sendall(data)


class _EchoServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _EchoHandler)
        self.lock = threading.Lock()
        self.accepted_count = 0


@pytest.fixture
def echo_server():
    server = _EchoServer()
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)


def wait_for(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())
