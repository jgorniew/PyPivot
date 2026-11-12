from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import uuid

import pytest

from conftest import ROOT, wait_for


def _docker(*args, timeout=20, check=True):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )


def _require_running_compose():
    if shutil.which("docker") is None:
        pytest.fail("Docker CLI nie jest zainstalowany")
    if not (ROOT / "compose.yaml").exists() and not (ROOT / "docker-compose.yml").exists():
        pytest.fail("Missing compose.yaml in the project directory")
    try:
        result = _docker("version", timeout=8, check=False)
    except (OSError, subprocess.TimeoutExpired):
        pytest.fail("Docker Compose is not responding")
    if result.returncode != 0:
        pytest.fail("Docker Compose is unavailable")
    try:
        result = _docker("ps", "--services", "--status", "running", timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired):
        pytest.fail("Cannot read container status")
    if result.returncode != 0:
        pytest.fail("Docker Compose: containers are unavailable")
    services = set(result.stdout.split())
    if not {"operator", "pivot", "target"}.issubset(services):
        pytest.fail("Start the containers first: docker compose up -d")


def _launch_in_container(service, sh_script, log_path):
    logfile = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            ["docker", "compose", "exec", "-T", service, "sh", "-c", sh_script],
            cwd=ROOT,
            stdout=logfile,
            stderr=subprocess.STDOUT,
        )
    finally:
        logfile.close()
    return proc


def _wait_for_log(proc, log_path, expected, timeout=12):
    ok = wait_for(
        lambda: expected in log_path.read_text(encoding="utf-8") or proc.poll() is not None,
        timeout=timeout,
    )
    if not ok or proc.poll() is not None:
        pytest.fail(f"Docker process did not start: {expected}\nLog:\n{log_path.read_text(encoding='utf-8')}")


def _stop_in_container(service, pidfile, proc):
    shell = f'if [ -f "{pidfile}" ]; then kill -TERM "$(cat "{pidfile}")" 2>/dev/null || true; rm -f "{pidfile}"; fi'
    try:
        _docker("exec", "-T", service, "sh", "-c", shell, timeout=8, check=False)
    except (OSError, subprocess.TimeoutExpired):
        pass
    if proc is not None:
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)


@pytest.mark.docker
def test_docker_network_isolation_http_transfer_and_concurrency(tmp_path):
    _require_running_compose()

    suffix = uuid.uuid4().hex[:10]
    filename = f"pypivot_pytest_{suffix}.bin"
    file_path = f"/tmp/{filename}"
    target_pidfile = f"/tmp/pypivot_pytest_target_{suffix}.pid"
    pivot_pidfile = f"/tmp/pypivot_pytest_pivot_{suffix}.pid"
    target_port = 20000 + secrets.randbelow(10000)
    listen_port = 30000 + secrets.randbelow(10000)
    target_process = None
    pivot_process = None
    target_log = tmp_path / "docker_target.log"
    pivot_log = tmp_path / "docker_pivot.log"

    try:
        _docker(
            "exec", "-T", "target", "python", "-c",
            f"from pathlib import Path; Path({file_path!r}).write_bytes(bytes(range(256)) * 4096)",
            timeout=12,
        )

        target_process = _launch_in_container(
            "target",
            f"echo $$ > {target_pidfile}; exec python -u -m http.server {target_port} "
            f"--bind 172.29.20.10 --directory /tmp",
            target_log,
        )
        _wait_for_log(target_process, target_log, "Serving HTTP on")

        pivot_process = _launch_in_container(
            "pivot",
            f"echo $$ > {pivot_pidfile}; exec python -u /app/src/pypivot/forwarder.py "
            f"--listen-host 172.29.10.20 --listen-port {listen_port} "
            f"--target-host 172.29.20.10 --target-port {target_port} --max-sessions 10",
            pivot_log,
        )
        _wait_for_log(pivot_process, pivot_log, "Pivot listening")

        operator_script = f'''
import hashlib
import socket
import urllib.request
from concurrent.futures import ThreadPoolExecutor

# Verify isolation: the Operator cannot connect to Target directly.
try:
    direct = socket.create_connection(("172.29.20.10", {target_port}), timeout=2)
except OSError:
    print("DIRECT_BLOCKED", flush=True)
else:
    direct.close()
    raise AssertionError("Operator unexpectedly reached Target directly")

expected = bytes(range(256)) * 4096
url = "http://172.29.10.20:{listen_port}/{filename}"

def get_one(_):
    with urllib.request.urlopen(url, timeout=15) as response:
        body = response.read()
        assert response.status == 200
        assert len(body) == 1048576
        assert hashlib.sha256(body).digest() == hashlib.sha256(expected).digest()
    return len(body)

with ThreadPoolExecutor(max_workers=5) as pool:
    results = list(pool.map(get_one, range(5)))
assert len(results) == 5 and all(x == 1048576 for x in results)
print("HTTP_200_AND_SHA256_OK", flush=True)
print("CONCURRENT_REQUESTS_5_OK", flush=True)
'''
        result = _docker("exec", "-T", "operator", "python", "-c", operator_script,
                         timeout=60, check=False)
        assert result.returncode == 0, (
            f"Operator: {result.stdout}\n{result.stderr}\n"
            f"Pivot:\n{pivot_log.read_text(encoding='utf-8')}\n"
            f"Target:\n{target_log.read_text(encoding='utf-8')}"
        )
        assert "DIRECT_BLOCKED" in result.stdout
        assert "HTTP_200_AND_SHA256_OK" in result.stdout
        assert "CONCURRENT_REQUESTS_5_OK" in result.stdout
        assert pivot_process.poll() is None

    finally:
        if pivot_process is not None:
            _stop_in_container("pivot", pivot_pidfile, pivot_process)
        if target_process is not None:
            _stop_in_container("target", target_pidfile, target_process)
        try:
            _docker("exec", "-T", "target", "rm", "-f", file_path, timeout=8, check=False)
        except (OSError, subprocess.TimeoutExpired):
            pass
