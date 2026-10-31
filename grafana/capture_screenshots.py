"""Capture full-page screenshots of the provisioned Grafana dashboards via Chrome CDP."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import socket
import struct
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000").rstrip("/")
DASHBOARDS = {
    "exec-overview": "executive-overview",
    "customer-activity": "customer-activity",
    "payment-monitoring": "payment-monitoring",
    "inventory-shipping": "inventory-and-shipping",
    "data-engineering": "data-engineering-monitoring",
}
requested = os.getenv("GRAFANA_SCREENSHOT_UIDS")
if requested:
    selected = {uid: slug for uid, slug in DASHBOARDS.items() if uid in requested.split(",")}
    if not selected:
        raise ValueError("GRAFANA_SCREENSHOT_UIDS did not match a configured dashboard UID")
    DASHBOARDS = selected


def get_json(url: str, *, method: str = "GET") -> dict[str, Any]:
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


class DevTools:
    """Small standard-library WebSocket client for Chrome's local DevTools endpoint."""

    def __init__(self, websocket_url: str) -> None:
        parsed = urlparse(websocket_url)
        self.socket = socket.create_connection((parsed.hostname, parsed.port or 80), timeout=10)
        key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        handshake = (
            f"GET {parsed.path} HTTP/1.1\r\nHost: {parsed.netloc}\r\n"
            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
            "Origin: http://localhost\r\n\r\n"
        )
        self.socket.sendall(handshake.encode("ascii"))
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("Chrome closed the DevTools handshake")
            response.extend(chunk)
        headers, remainder = bytes(response).split(b"\r\n\r\n", 1)
        if b" 101 " not in headers.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"DevTools WebSocket upgrade failed: {headers[:200]!r}")
        self.pending = bytearray(remainder)
        self.message_id = 0

    def _send_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        mask = secrets.token_bytes(4)
        length = len(payload)
        if length < 126:
            header = bytes((0x81, 0x80 | length))
        elif length <= 0xFFFF:
            header = bytes((0x81, 0xFE)) + struct.pack("!H", length)
        else:
            header = bytes((0x81, 0xFF)) + struct.pack("!Q", length)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self.socket.sendall(header + mask + masked)

    def _read_exactly(self, length: int) -> bytes:
        while len(self.pending) < length:
            chunk = self.socket.recv(max(4096, length - len(self.pending)))
            if not chunk:
                raise ConnectionError("Chrome closed the DevTools connection")
            self.pending.extend(chunk)
        result = bytes(self.pending[:length])
        del self.pending[:length]
        return result

    def _read_text(self) -> str:
        parts: list[bytes] = []
        while True:
            first, second = self._read_exactly(2)
            final, opcode = bool(first & 0x80), first & 0x0F
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._read_exactly(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._read_exactly(8))[0]
            mask = self._read_exactly(4) if second & 0x80 else None
            chunk = self._read_exactly(length)
            if mask:
                chunk = bytes(value ^ mask[index % 4] for index, value in enumerate(chunk))
            if opcode == 0x8:
                raise ConnectionError("Chrome closed the DevTools WebSocket")
            if opcode in (0x1, 0x0):
                parts.append(chunk)
            if final:
                return b"".join(parts).decode("utf-8")

    def call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.message_id += 1
        message_id = self.message_id
        self._send_text(json.dumps({"id": message_id, "method": method, "params": params or {}}))
        while True:
            response = json.loads(self._read_text())
            if response.get("id") == message_id:
                if "error" in response:
                    raise RuntimeError(f"CDP {method}: {response['error']}")
                return response

    def close(self) -> None:
        self.socket.close()


def admin_password() -> str:
    password = os.environ.get("GRAFANA_ADMIN_PASSWORD")
    if password:
        return password
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("GRAFANA_ADMIN_PASSWORD="):
                password = line.split("=", 1)[1].strip().strip("\"'")
                if password:
                    return password
    raise RuntimeError("Set GRAFANA_ADMIN_PASSWORD in the process environment or project .env file.")


def chrome_executable() -> Path:
    options = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    ]
    for executable in options:
        if executable.exists():
            return executable
    raise FileNotFoundError("Chrome or Edge was not found; no browser was installed by this script.")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def main() -> None:
    output_dir = ROOT / "docs" / "screenshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="grafana-shot-") as profile:
        process = subprocess.Popen(
            [str(chrome_executable()), "--headless=new", "--disable-gpu", "--no-first-run",
             "--no-default-browser-check", f"--remote-debugging-port={port}",
             "--remote-allow-origins=*", "--disable-backgrounding-occluded-windows",
             "--disable-renderer-backgrounding", "--disable-background-timer-throttling",
             f"--user-data-dir={profile}", "about:blank"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        devtools: DevTools | None = None
        try:
            version_url = f"http://127.0.0.1:{port}/json/version"
            for _ in range(40):
                try:
                    get_json(version_url)
                    break
                except Exception:
                    if process.poll() is not None:
                        raise RuntimeError("Headless browser exited before DevTools started")
                    time.sleep(0.25)
            else:
                raise TimeoutError("Headless browser DevTools endpoint did not start")

            tab = get_json(f"http://127.0.0.1:{port}/json/new?about:blank", method="PUT")
            devtools = DevTools(tab["webSocketDebuggerUrl"])
            auth = base64.b64encode(f"admin:{admin_password()}".encode("utf-8")).decode("ascii")
            devtools.call("Network.enable")
            devtools.call("Network.setExtraHTTPHeaders", {"headers": {"Authorization": f"Basic {auth}"}})
            devtools.call("Emulation.setDeviceMetricsOverride", {
                "width": 1600, "height": 1800, "deviceScaleFactor": 1, "mobile": False,
            })
            for uid, slug in DASHBOARDS.items():
                url = f"{GRAFANA_URL}/d/{uid}/{slug}?orgId=1&from=now-30d&to=now&timezone=UTC"
                devtools.call("Page.bringToFront")
                devtools.call("Page.navigate", {"url": url})
                expected_titles = [panel["title"] for panel in json.loads((ROOT / "grafana" / "dashboards" / f"{uid}.json").read_text(encoding="utf-8"))["panels"]]
                missing = expected_titles
                for _ in range(60):
                    page_text = devtools.call("Runtime.evaluate", {
                        "expression": "document.body.innerText", "returnByValue": True,
                    })["result"]["result"].get("value", "")
                    missing = [title for title in expected_titles if title not in page_text]
                    if not missing:
                        break
                    time.sleep(0.5)
                if missing:
                    raise RuntimeError(f"{uid} did not render panel titles: {', '.join(missing)}")
                time.sleep(12)
                title = expected_titles[0]
                diagnostics = devtools.call("Runtime.evaluate", {
                    "expression": "JSON.stringify((()=>{const title=" + json.dumps(title) + ";const e=[...document.querySelectorAll('h1,h2,h3,div,span')].find(n=>n.childElementCount===0&&n.textContent.trim()===title);const r=e?.getBoundingClientRect();return {visibility:document.visibilityState,panelContents:document.querySelectorAll('[data-testid=panel-content]').length,svgs:document.querySelectorAll('svg').length,canvases:document.querySelectorAll('canvas').length,loading:document.body.innerText.includes('Loading'),errors:document.body.innerText.includes('Query error'),titleBox:r?{x:r.x,y:r.y,w:r.width,h:r.height,display:getComputedStyle(e).display,visibility:getComputedStyle(e).visibility}:null,bodyEnd:document.body.innerText.slice(-300)}})())",
                    "returnByValue": True,
                })["result"]["result"].get("value", "{}")
                metrics = devtools.call("Page.getLayoutMetrics")["result"]
                size = metrics.get("cssContentSize") or metrics["contentSize"]
                height = max(1800, min(int(size["height"] + 0.999), 8000))
                if height > 1800:
                    devtools.call("Emulation.setDeviceMetricsOverride", {
                        "width": 1600, "height": height, "deviceScaleFactor": 1, "mobile": False,
                    })
                    time.sleep(1)
                result = devtools.call("Page.captureScreenshot", {"format": "png", "fromSurface": True})
                image = base64.b64decode(result["result"]["data"])
                path = output_dir / f"{uid}.png"
                path.write_bytes(image)
                print(f"Captured {path.relative_to(ROOT)} ({size['width']}x{height}, {len(image):,} bytes; {diagnostics})")
        finally:
            if devtools:
                devtools.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    main()
