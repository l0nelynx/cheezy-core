"""Exercise a built core through YAML, the UDP tunnel inbound and its HTTP controller."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request


def port(kind):
    with socket.socket(socket.AF_INET, kind) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main(binary, model=None):
    token = secrets.token_hex(16)
    controller = port(socket.SOCK_STREAM)
    inbound = port(socket.SOCK_DGRAM)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def api(path, body=None, secret=token):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{controller}{path}", data=data,
                                     headers={"Authorization": "Bearer " + secret,
                                              "Content-Type": "application/json"})
        with opener.open(req, timeout=2) as response:
            result = response.read()
            return json.loads(result) if result else None

    with tempfile.TemporaryDirectory() as directory, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as echo:
        echo.bind(("127.0.0.1", 0))
        target = echo.getsockname()[1]
        conf = {
            "external-controller": f"127.0.0.1:{controller}", "secret": token,
            "proxies": [{"type": "direct", "name": node} for node in ("A", "B")],
            "proxy-groups": [{"type": "antilag", "name": "game", "proxies": ["A", "B"],
                              "probe-targets": [{"address": f"127.0.0.1:{target}"}],
                              "url": f"http://127.0.0.1:{controller}/version"}],
            "tunnels": [f"udp,127.0.0.1:{inbound},127.0.0.1:{target},game"],
            "rules": ["MATCH,game"],
        }
        config = Path(directory, "config.yaml")
        if model:
            Path(directory, "shadow.json").write_bytes(Path(model).read_bytes())
            conf["proxy-groups"][0]["shadow-model"] = "shadow.json"
        config.write_text(json.dumps(conf), encoding="utf-8")
        def serve():
            while True:
                try:
                    data, remote = echo.recvfrom(4096)
                    if data == b"simulate-stall":
                        continue
                    if len(data) == 20 and data[:2] == b"\x00\x01":
                        echo.sendto(b"\x01\x01" + data[2:], remote)
                        continue
                    echo.sendto(str(remote[1]).encode() + b":" + data, remote)
                except OSError:
                    return
        threading.Thread(target=serve, daemon=True).start()
        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        with tempfile.TemporaryFile() as log:
            proc = subprocess.Popen([binary, "-d", directory, "-f", str(config)],
                                    stdout=log, stderr=log, creationflags=flags)
            try:
                for _ in range(100):
                    try:
                        api("/version")
                        break
                    except OSError:
                        if proc.poll() is not None:
                            log.seek(0)
                            raise RuntimeError(log.read().decode(errors="replace"))
                        time.sleep(.1)
                else:
                    raise RuntimeError("controller did not start")
                group = api("/group/game")
                assert group["shadowStatus"] == ("ready" if model else "disabled")
                if model:
                    assert group["shadowModelId"]
                try:
                    api("/group/game/bindings", secret="wrong")
                except urllib.error.HTTPError as exc:
                    assert exc.code == 401
                else:
                    raise AssertionError("bindings accessible with an invalid secret")
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
                    client.settimeout(4)
                    def exchange():
                        client.sendto(b"game", ("127.0.0.1", inbound))
                        data, _ = client.recvfrom(4096)
                        assert data.endswith(b":game")
                        return data
                    before = exchange()
                    binding, = api("/group/game/bindings")
                    def displayed():
                        return next(c for c in api("/connections")["connections"]
                                    if any(r["id"] == binding["id"] for r in c.get("udpRoutes", [])))
                    initial = displayed()
                    assert initial["chains"] == [binding["active"], "game"]
                    candidate = "B" if binding["active"] == "A" else "A"
                    command = {"id": binding["id"], "candidate": candidate}
                    for _ in range(20):
                        exchange()
                        time.sleep(.02)
                    for _ in range(25):
                        client.sendto(b"simulate-stall", ("127.0.0.1", inbound))
                        time.sleep(.02)
                    api("/group/game/failover", command)
                    after = exchange()
                    assert before != after
                    api("/group/game/failover", command)
                    assert exchange() == after
                    final, = api("/group/game/bindings")
                    assert final["active"] == candidate and final["id"] == binding["id"]
                    assert final["receiveGapMs"] is not None
                    old = next(c for c in final["candidates"] if c["node"] == binding["active"])
                    assert old["stalls"]["events"] == 1 and old["stalls"]["excessSeconds"] > 0
                    assert old["stalls"]["perActiveMinute"] > 0
                    decisions = api("/group/game/decisions")
                    current, previous = [d for d in decisions if d["binding"] == binding["id"]]
                    assert previous["status"] == "manual" and previous["events"] == 1
                    assert previous["destinationRegion"] == "unknown" and previous["geoSource"] == "unknown"
                    assert previous["firstResponseMs"] is not None and previous["activeSeconds"] > 0
                    assert current["selected"] == candidate and current["status"] == "active"
                    updated = displayed()
                    assert updated["id"] == initial["id"]
                    assert updated["chains"] == [candidate, "game"]
                    print("PASS", before, "->", after, "gap ms:", final["receiveGapMs"])
            finally:
                proc.terminate()
                proc.wait(timeout=5)


if __name__ == "__main__":
    main(str(Path(sys.argv[1]).resolve()), *sys.argv[2:])
