#!/usr/bin/env python3
import json
import mimetypes
import os
import re
import subprocess
import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.abspath(os.path.dirname(__file__))
PUBLIC_DIR = os.path.join(HERE, "public")
UNIT_RE = re.compile(r"^[A-Za-z0-9:._@\-]+$")
INVOCATION_RE = re.compile(r"^[0-9a-f]{32}$")


def run(cmd, timeout=4):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        msg = (p.stderr or p.stdout or "").strip()
        raise RuntimeError(msg or f"command failed: {' '.join(cmd)}")
    return p.stdout


def usec_to_ms(v):
    try:
        n = int(v)
    except Exception:
        return None
    if n <= 0:
        return None
    return n // 1000


def ms_to_iso(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def parse_unit_files(text):
    out = {}
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        parts = s.split(None, 1)
        if len(parts) != 2:
            continue
        unit, state = parts
        out[unit] = {"unit": unit, "unitFileState": state}
    return out


def parse_units(text):
    out = {}
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        parts = s.split(None, 4)
        if len(parts) < 4:
            continue
        unit, load, active, sub = parts[0], parts[1], parts[2], parts[3]
        desc = parts[4] if len(parts) == 5 else ""
        out[unit] = {
            "unit": unit,
            "loadState": load,
            "activeState": active,
            "subState": sub,
            "description": desc,
        }
    return out


def list_units():
    unit_files = run([
        "systemctl",
        "list-unit-files",
        "--no-pager",
        "--no-legend",
    ], timeout=8)
    units = run([
        "systemctl",
        "list-units",
        "--all",
        "--no-pager",
        "--no-legend",
    ], timeout=8)

    a = parse_unit_files(unit_files)
    b = parse_units(units)
    merged = {}
    for unit in set(a.keys()) | set(b.keys()):
        item = {"unit": unit}
        if unit in a:
            item.update(a[unit])
        if unit in b:
            item.update(b[unit])
        merged[unit] = item

    items = list(merged.values())
    items.sort(key=lambda x: x.get("unit", ""))
    return items


def list_targets():
    targets = run([
        "systemctl",
        "list-units",
        "--type=target",
        "--all",
        "--no-pager",
        "--no-legend",
    ], timeout=8)
    items = list(parse_units(targets).values())
    items.sort(key=lambda x: x.get("unit", ""))
    return items


def list_timers():
    out = run([
        "systemctl",
        "list-timers",
        "--all",
        "--no-pager",
        "--output=json",
    ], timeout=8)
    items = json.loads(out or "[]")
    res = []
    for it in items:
        next_ms = usec_to_ms(it.get("next"))
        last_ms = usec_to_ms(it.get("last"))
        res.append({
            "timer": it.get("unit"),
            "activates": it.get("activates"),
            "nextMs": next_ms,
            "nextIso": ms_to_iso(next_ms),
            "lastMs": last_ms,
            "lastIso": ms_to_iso(last_ms),
        })
    res.sort(key=lambda x: x.get("timer") or "")
    return res


def schedule_for_unit(unit):
    if not unit or len(unit) > 200 or not UNIT_RE.fullmatch(unit):
        raise ValueError("invalid unit")

    timers = list_timers()
    by_timer = {t.get("timer"): t for t in timers if t.get("timer")}

    if unit.endswith(".timer"):
        t = by_timer.get(unit)
        return {
            "unit": unit,
            "kind": "timer",
            "timer": unit,
            "activates": t.get("activates") if t else None,
            "nextMs": t.get("nextMs") if t else None,
            "nextIso": t.get("nextIso") if t else None,
            "lastMs": t.get("lastMs") if t else None,
            "lastIso": t.get("lastIso") if t else None,
        }

    if unit.endswith(".service"):
        candidates = [t for t in timers if t.get("activates") == unit]
        candidates.sort(key=lambda x: (x.get("nextMs") is None, x.get("nextMs") or 0))
        t = candidates[0] if candidates else None
        return {
            "unit": unit,
            "kind": "service",
            "timer": t.get("timer") if t else None,
            "activates": unit,
            "nextMs": t.get("nextMs") if t else None,
            "nextIso": t.get("nextIso") if t else None,
            "lastMs": t.get("lastMs") if t else None,
            "lastIso": t.get("lastIso") if t else None,
        }

    return {"unit": unit, "kind": "other"}


def resolve_log_unit(unit):
    if unit.endswith(".timer"):
        s = schedule_for_unit(unit)
        a = s.get("activates")
        if a and isinstance(a, str) and UNIT_RE.fullmatch(a):
            return a
    return unit


def list_runs(unit, limit=10):
    if not unit or len(unit) > 200 or not UNIT_RE.fullmatch(unit):
        raise ValueError("invalid unit")
    if limit < 1 or limit > 50:
        raise ValueError("invalid limit")

    log_unit = resolve_log_unit(unit)

    scan = run([
        "journalctl",
        f"--unit={log_unit}",
        "-n",
        "20000",
        "--no-pager",
        "-o",
        "json",
    ], timeout=14)

    inv_ids = []
    inv_set = set()
    for line in scan.splitlines():
        if not line:
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        inv = e.get("INVOCATION_ID")
        if not inv or not INVOCATION_RE.fullmatch(inv):
            continue
        if inv in inv_set:
            continue
        inv_set.add(inv)
        inv_ids.append(inv)
        if len(inv_ids) >= limit:
            break

    def upd_status(prev, msg):
        m = (msg or "").lower()
        if "failed" in m or "error" in m or "exited" in m:
            return "failed"
        if "deactivated successfully" in m or m.startswith("finished "):
            return prev if prev == "failed" else "success"
        return prev

    runs = []
    for inv in inv_ids:
        raw = run([
            "journalctl",
            f"--unit={log_unit}",
            f"INVOCATION_ID={inv}",
            "--no-pager",
            "-o",
            "json",
        ], timeout=10)

        start_ms = None
        end_ms = None
        status = "unknown"
        cpu_usage = None

        for line in raw.splitlines():
            if not line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                continue
            ts_ms = usec_to_ms(e.get("__REALTIME_TIMESTAMP"))
            if ts_ms is not None:
                start_ms = ts_ms if start_ms is None else min(start_ms, ts_ms)
                end_ms = ts_ms if end_ms is None else max(end_ms, ts_ms)

            cpu = e.get("CPU_USAGE_NSEC")
            try:
                cpu_n = int(cpu) if cpu is not None else None
            except Exception:
                cpu_n = None
            if cpu_n is not None:
                cpu_usage = cpu_n if cpu_usage is None else max(cpu_usage, cpu_n)

            status = upd_status(status, e.get("MESSAGE"))

        duration_ms = (end_ms - start_ms) if (start_ms is not None and end_ms is not None) else None
        runs.append({
            "invocationId": inv,
            "startMs": start_ms,
            "startIso": ms_to_iso(start_ms),
            "endMs": end_ms,
            "endIso": ms_to_iso(end_ms),
            "durationMs": duration_ms,
            "status": status,
            "cpuUsageNsec": cpu_usage,
        })

    return {"logUnit": log_unit, "runs": runs}


def logs_for_invocation(unit, invocation_id, limit=400):
    if not unit or len(unit) > 200 or not UNIT_RE.fullmatch(unit):
        raise ValueError("invalid unit")
    if not invocation_id or not INVOCATION_RE.fullmatch(invocation_id):
        raise ValueError("invalid invocation")
    if limit < 1 or limit > 5000:
        raise ValueError("invalid limit")

    log_unit = resolve_log_unit(unit)

    out = run([
        "journalctl",
        f"--unit={log_unit}",
        f"INVOCATION_ID={invocation_id}",
        "-n",
        str(limit),
        "--no-pager",
        "-o",
        "cat",
    ], timeout=12)
    return out


def units_for_targets(targets):
    if not targets:
        return list_units()

    wanted = set()
    for t in targets:
        if not t or len(t) > 200 or not UNIT_RE.fullmatch(t) or not t.endswith(".target"):
            continue
        deps = run([
            "systemctl",
            "list-dependencies",
            "--all",
            "--plain",
            "--no-pager",
            "--no-legend",
            "--",
            t,
        ], timeout=10)
        wanted.add(t)
        for line in deps.splitlines():
            s = line.lstrip(" \t●○*├└│─")
            if not s:
                continue
            u = s.split(None, 1)[0]
            if UNIT_RE.fullmatch(u):
                wanted.add(u)

    if not wanted:
        return list_units()

    items = list_units()
    return [u for u in items if u.get("unit") in wanted]


def unit_detail(unit):
    if not unit or len(unit) > 200 or not UNIT_RE.fullmatch(unit):
        raise ValueError("invalid unit")

    props_out = run([
        "systemctl",
        "show",
        "--no-pager",
        "-p",
        "Id",
        "-p",
        "Description",
        "-p",
        "LoadState",
        "-p",
        "ActiveState",
        "-p",
        "SubState",
        "-p",
        "UnitFileState",
        "-p",
        "FragmentPath",
        "-p",
        "DropInPaths",
        "-p",
        "Documentation",
        "-p",
        "After",
        "-p",
        "Requires",
        "-p",
        "Wants",
        "--",
        unit,
    ], timeout=6)

    props = {}
    for line in props_out.splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        props[k] = v

    cat_out = run(["systemctl", "cat", "--no-pager", "--", unit], timeout=6)
    return {"unit": unit, "properties": props, "cat": cat_out}


class Handler(BaseHTTPRequestHandler):
    server_version = "systemd-ui/0"

    def log_message(self, format, *args):
        return

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, status, text, content_type="text/plain; charset=utf-8"):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/targets":
            try:
                items = list_targets()
                self.send_json(HTTPStatus.OK, {"targets": items})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path == "/api/timers":
            try:
                items = list_timers()
                self.send_json(HTTPStatus.OK, {"timers": items})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path.startswith("/api/schedule/"):
            unit = urllib.parse.unquote(path[len("/api/schedule/"):])
            try:
                data = schedule_for_unit(unit)
                self.send_json(HTTPStatus.OK, data)
            except ValueError as e:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path.startswith("/api/runs/"):
            unit = urllib.parse.unquote(path[len("/api/runs/"):])
            try:
                qs = urllib.parse.parse_qs(parsed.query)
                limit = int((qs.get("limit", ["10"])[0]) or "10")
                data = list_runs(unit, limit=limit)
                self.send_json(HTTPStatus.OK, {"unit": unit, "logUnit": data.get("logUnit"), "runs": data.get("runs")})
            except ValueError as e:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path.startswith("/api/logs/"):
            rest = path[len("/api/logs/"):]
            if "/" not in rest:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid path"})
                return
            unit_enc, inv_enc = rest.split("/", 1)
            unit = urllib.parse.unquote(unit_enc)
            inv = urllib.parse.unquote(inv_enc)
            try:
                qs = urllib.parse.parse_qs(parsed.query)
                limit = int((qs.get("limit", ["400"])[0]) or "400")
                txt = logs_for_invocation(unit, inv, limit=limit)
                self.send_json(HTTPStatus.OK, {"unit": unit, "invocationId": inv, "logs": txt})
            except ValueError as e:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path == "/api/units":
            try:
                qs = urllib.parse.parse_qs(parsed.query)
                raw = qs.get("targets", [])
                targets = []
                for v in raw:
                    for t in v.split(","):
                        t = t.strip()
                        if t:
                            targets.append(t)
                items = units_for_targets(targets)
                self.send_json(HTTPStatus.OK, {"units": items})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        if path.startswith("/api/unit/"):
            unit = urllib.parse.unquote(path[len("/api/unit/"):])
            try:
                detail = unit_detail(unit)
                self.send_json(HTTPStatus.OK, detail)
            except ValueError as e:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            except Exception as e:
                self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return

        self.serve_static(path)

    def serve_static(self, path):
        if path == "/":
            path = "/index.html"

        safe = os.path.normpath(path).lstrip("/")
        if safe.startswith(".."): 
            self.send_text(HTTPStatus.NOT_FOUND, "not found")
            return

        file_path = os.path.join(PUBLIC_DIR, safe)
        if not os.path.isfile(file_path):
            self.send_text(HTTPStatus.NOT_FOUND, "not found")
            return

        ctype, _ = mimetypes.guess_type(file_path)
        if not ctype:
            ctype = "application/octet-stream"

        with open(file_path, "rb") as f:
            data = f.read()

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "5173"))
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
