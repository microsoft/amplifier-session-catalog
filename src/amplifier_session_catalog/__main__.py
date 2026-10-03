"""Long-lived JSON-RPC stdio process. Standard output contains protocol only."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import sys
import threading

from .catalog import Catalog
from .discovery import Discovery
from .hints import HintInbox


class Server:
    def __init__(self, catalog, discovery):
        self.catalog = catalog
        self.discovery = discovery
        self.methods = {"list": catalog.list, "get": catalog.get, "upsert": catalog.upsert, "claimNative": catalog.claim_native,
                        "workspace": catalog.workspace, "projectSessionArchives":catalog.project_session_archives, "listWorkspaces": catalog.list_workspaces, "getWorkspace": catalog.get_workspace,
                        "projectWorkspaces": catalog.project_workspaces, "workspaceProjectionStatus": catalog.workspace_projection_status, "recheck": catalog.recheck_workspaces,
                        "hint": discovery.hint, "scan": discovery.start, "status": catalog.progress,
                        "projectSessionVisibility": catalog.project_session_visibility,
                        "sessionVisibilityProjectionStatus": catalog.session_visibility_projection_status,
                        "sessionVisibilityProjectionReceipt": catalog.session_visibility_projection_receipt,
                        "setVisibility": catalog.set_visibility, "resolveNative": catalog.resolve_native, "directoryInUse": catalog.directory_in_use,
                        "retentionCandidates":catalog.retention_candidates,"descendants":catalog.descendants}

    def handle(self, request):
        identity = request.get("id") if isinstance(request, dict) else None
        try:
            if not isinstance(request, dict) or request.get("jsonrpc") != "2.0" or not isinstance(request.get("method"), str):
                return {"jsonrpc": "2.0", "id": identity, "error": {"code": -32600, "message": "Invalid JSON-RPC request"}}
            method = self.methods.get(request["method"])
            if method is None:
                return ({"jsonrpc": "2.0", "id": identity, "error": {"code": -32601, "message": "Method not found"}}
                        if "id" in request else None)
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("params must be an object")
            result = method(**params)
            response = {"jsonrpc": "2.0", "id": identity, "result": result}
        except (ValueError, TypeError) as exc:
            response = {"jsonrpc": "2.0", "id": identity, "error": {"code": -32602, "message": str(exc)}}
        except Exception as exc:
            print(f"catalog request failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            response = {"jsonrpc": "2.0", "id": identity, "error": {"code": -32603, "message": "Catalog unavailable; preserve the index and inspect diagnostics"}}
        return response if "id" in request else None

    def serve(self):
        output_lock = threading.Lock()
        slots = threading.BoundedSemaphore(64)

        def complete(future):
            try:
                response = future.result()
                if response is not None:
                    with output_lock:
                        print(json.dumps(response, ensure_ascii=False), flush=True)
            finally:
                slots.release()

        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="catalog-query") as pool:
            while True:
                raw = sys.stdin.buffer.readline(1024 * 1024 + 1)
                if not raw:
                    break
                if len(raw) > 1024 * 1024:
                    print("Catalog input exceeds 1 MiB; connection closed", file=sys.stderr)
                    break
                try:
                    request = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    with output_lock:
                        print(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}), flush=True)
                    continue
                slots.acquire()
                pool.submit(self.handle, request).add_done_callback(complete)
        self.discovery.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("serve", "scan", "list"))
    parser.add_argument("--db", required=True)
    parser.add_argument("--home", action="append", default=[])
    parser.add_argument("--app-home", action="append", default=[], help="Explicit native adapter state home for lifecycle visibility markers")
    parser.add_argument("--workspace", action="append", default=[])
    parser.add_argument("--engine", default="amplifier")
    parser.add_argument("--scan-on-start", action="store_true")
    parser.add_argument("--scan-interval", type=float, default=300,
                        help="Background metadata reconciliation seconds; zero disables")
    parser.add_argument("--workspace-check-interval", type=float, default=15,
                        help="Seconds between bounded workspace existence checks; zero disables")
    parser.add_argument("--hint-directory", help="Optional private location-only writer hint inbox")
    parser.add_argument("--hint-interval", type=float, default=1, help="Seconds between bounded hint batches; positive when enabled")
    args = parser.parse_args()
    catalog = Catalog(args.db)
    discovery = Discovery(catalog, args.home, known_workspaces=args.workspace, engine_id=args.engine, app_homes=args.app_home)
    if args.command == "serve":
        if args.scan_interval < 0 or args.workspace_check_interval < 0:
            parser.error("maintenance intervals cannot be negative")
        if args.scan_on_start:
            discovery.start()
        if args.hint_directory and (not math.isfinite(args.hint_interval) or args.hint_interval <= 0):
            parser.error("hint interval must be positive")
        inbox = HintInbox(args.hint_directory, discovery) if args.hint_directory else None
        maintenance = Maintenance(catalog, discovery, args.scan_interval, args.workspace_check_interval, inbox, args.hint_interval)
        maintenance.start()
        try:
            Server(catalog, discovery).serve()
        finally:
            maintenance.close()
    elif args.command == "scan":
        print(json.dumps(discovery.scan()))
    else:
        print(json.dumps(catalog.list(connectionId="cli")))


class Maintenance:
    """One bounded background reconciler; interactive reads never perform I/O scans."""

    def __init__(self, catalog, discovery, scan_interval=300, workspace_interval=15, inbox=None, hint_interval=1):
        self.catalog, self.discovery = catalog, discovery
        self.inbox, self.hint_interval = inbox, hint_interval
        self.scan_interval, self.workspace_interval = scan_interval, workspace_interval
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name="catalog-maintenance", daemon=True)

    def start(self):
        self.thread.start()

    def run(self):
        import time
        now = time.monotonic()
        next_scan = now + self.scan_interval if self.scan_interval else float("inf")
        next_workspace = now + self.workspace_interval if self.workspace_interval else float("inf")
        next_hint = now + self.hint_interval if self.inbox else float("inf")
        while not self.stop.wait(min(max(0, min(next_scan, next_workspace, next_hint) - time.monotonic()), 60)):
            now = time.monotonic()
            try:
                if now >= next_hint:
                    self.inbox.drain(limit=64)
                    next_hint = now + self.hint_interval
                if now >= next_workspace:
                    self.catalog.recheck_workspaces(limit=64)
                    next_workspace = now + self.workspace_interval
                if now >= next_scan:
                    self.discovery.start()
                    next_scan = now + self.scan_interval
            except Exception as exc:
                print(f"catalog maintenance failed: {type(exc).__name__}: {exc}", file=sys.stderr)
                # Avoid retry storms while the source or database is unavailable.
                next_workspace = now + max(15, self.workspace_interval) if self.workspace_interval else float("inf")
                next_scan = now + max(15, self.scan_interval) if self.scan_interval else float("inf")
                next_hint = now + max(1, self.hint_interval) if self.inbox else float("inf")

    def close(self):
        self.stop.set()
        self.thread.join(timeout=10)
        if self.inbox and not self.thread.is_alive():
            self.inbox.close()


if __name__ == "__main__":
    main()
