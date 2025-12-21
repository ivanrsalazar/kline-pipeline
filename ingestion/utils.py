# ingestion/utils.py

from datetime import datetime, timezone
import os
import time

def cleanup_local_files(
    base_dir: str,
    active_file: str,
    max_age_seconds: int = 6 * 3600
):
    now = time.time()

    for root, _, files in os.walk(base_dir):
        for f in files:
            path = os.path.join(root, f)

            # Never delete active file
            if path == active_file:
                continue

            try:
                age = now - os.path.getmtime(path)
            except FileNotFoundError:
                continue

            if age > max_age_seconds:
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"[CLEANUP] Failed to delete {path}: {e}")


def ts_ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()

def ts_ns_to_iso(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).isoformat()

def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
