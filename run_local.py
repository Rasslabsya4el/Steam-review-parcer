from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from steam_project_paths import PROJECT_ROOT, ensure_datasets_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start backend API (FastAPI/Uvicorn) and Flask UI for local use."
    )
    parser.add_argument("--api-host", default="127.0.0.1", help="API host (default: 127.0.0.1)")
    parser.add_argument("--api-port", type=int, default=8000, help="API port (default: 8000)")
    parser.add_argument("--ui-host", default="127.0.0.1", help="UI host (default: 127.0.0.1)")
    parser.add_argument("--ui-port", type=int, default=5050, help="UI port (default: 5050)")
    parser.add_argument("--ui-debug", action="store_true", help="Enable Flask debug/reloader")
    return parser.parse_args()


def _stream_output(prefix: str, pipe) -> None:
    try:
        for line in iter(pipe.readline, ""):
            if not line:
                break
            sys.stdout.write(f"[{prefix}] {line}")
            sys.stdout.flush()
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def _terminate_process(proc: subprocess.Popen[str], name: str) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        return

    try:
        proc.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        proc.kill()
    except Exception:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        print(f"Failed to stop {name} cleanly", file=sys.stderr)


def start_processes(args: argparse.Namespace) -> tuple[subprocess.Popen[str], subprocess.Popen[str]]:
    ensure_datasets_root()

    env_api = os.environ.copy()
    env_ui = os.environ.copy()
    env_api["PYTHONUNBUFFERED"] = "1"
    env_ui["PYTHONUNBUFFERED"] = "1"
    env_ui["STEAM_API_BASE_URL"] = f"http://{args.api_host}:{args.api_port}"
    env_ui["STEAM_UI_HOST"] = args.ui_host
    env_ui["STEAM_UI_PORT"] = str(args.ui_port)
    env_ui["STEAM_UI_DEBUG"] = "1" if args.ui_debug else "0"

    api_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "steam_backend_api:app",
        "--host",
        args.api_host,
        "--port",
        str(args.api_port),
    ]
    ui_cmd = [sys.executable, "steam_frontend_flask.py"]

    api_proc = subprocess.Popen(
        api_cmd,
        cwd=str(PROJECT_ROOT),
        env=env_api,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    ui_proc = subprocess.Popen(
        ui_cmd,
        cwd=str(PROJECT_ROOT),
        env=env_ui,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    return api_proc, ui_proc


def main() -> int:
    args = parse_args()
    api_proc, ui_proc = start_processes(args)

    threads = [
        threading.Thread(target=_stream_output, args=("api", api_proc.stdout), daemon=True),
        threading.Thread(target=_stream_output, args=("ui", ui_proc.stdout), daemon=True),
    ]
    for t in threads:
        t.start()

    print(f"Project root : {PROJECT_ROOT}")
    print(f"Datasets dir  : {ensure_datasets_root()}")
    print(f"API           : http://{args.api_host}:{args.api_port}")
    print(f"UI            : http://{args.ui_host}:{args.ui_port}")
    print("Press Ctrl+C to stop both processes.")

    try:
        while True:
            api_rc = api_proc.poll()
            ui_rc = ui_proc.poll()
            if api_rc is not None or ui_rc is not None:
                if api_rc is not None:
                    print(f"API process exited with code {api_rc}")
                if ui_rc is not None:
                    print(f"UI process exited with code {ui_rc}")
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        _terminate_process(ui_proc, "UI")
        _terminate_process(api_proc, "API")
        time.sleep(0.2)

    # Prefer non-zero exit if any child failed unexpectedly.
    api_rc = api_proc.poll()
    ui_rc = ui_proc.poll()
    if api_rc not in (None, 0) or ui_rc not in (None, 0):
        return 1
    return 0


if __name__ == "__main__":
    if os.name == "nt":
        # Avoid Ctrl+C being swallowed by some nested subprocess combinations on Windows.
        signal.signal(signal.SIGINT, signal.default_int_handler)
    raise SystemExit(main())

