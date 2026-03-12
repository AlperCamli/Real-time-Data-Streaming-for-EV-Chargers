"""Environment metadata capture for benchmark result records."""

from __future__ import annotations

import os
import platform
import socket
import subprocess
from datetime import datetime, timezone


def capture_environment_metadata() -> dict[str, object]:
    metadata: dict[str, object] = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }

    ram_bytes = _capture_memory_bytes()
    if ram_bytes is not None:
        metadata["memory_total_bytes"] = ram_bytes

    git_commit = _capture_git_commit()
    if git_commit:
        metadata["git_commit"] = git_commit

    return metadata


def _capture_memory_bytes() -> int | None:
    try:
        import psutil  # type: ignore
    except ModuleNotFoundError:
        return None

    try:
        return int(psutil.virtual_memory().total)
    except Exception:  # noqa: BLE001
        return None


def _capture_git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:  # noqa: BLE001
        return None
    commit = result.stdout.strip()
    return commit or None
