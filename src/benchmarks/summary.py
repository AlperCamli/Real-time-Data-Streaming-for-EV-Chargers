"""Benchmark result persistence and summary materialization."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def persist_benchmark_result(*, result: dict[str, Any], base_dir: str | Path = "benchmark_results") -> dict[str, str]:
    base_path = Path(base_dir)
    runs_path = base_path / "runs"
    latest_path = base_path / "latest"
    runs_path.mkdir(parents=True, exist_ok=True)
    latest_path.mkdir(parents=True, exist_ok=True)

    run_id = _build_run_id(str(result.get("profile_name", "unknown")))
    run_dir = runs_path / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    result_json_path = run_dir / "result.json"
    result_json_path.write_text(json.dumps(result, indent=2, ensure_ascii=True), encoding="utf-8")

    _write_single_result_csv(run_dir / "result.csv", result)
    _write_markdown_summary(run_dir / "summary.md", result)

    (latest_path / "result.json").write_text(result_json_path.read_text(encoding="utf-8"), encoding="utf-8")
    (latest_path / "result.csv").write_text((run_dir / "result.csv").read_text(encoding="utf-8"), encoding="utf-8")
    (latest_path / "summary.md").write_text((run_dir / "summary.md").read_text(encoding="utf-8"), encoding="utf-8")

    all_results = _load_all_results(runs_path)
    _write_summary_json(base_path / "summary.json", all_results)
    _write_summary_csv(base_path / "summary.csv", all_results)
    _write_summary_md(base_path / "summary.md", all_results)

    return {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "latest_dir": str(latest_path),
        "summary_json": str(base_path / "summary.json"),
        "summary_csv": str(base_path / "summary.csv"),
        "summary_md": str(base_path / "summary.md"),
    }


def _build_run_id(profile_name: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_profile = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in profile_name).strip("-")
    return f"{timestamp}_{safe_profile or 'profile'}"


def _write_single_result_csv(path: Path, result: dict[str, Any]) -> None:
    row = _flatten_result(result)
    fieldnames = sorted(row.keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)


def _write_markdown_summary(path: Path, result: dict[str, Any]) -> None:
    lines = [
        "| field | value |",
        "|---|---|",
    ]
    for key, value in sorted(_flatten_result(result).items()):
        lines.append(f"| {key} | {value} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _flatten_result(result: dict[str, Any]) -> dict[str, str]:
    flattened: dict[str, str] = {}
    for key, value in result.items():
        if isinstance(value, (dict, list)):
            flattened[key] = json.dumps(value, ensure_ascii=True, sort_keys=True)
        else:
            flattened[key] = str(value)
    return flattened


def _load_all_results(runs_path: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for result_path in sorted(runs_path.glob("*/result.json")):
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if isinstance(payload, dict):
            results.append(payload)
    return results


def _write_summary_json(path: Path, results: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(results, indent=2, ensure_ascii=True), encoding="utf-8")


def _write_summary_csv(path: Path, results: list[dict[str, Any]]) -> None:
    rows = [_flatten_result(item) for item in results]
    all_fields: set[str] = set()
    for row in rows:
        all_fields.update(row.keys())
    fieldnames = sorted(all_fields)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_summary_md(path: Path, results: list[dict[str, Any]]) -> None:
    if not results:
        path.write_text("No benchmark results yet.\n", encoding="utf-8")
        return

    columns = [
        "profile_name",
        "test_type",
        "target_eps",
        "achieved_eps",
        "p95_ingest_lag_ms",
        "p99_ingest_lag_ms",
        "p95_end_to_end_latency_ms",
        "p99_end_to_end_latency_ms",
        "peak_consumer_lag",
        "outcome",
        "primary_bottleneck",
    ]

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for result in results:
        row = []
        for column in columns:
            value = result.get(column, "")
            row.append(str(value))
        lines.append("| " + " | ".join(row) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
