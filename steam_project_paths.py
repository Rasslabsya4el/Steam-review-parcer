from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DATASETS_DIRNAME = (os.getenv("STEAM_DATASETS_DIR", "datasets") or "datasets").strip() or "datasets"
DATASETS_ROOT = (PROJECT_ROOT / DATASETS_DIRNAME).resolve()


def ensure_datasets_root() -> Path:
    DATASETS_ROOT.mkdir(parents=True, exist_ok=True)
    return DATASETS_ROOT


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _normalize_dataset_arg(dataset_dir: str | Path) -> tuple[str, str]:
    raw = str(dataset_dir).strip()
    if not raw:
        raise ValueError("dataset_dir must not be empty")

    normalized = raw.replace("\\", "/")
    parts = [p for p in normalized.split("/") if p]
    if ".." in parts:
        raise ValueError("dataset_dir must not contain '..'")
    return raw, normalized


def resolve_dataset_path(dataset_dir: str | Path, *, prefer_existing: bool = True) -> Path:
    """
    Resolve dataset location inside the project.

    Behavior:
    - bare name (e.g. "steam_dataset") -> <PROJECT_ROOT>/datasets/steam_dataset
    - explicit "datasets/<name>" -> respected
    - legacy path at project root (e.g. "<PROJECT_ROOT>/steam_dataset") still works if it already exists
    """
    ensure_datasets_root()
    raw, normalized = _normalize_dataset_arg(dataset_dir)

    p = Path(raw)
    if p.is_absolute():
        candidate = p.resolve()
        if not _is_within(candidate, PROJECT_ROOT):
            raise ValueError("dataset_dir escapes project root")
        return candidate

    candidates: list[Path] = []
    if normalized == DATASETS_DIRNAME or normalized.startswith(DATASETS_DIRNAME + "/"):
        candidates.append((PROJECT_ROOT / Path(normalized)).resolve())
    else:
        candidates.append((DATASETS_ROOT / Path(raw)).resolve())
        # Legacy compatibility: allow existing datasets stored in project root.
        candidates.append((PROJECT_ROOT / Path(raw)).resolve())

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if not _is_within(candidate, PROJECT_ROOT):
            raise ValueError("dataset_dir escapes project root")
        deduped.append(candidate)

    if prefer_existing:
        for candidate in deduped:
            if candidate.exists():
                return candidate

    return deduped[0]


def dataset_display_id(path: Path) -> str:
    """Return a stable dataset id for UI/API (prefer path relative to datasets root)."""
    resolved = path.resolve()
    if _is_within(resolved, DATASETS_ROOT):
        return str(resolved.relative_to(DATASETS_ROOT)).replace("\\", "/")
    if _is_within(resolved, PROJECT_ROOT):
        return str(resolved.relative_to(PROJECT_ROOT)).replace("\\", "/")
    return str(resolved)

