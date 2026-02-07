import os
import shutil
import sys
from pathlib import Path


def get_resource_root() -> Path:
    if hasattr(sys, "_MEIPASS"):
        return Path(getattr(sys, "_MEIPASS")).resolve()
    return Path(__file__).resolve().parent.parent


def get_data_root() -> Path:
    override = os.environ.get("SLG_MONITOR_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata).expanduser().resolve() / "SLGMonitor"
    return Path.home().resolve() / "AppData" / "Roaming" / "SLGMonitor"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_copytree(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    try:
        shutil.copytree(src, dst)
    except Exception:
        return


def _safe_copyfile(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    except Exception:
        return


def ensure_seed_data(resource_root: Path, data_root: Path) -> None:
    """Seed minimal data into AppData for first run."""
    ensure_dir(data_root)

    seed_dirs = [
        "mapping",
        "labels",
        "newproducts",
        os.path.join("frontend", "data"),
        "deploy",
    ]
    for rel in seed_dirs:
        src = resource_root / rel
        dst = data_root / rel
        if src.is_dir():
            _safe_copytree(src, dst)

    auth_src = resource_root / "deploy" / "auth_users.json"
    auth_dst = data_root / "deploy" / "auth_users.json"
    if auth_src.is_file():
        _safe_copyfile(auth_src, auth_dst)

    for rel in [
        "raw_csv",
        "intermediate",
        "output",
        "target",
        "final_join",
        "advertisements",
        os.path.join("request", "country_data"),
        "request",
    ]:
        ensure_dir(data_root / rel)
