# storage_io.py
# -*- coding: utf-8 -*-

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict


@dataclass(frozen=True)
class ADLSConfig:
    # Storage account name only (e.g., "adlscompasspublic")
    storage_account: str
    # Container name (e.g., "bronze")
    container: str
    # Base directory inside container (e.g., "simulation")
    base_dir: str = "simulation"

    def abfss_path(self, relative_path: str) -> str:
        """Build a full ABFSS path from a relative path."""
        rel = relative_path.lstrip("/")
        base = self.base_dir.strip("/")
        if base:
            return f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{base}/{rel}"
        return f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{rel}"


class StorageIO:
    """
    Minimal storage helper for small JSON state in ADLS Gen2 using dbutils.fs.

    Notes:
    - Assumes the cluster/workspace already has access configured (MI/OAuth/UC external location, etc.)
    - Designed for tiny state files (<= ~1MB) read via dbutils.fs.head
    """

    def __init__(self, dbutils, cfg: ADLSConfig):
        self.dbutils = dbutils
        self.cfg = cfg

    # ---------- Low-level text IO ----------

    def exists_file(self, path: str) -> bool:
        """Fast-ish existence check for a single file using head()."""
        try:
            _ = self.dbutils.fs.head(path, 1)
            return True
        except Exception:
            return False

    def read_text(self, path: str, max_bytes: int = 1024 * 1024) -> str:
        """
        Read a small text file.
        dbutils.fs.head is capped; this utility assumes state files are small.
        """
        return self.dbutils.fs.head(path, max_bytes)

    def _mkdirs_parent(self, path: str) -> None:
        """Create parent directory if possible; no-op if not needed."""
        if "/" not in path:
            return
        parent = path.rsplit("/", 1)[0]
        if not parent:
            return
        try:
            self.dbutils.fs.mkdirs(parent)
        except Exception:
            # Best-effort; some schemes or permissions might not allow mkdirs
            pass

    def write_text_atomic(self, path: str, text: str, overwrite: bool = True) -> None:
        """
        Write text in an atomic-ish manner:
          1) write to a temp file
          2) move temp into final location

        This avoids partially-written files when readers are concurrent.
        """
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        tmp_path = f"{path}.tmp.{ts}"

        self._mkdirs_parent(path)

        # Always overwrite temp
        self.dbutils.fs.put(tmp_path, text, overwrite=True)

        if overwrite and self.exists_file(path):
            # Remove the old file before move to avoid conflicts
            try:
                self.dbutils.fs.rm(path, recurse=False)
            except Exception:
                pass

        # Move temp into place
        self.dbutils.fs.mv(tmp_path, path)

    # ---------- JSON helpers ----------

    def read_json(self, path: str) -> Dict[str, Any]:
        raw = self.read_text(path)
        return json.loads(raw)

    def write_json_atomic(self, path: str, obj: Dict[str, Any], overwrite: bool = True) -> None:
        text = json.dumps(obj, ensure_ascii=False, indent=2)
        self.write_text_atomic(path, text, overwrite=overwrite)

    # ---------- Simulation state helpers ----------

    def default_simulation_state(self, start_date: str, end_date: str) -> Dict[str, Any]:
        return {
            "start_date": start_date,
            "current_date": start_date,
            "end_date": end_date,
            "day_counter": 0,
            "running": True,
        }

    def load_or_init_simulation_state(self, state_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
        if self.exists_file(state_path):
            return self.read_json(state_path)

        state = self.default_simulation_state(start_date=start_date, end_date=end_date)
        self.write_json_atomic(state_path, state, overwrite=True)
        return state

    def advance_one_day(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Advance simulation state by one day."""
        cur = date.fromisoformat(state["current_date"])
        nxt = cur + timedelta(days=1)

        new_state = dict(state)
        new_state["current_date"] = nxt.isoformat()
        new_state["day_counter"] = int(state.get("day_counter", 0)) + 1
        return new_state

