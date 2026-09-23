"""Output paths for hub data.

No DVC / versioning here on purpose (see README.md): every dataset lives at
one fixed, current-state path that downstream consumers (book repos,
`dvc import`-free) read directly. Dagster's own materialization history
(timestamps, row counts as metadata) is the audit trail, not the file
system.
"""

from pathlib import Path

HUB_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = HUB_ROOT / "data"
SMARD_DIR = DATA_ROOT / "smard"


def smard_file(name: str) -> Path:
    """Path for one SMARD series, e.g. `smard_file("load")` ->
    `data/smard/load.parquet`."""
    SMARD_DIR.mkdir(parents=True, exist_ok=True)
    return SMARD_DIR / f"{name}.parquet"
