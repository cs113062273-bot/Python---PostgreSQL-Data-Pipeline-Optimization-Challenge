"""Load the preserved application without installing its generic module names."""
from pathlib import Path
import sys

APP = Path(__file__).resolve().parents[1] / "app"
if str(APP) not in sys.path:
    sys.path.append(str(APP))
from services.postgres_service import PostgresAdminService  # noqa: E402
