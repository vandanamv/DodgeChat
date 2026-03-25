from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE_DIR / "sap-order-to-cash-dataset" / "sap-o2c-data"
FRONTEND_DIR = BASE_DIR / "Frontend"
CACHE_DIR = BASE_DIR / ".cache"
SQLITE_CACHE_PATH = CACHE_DIR / "o2c_cache.sqlite3"
GRAPH_CACHE_PATH = CACHE_DIR / "graph_cache.json"
