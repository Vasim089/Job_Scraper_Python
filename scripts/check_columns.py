import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor

with get_dict_cursor() as cur:
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'jobs'")
    for row in cur.fetchall():
        print(f"{row['column_name']}: {row['data_type']}")
