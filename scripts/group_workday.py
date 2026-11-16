import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor
from urllib.parse import urlparse

with get_dict_cursor() as cur:
    cur.execute("""
        SELECT j.id, c.name as company_name, j.job_url 
        FROM jobs j 
        LEFT JOIN companies c ON c.id = j.company_id
        WHERE j.job_url LIKE '%myworkdayjobs.com%'
    """)
    rows = cur.fetchall()
    print(f"Total Workday jobs: {len(rows)}")
    hosts = {}
    for r in rows:
        host = urlparse(r['job_url']).netloc
        comp = r['company_name'] or 'Unknown'
        key = f"{comp} ({host})"
        hosts[key] = hosts.get(key, 0) + 1

    for k, v in sorted(hosts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
