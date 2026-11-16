import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor

with get_dict_cursor() as cur:
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = [r['table_name'] for r in cur.fetchall()]
    print("Tables in public schema:", tables)

    print("\n--- Searching for 'exist' across jobs ---")
    cur.execute("SELECT id, title, job_url, description FROM jobs WHERE title ILIKE '%exist%' OR description ILIKE '%exist%'")
    rows = cur.fetchall()
    print(f"Jobs with 'exist' in title or description: {len(rows)}")
    for r in rows[:10]:
        print(f"ID {r['id']} | Title: {r['title']} | URL: {r['job_url']}")
        desc_snippet = (r['description'] or '')[:100].replace('\n', ' ')
        print(f"   Desc: {desc_snippet}")

    print("\n--- Searching for '404' or 'not found' in title or description ---")
    cur.execute("SELECT id, title, job_url FROM jobs WHERE title ILIKE '%not found%' OR title ILIKE '%404%'")
    rows_nf = cur.fetchall()
    print(f"Jobs with 'not found' or '404' in title: {len(rows_nf)}")
    for r in rows_nf:
        print(f"ID {r['id']} | Title: {r['title']} | URL: {r['job_url']}")

    print("\n--- Searching for 'workday' in jobs table ---")
    cur.execute("SELECT id, title, job_url FROM jobs WHERE title ILIKE '%workday support%' OR description ILIKE '%workday support%'")
    rows_wd = cur.fetchall()
    print(f"Jobs with 'workday support': {len(rows_wd)}")
    for r in rows_wd:
        print(f"ID {r['id']} | Title: {r['title']} | URL: {r['job_url']}")
