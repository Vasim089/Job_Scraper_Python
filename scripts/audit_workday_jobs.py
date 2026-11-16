import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor
from scraper_agent.verification.checker import JobAvailabilityChecker

def inspect_workday_jobs():
    checker = JobAvailabilityChecker()
    with get_dict_cursor() as cur:
        cur.execute("SELECT id, title, job_url, description FROM jobs WHERE job_url LIKE '%myworkdayjobs.com%'")
        rows = cur.fetchall()
        print(f"Total Workday jobs found in DB: {len(rows)}")
        
        # Test the first 10
        for r in rows[:10]:
            url = r['job_url']
            cxs_res = checker.verify_workday_cxs(url)
            print(f"ID {r['id']} | Title: {r['title'][:40]} | URL: {url} -> CXS: {cxs_res}")

if __name__ == "__main__":
    inspect_workday_jobs()
