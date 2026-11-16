import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor
from scraper_agent.verification.checker import JobAvailabilityChecker

def inspect_failed():
    checker = JobAvailabilityChecker(request_timeout=5)
    with get_dict_cursor() as cur:
        cur.execute("SELECT id, title, job_url FROM jobs WHERE job_url LIKE '%myworkdayjobs.com%'")
        jobs = cur.fetchall()

    failed = []
    for j in jobs:
        res = checker.verify_workday_cxs(j["job_url"])
        if res is None:
            failed.append(j)

    print(f"Total failed / returned None: {len(failed)}")
    for f in failed[:20]:
        print(f"ID {f['id']} | Title: {f['title'][:40]} | URL: {f['job_url']}")

if __name__ == "__main__":
    inspect_failed()
