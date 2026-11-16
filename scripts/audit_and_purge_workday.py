import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor, get_db_connection
from scraper_agent.verification.checker import JobAvailabilityChecker

def verify_and_purge_workday_jobs(dry_run=True):
    checker = JobAvailabilityChecker(request_timeout=6)
    with get_dict_cursor() as cur:
        cur.execute("SELECT id, title, job_url FROM jobs WHERE job_url LIKE '%myworkdayjobs.com%' ORDER BY id ASC")
        jobs = cur.fetchall()

    print(f"Total Workday jobs to audit: {len(jobs)}")
    dead_jobs = []
    active_jobs = []
    failed_checks = []

    def check_job(job):
        jid, title, url = job["id"], job["title"], job["job_url"]
        res = checker.verify_workday_cxs(url)
        return jid, title, url, res

    start = time.time()
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(check_job, j): j for j in jobs}
        completed = 0
        for f in as_completed(futures):
            jid, title, url, res = f.result()
            completed += 1
            if res is None:
                failed_checks.append((jid, title, url, "Unknown URL format / parse error"))
            else:
                is_active, note = res
                if not is_active:
                    dead_jobs.append((jid, title, url, note))
                else:
                    active_jobs.append((jid, title, url, note))

            if completed % 100 == 0 or completed == len(jobs):
                print(f"Progress: {completed}/{len(jobs)} | Active: {len(active_jobs)} | Dead/Expired: {len(dead_jobs)} | Failed: {len(failed_checks)}")

    elapsed = round(time.time() - start, 2)
    print(f"\nAudit completed in {elapsed}s.")
    print(f"Summary: Active={len(active_jobs)}, Dead/Expired={len(dead_jobs)}, Failed/Format={len(failed_checks)}")

    if dead_jobs:
        print("\nSample of dead/expired Workday jobs (showing 'The page you are looking for doesn't exist'):")
        for jid, title, url, note in dead_jobs[:10]:
            print(f"  [DEAD] ID {jid} | {title} | {url}\n         Reason: {note}")

    if not dry_run and dead_jobs:
        dead_ids = [d[0] for d in dead_jobs]
        print(f"\nDeleting {len(dead_ids)} dead Workday jobs from PostgreSQL...")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM jobs WHERE id = ANY(%s)", (dead_ids,))
                deleted_count = cur.rowcount
        print(f"Successfully deleted {deleted_count} dead Workday jobs from database!")
    elif dry_run:
        print("\nDRY RUN mode: No jobs were deleted.")

    return dead_jobs, active_jobs, failed_checks

if __name__ == "__main__":
    dry_run = "--delete" not in sys.argv
    verify_and_purge_workday_jobs(dry_run=dry_run)
