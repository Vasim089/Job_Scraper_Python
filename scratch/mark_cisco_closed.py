import sys
sys.path.insert(0, ".")
import psycopg
from scraper_agent.db.connection import DB_CONFIG

try:
    with psycopg.connect(DB_CONFIG.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, title, status, job_url FROM jobs WHERE id = 8461;")
            row = cur.fetchone()
            print("JOB 8461 STATUS:", row)

            # Update status to CLOSED since verified expired on Cisco Workday
            cur.execute("UPDATE jobs SET status = 'CLOSED', updated_at = CURRENT_TIMESTAMP WHERE id = 8461;")
            conn.commit()
            print("Successfully updated Job 8461 status to CLOSED in database.")
except Exception as e:
    print(f"ERROR: {e}")

