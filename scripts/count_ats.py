import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor

def count_job_domains():
    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT 
                CASE 
                    WHEN job_url LIKE '%myworkdayjobs.com%' THEN 'Workday'
                    WHEN job_url LIKE '%greenhouse.io%' THEN 'Greenhouse'
                    WHEN job_url LIKE '%lever.co%' THEN 'Lever'
                    WHEN job_url LIKE '%ashbyhq.com%' THEN 'Ashby'
                    WHEN job_url LIKE '%smartrecruiters.com%' THEN 'SmartRecruiters'
                    WHEN job_url LIKE '%icims.com%' THEN 'iCIMS'
                    WHEN job_url LIKE '%dice.com%' THEN 'Dice'
                    ELSE 'Other'
                END as ats_type,
                COUNT(*) as count
            FROM jobs
            GROUP BY ats_type
            ORDER BY count DESC;
        """)
        rows = cur.fetchall()
        for r in rows:
            print(f"{r['ats_type']}: {r['count']}")

if __name__ == "__main__":
    count_job_domains()
