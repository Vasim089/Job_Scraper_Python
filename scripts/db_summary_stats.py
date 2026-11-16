import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor

with get_dict_cursor() as cur:
    # 1. Total jobs by ATS type
    cur.execute("""
        SELECT 
            CASE 
                WHEN job_url LIKE '%myworkdayjobs.com%' THEN 'Workday'
                WHEN job_url LIKE '%greenhouse.io%' THEN 'Greenhouse'
                WHEN job_url LIKE '%lever.co%' THEN 'Lever'
                WHEN job_url LIKE '%ashbyhq.com%' THEN 'Ashby'
                WHEN job_url LIKE '%smartrecruiters.com%' THEN 'SmartRecruiters'
                WHEN job_url LIKE '%dice.com%' THEN 'Dice'
                ELSE 'Other'
            END as ats_type,
            COUNT(id) as count
        FROM jobs
        GROUP BY ats_type
        ORDER BY count DESC;
    """)
    ats_rows = cur.fetchall()
    print("=== Jobs by ATS Platform ===")
    total = 0
    for r in ats_rows:
        print(f"  {r['ats_type']:16}: {r['count']:,}")
        total += r['count']
    print(f"  TOTAL ALL JOBS  : {total:,}\n")

    # 2. Workday jobs breakdown by Company
    cur.execute("""
        SELECT c.name as company_name, COUNT(j.id) as count
        FROM jobs j
        JOIN companies c ON c.id = j.company_id
        WHERE j.job_url LIKE '%myworkdayjobs.com%'
        GROUP BY c.name
        ORDER BY count DESC;
    """)
    wd_companies = cur.fetchall()
    print("=== Workday Jobs by Employer ===")
    wd_total = 0
    for r in wd_companies:
        print(f"  {r['company_name']:28}: {r['count']:,}")
        wd_total += r['count']
    print(f"  TOTAL WORKDAY JOBS          : {wd_total:,}\n")

    # 3. Employment Type Breakdown
    cur.execute("""
        SELECT COALESCE(employment_type, 'Not Specified') as emp_type, COUNT(id) as count
        FROM jobs
        GROUP BY emp_type
        ORDER BY count DESC;
    """)
    emp_types = cur.fetchall()
    print("=== Employment Type Breakdown ===")
    for e in emp_types:
        print(f"  {e['emp_type']:28}: {e['count']:,}")
    print()

    # 4. Contract Jobs by ATS / Source
    cur.execute("""
        SELECT 
            CASE 
                WHEN job_url LIKE '%dice.com%' THEN 'Dice'
                WHEN job_url LIKE '%greenhouse.io%' THEN 'Greenhouse'
                WHEN job_url LIKE '%myworkdayjobs.com%' THEN 'Workday'
                WHEN job_url LIKE '%smartrecruiters.com%' THEN 'SmartRecruiters'
                WHEN job_url LIKE '%lever.co%' THEN 'Lever'
                ELSE 'Other'
            END as platform,
            COUNT(id) as count
        FROM jobs
        WHERE LOWER(employment_type) LIKE '%contract%' 
           OR LOWER(employment_type) LIKE '%c2c%'
           OR LOWER(employment_type) LIKE '%corp%'
           OR LOWER(title) LIKE '%contract%'
           OR LOWER(title) LIKE '%c2c%'
        GROUP BY platform
        ORDER BY count DESC;
    """)
    contract_rows = cur.fetchall()
    print("=== Contract & C2C Jobs by Platform ===")
    contract_total = 0
    for c in contract_rows:
        print(f"  {c['platform']:28}: {c['count']:,}")
        contract_total += c['count']
    print(f"  TOTAL CONTRACT / C2C ROLES  : {contract_total:,}\n")

    # 5. Check for any closed or error status
    cur.execute("SELECT status, count(id) FROM jobs GROUP BY status")
    statuses = cur.fetchall()
    print("=== Job Status Breakdown ===")
    for s in statuses:
        print(f"  Status {s['status']}: {s['count']:,}")
