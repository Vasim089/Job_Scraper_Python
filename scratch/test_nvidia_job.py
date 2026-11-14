import sys
sys.path.insert(0, ".")
import psycopg
from scraper_agent.db.connection import DB_CONFIG
from scraper_agent.verification.checker import JobAvailabilityChecker
from scraper_agent.verification.models import VerificationAction

url = "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/job/US-CA-Santa-Clara/Senior-Software-Engineer--NeMo-Core-Platform_JR2024160"

checker = JobAvailabilityChecker()
res = checker.verify_job(url, job_id=6015)
print("VERIFICATION RESULT:")
print("Action:", res.action)
print("Reason:", res.reason)
print("Target DB Status:", res.target_db_status)
print("Notes:", res.details.notes)

if res.action == VerificationAction.MARK_CLOSED:
    with psycopg.connect(DB_CONFIG.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET status = 'CLOSED', updated_at = CURRENT_TIMESTAMP WHERE id = 6015;")
            conn.commit()
            print("Successfully updated Job 6015 (NVIDIA NeMo) to CLOSED in PostgreSQL!")
