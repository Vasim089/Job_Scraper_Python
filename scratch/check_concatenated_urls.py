import sys
sys.path.insert(0, ".")
import psycopg
from scraper_agent.db.connection import DB_CONFIG

with psycopg.connect(DB_CONFIG.connection_string) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, title, job_url 
            FROM jobs 
            WHERE job_url ILIKE '%R-00195460%' 
               OR job_url ILIKE '%2024886%'
               OR external_job_id ILIKE '%R-00195460%'
               OR external_job_id ILIKE '%2024886%';
        """)
        rows = cur.fetchall()
        for r in rows:
            print(f"ID: {r[0]} | Title: {r[1]} | URL: {r[2]}")
