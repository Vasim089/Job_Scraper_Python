import sys
sys.path.insert(0, ".")
import psycopg
from scraper_agent.db.connection import DB_CONFIG

# Extended list of international cities and keywords to test against
FOREIGN_KEYWORDS = [
    # India cities / states
    "india", "bengaluru", "bangalore", "hyderabad", "pune", "mumbai", "chennai", "delhi", "new delhi",
    "noida", "gurgaon", "gurugram", "kolkata", "ahmedabad", "visakhapatnam", "vizag", "vijayawada",
    "kochi", "coimbatore", "trivandrum", "chandigarh", "jaipur", "indore", "karnataka", "telangana",
    "maharashtra", "tamil nadu", "andhra", "kerala", "gujarat",
    # UK / Europe
    "united kingdom", "london", "manchester", "birmingham", "edinburgh", "glasgow", "bristol", "leeds",
    "germany", "berlin", "munich", "frankfurt", "hamburg", "cologne", "stuttgart",
    "france", "paris", "lyon", "toulouse",
    "netherlands", "amsterdam", "rotterdam", "utrecht",
    "ireland", "dublin", "cork", "galway",
    "poland", "warsaw", "krakow", "wroclaw",
    "spain", "madrid", "barcelona",
    "switzerland", "zurich", "geneva",
    "sweden", "stockholm",
    # Canada
    "canada", "toronto", "vancouver", "montreal", "ottawa", "calgary", "waterloo", "ontario", "quebec", "british columbia",
    # Asia / Australia / LATAM
    "australia", "sydney", "melbourne", "brisbane",
    "singapore", "tokyo", "japan",
    "israel", "tel aviv",
    "brazil", "sao paulo",
    "mexico", "guadalajara", "mexico city",
    "philippines", "manila",
    "emea", "apac", "latam"
]

with psycopg.connect(DB_CONFIG.connection_string) as conn:
    with conn.cursor() as cur:
        # 1. Total jobs in DB
        cur.execute("SELECT COUNT(*) FROM jobs;")
        total_jobs = cur.fetchone()[0]

        # 2. Check jobs where country is explicitly NOT 'United States'
        cur.execute("""
            SELECT id, title, location, city, state, country, job_url
            FROM jobs
            WHERE country IS NOT NULL AND country != 'United States';
        """)
        non_us_country_jobs = cur.fetchall()

        # 3. Check jobs where location or city mentions foreign keywords
        # Build dynamic SQL pattern
        foreign_clauses = " OR ".join([f"location ILIKE '%{kw}%' OR city ILIKE '%{kw}%' OR title ILIKE '%{kw}%'" for kw in FOREIGN_KEYWORDS])
        cur.execute(f"""
            SELECT id, title, location, city, state, country, job_url
            FROM jobs
            WHERE ({foreign_clauses});
        """)
        keyword_matched_jobs = cur.fetchall()

        # 4. Check Canada misclassified as California (', ca' where location also has Canada indicators or Canadian cities)
        cur.execute("""
            SELECT id, title, location, city, state, country, job_url
            FROM jobs
            WHERE state = 'CA'
              AND (location ILIKE '%toronto%' OR location ILIKE '%vancouver%' OR location ILIKE '%ontario%' OR location ILIKE '%montreal%' OR location ILIKE '%, ca, canada%');
        """)
        canada_misclassified = cur.fetchall()

        # 5. Check Germany misclassified as Delaware (state='DE')
        cur.execute("""
            SELECT id, title, location, city, state, country, job_url
            FROM jobs
            WHERE state = 'DE'
              AND (location ILIKE '%berlin%' OR location ILIKE '%munich%' OR location ILIKE '%frankfurt%' OR location ILIKE '%germany%');
        """)
        germany_misclassified = cur.fetchall()

        # Consolidate distinct suspect IDs
        all_suspects = {}
        for r in non_us_country_jobs:
            all_suspects[r[0]] = ("Non-US Country in DB", r)
        for r in keyword_matched_jobs:
            if r[0] not in all_suspects:
                all_suspects[r[0]] = ("Foreign Keyword Match", r)
        for r in canada_misclassified:
            if r[0] not in all_suspects:
                all_suspects[r[0]] = ("Canada misclassified as CA", r)
        for r in germany_misclassified:
            if r[0] not in all_suspects:
                all_suspects[r[0]] = ("Germany misclassified as DE", r)

        print(f"TOTAL_JOBS_IN_DB: {total_jobs}")
        print(f"TOTAL_SUSPECT_NON_USA_JOBS: {len(all_suspects)}")
        print("\n--- DETAILED BREAKDOWN ---")
        for jid, (reason, r) in list(all_suspects.items()):
            print(f"[{reason}] ID: {r[0]} | Title: {r[1]} | Loc: {r[2]} | City: {r[3]} | State: {r[4]} | Country: {r[5]} | URL: {r[6]}")
