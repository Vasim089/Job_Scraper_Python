import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor

def check_all_jobs_for_error_phrases():
    phrases = [
        "%the page you are looking for doesn't exist%",
        "%the page you are looking for does not exist%",
        "%page you are looking for doesn't exist%",
        "%page you are looking for does not exist%",
        "%job you are looking for doesn't exist%",
        "%job doesn't exist%",
        "%job does not exist%",
        "%page not found%",
        "%can't find that page%",
        "%can't find the page%",
        "%contact workday support%",
        "%workday support%",
        "%job is no longer available%",
        "%no longer accepting applications%",
        "%posting has expired%",
        "%this job has expired%",
        "%position has been filled%",
    ]
    with get_dict_cursor() as cur:
        for phrase in phrases:
            cur.execute("SELECT id, title, job_url, description FROM jobs WHERE description ILIKE %s OR title ILIKE %s", (phrase, phrase))
            matches = cur.fetchall()
            if matches:
                print(f"Phrase '{phrase}': {len(matches)} matches!")
                for m in matches[:5]:
                    print(f"   ID {m['id']} | Title: {m['title']} | URL: {m['job_url']}")
                    print(f"   Desc snippet: {m['description'][:150]}...")
            else:
                print(f"Phrase '{phrase}': 0 matches.")

if __name__ == "__main__":
    check_all_jobs_for_error_phrases()
