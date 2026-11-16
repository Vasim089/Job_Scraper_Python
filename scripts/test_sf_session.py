import requests

session = requests.Session()
# First, fetch the HTML page like a real browser to see cookies / redirects
url = "https://salesforce.wd12.myworkdayjobs.com/External_Career_Site/job/California---San-Francisco/Software-Engineering-LMTS_JR367337"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

print("1. Fetching page HTML...")
r = session.get(url, headers=headers, timeout=10)
print("Page status:", r.status_code, "Final URL:", r.url)
print("Cookies:", session.cookies.get_dict())

# Now try CXS with the session cookies:
cxs_candidates = [
    "https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/job/California---San-Francisco/Software-Engineering-LMTS_JR367337",
    "https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/jobs",
]

for c_url in cxs_candidates:
    c_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": r.url,
    }
    c_resp = session.get(c_url, headers=c_headers, timeout=10)
    print(f"CXS URL: {c_url}")
    print(f"Status: {c_resp.status_code}")
    print(f"Body: {c_resp.text[:300]}\n")
