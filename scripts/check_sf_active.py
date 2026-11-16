import requests

session = requests.Session()
url = "https://salesforce.wd12.myworkdayjobs.com/External_Career_Site"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

r = session.get(url, headers=headers, timeout=10)

api_search = "https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/jobs"
c_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Referer": url,
}

payload = {"appliedFacets": {}, "limit": 5, "offset": 0, "searchText": ""}
res = session.post(api_search, json=payload, headers=c_headers, timeout=10)
print("Search status:", res.status_code)
if res.status_code == 200:
    data = res.json()
    print("Total active postings found:", data.get("total"))
    postings = data.get("jobPostings", [])
    print("First posting:", postings[0] if postings else "None")
    
    if postings:
        ext_path = postings[0]["externalPath"]
        detail_url = f"https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site{ext_path}"
        d_resp = session.get(detail_url, headers=c_headers, timeout=10)
        print("Active job detail status:", d_resp.status_code)
        print("Active job title:", d_resp.json().get("jobPostingInfo", {}).get("title"))
