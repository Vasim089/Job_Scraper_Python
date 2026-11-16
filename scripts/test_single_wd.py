import requests

url = "https://salesforce.wd12.myworkdayjobs.com/External_Career_Site/job/California---San-Francisco/Software-Engineering-LMTS_JR367337"
# api_url for ID 6136:
# host = salesforce.wd12.myworkdayjobs.com
# tenant = salesforce
# board = External_Career_Site
# clean_path = /job/California---San-Francisco/Software-Engineering-LMTS_JR367337
api_url = "https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/job/California---San-Francisco/Software-Engineering-LMTS_JR367337"

headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

print("Fetching api_url:", api_url)
try:
    resp = requests.get(api_url, headers=headers, timeout=10)
    print("Status:", resp.status_code)
    print("Headers:", resp.headers.get("content-type"))
    print("Body preview:", resp.text[:300])
except Exception as e:
    print("Error:", type(e), e)
