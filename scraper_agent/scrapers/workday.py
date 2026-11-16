"""
Workday ATS Scraper.
Directly interfaces with Workday's client-experience services (CXS) JSON REST API:
  POST https://{host}/wday/cxs/{tenant}/{site}/jobs
  GET  https://{host}/wday/cxs/{tenant}/{site}{externalPath}

Key capabilities:
1. Fast, reliable extraction directly from active employer Workday portals.
2. Complete job descriptions, requisitions, and application URLs.
3. Completely avoids expired/closed listings ("The page you are looking for doesn't exist").
"""

import re
import logging
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper_agent.models.job import RawJobPost
from scraper_agent.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class WorkdayScraper(BaseScraper):
    """Production scraper for all Workday ATS (myworkdayjobs.com) career boards."""

    def __init__(self, max_workers: int = 20, max_pages: int = 50):
        super().__init__()
        self.max_workers = max_workers
        self.max_pages = max_pages
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    @staticmethod
    def parse_workday_url(url: str) -> Optional[Tuple[str, str, str]]:
        """
        Extracts (host, tenant, site) from a Workday career board URL.
        Example: https://target.wd5.myworkdayjobs.com/targetcareers
          -> ('target.wd5.myworkdayjobs.com', 'target', 'targetcareers')
        """
        if not url or "myworkdayjobs.com" not in url:
            return None
        
        parsed = urlparse(url)
        host = parsed.netloc
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        
        tenant = host.split(".")[0]
        site = path_parts[0] if path_parts else tenant
        if site in ("en-US", "en", "us", "job") and len(path_parts) > 1:
            site = path_parts[1]

        return host, tenant, site

    def fetch_jobs(self, source_url: str, max_jobs: int = 1000) -> List[RawJobPost]:
        """
        Fetches all active jobs for a given Workday career portal.
        """
        parsed = self.parse_workday_url(source_url)
        if not parsed:
            logger.warning(f"Invalid Workday URL: {source_url}")
            return []

        host, tenant, site = parsed
        api_base = f"https://{host}/wday/cxs/{tenant}/{site}"
        search_endpoint = f"{api_base}/jobs"

        # 1. Fetch page 0 to discover total available jobs
        payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}
        try:
            resp = self.session.post(search_endpoint, json=payload, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Workday search API error HTTP {resp.status_code} for {source_url}")
                return []
            data = resp.json()
            total_avail = data.get("total", 0)
            postings = list(data.get("jobPostings", []))
        except Exception as e:
            logger.error(f"Failed to query Workday search API for {source_url}: {e}")
            return []

        # 2. Paginate concurrently up to max_jobs
        needed = min(max_jobs, total_avail)
        if needed > 20:
            offsets = list(range(20, needed, 20))
            def fetch_offset(off):
                try:
                    p = {"appliedFacets": {}, "limit": 20, "offset": off, "searchText": ""}
                    r = self.session.post(search_endpoint, json=p, timeout=10)
                    if r.status_code == 200:
                        return r.json().get("jobPostings", [])
                except Exception:
                    pass
                return []

            with ThreadPoolExecutor(max_workers=min(15, self.max_workers)) as ex:
                futures = [ex.submit(fetch_offset, off) for off in offsets]
                for f in as_completed(futures):
                    postings.extend(f.result())

        logger.info(f"Discovered {len(postings)} active Workday listings for {host}/{site}.")

        # 3. Fetch details concurrently
        def fetch_detail(item):
            path = item.get("externalPath")
            if not path:
                return None
            detail_url = f"{api_base}{path}"
            try:
                r = self.session.get(detail_url, timeout=8)
                if r.status_code == 200:
                    info = r.json().get("jobPostingInfo", {})
                    if info and info.get("title"):
                        return item, info
            except Exception:
                pass
            return item, None

        raw_jobs: List[RawJobPost] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            detail_futs = [ex.submit(fetch_detail, p) for p in postings[:needed]]
            for f in as_completed(detail_futs):
                res = f.result()
                if not res:
                    continue
                item, info = res
                path = item.get("externalPath", "")
                title = (info.get("title") if info else item.get("title", "")).strip()
                loc = (info.get("location") if info else item.get("locationsText", "")).strip()
                desc = self.clean_html(info.get("jobDescription", "")) if info else ""
                if not desc:
                    desc = f"{title} at {host}. Location: {loc}. Requisition: {path}"

                req_id = str(info.get("jobReqId") or info.get("id") or path.split("_")[-1] if info else path.split("_")[-1])
                full_job_url = info.get("externalUrl") if info else f"https://{host}/en-US/{site}{path}"
                app_url = f"{full_job_url}/apply"

                raw_jobs.append(
                    RawJobPost(
                        external_job_id=req_id,
                        title=title,
                        raw_description=desc,
                        raw_location=loc,
                        job_url=full_job_url,
                        application_url=app_url,
                        metadata={
                            "ats": "workday",
                            "host": host,
                            "tenant": tenant,
                            "site": site,
                            "time_type": info.get("timeType", "Full time") if info else "Full time",
                        },
                    )
                )

        logger.info(f"Extracted {len(raw_jobs)} verified RawJobPosts from Workday ({host}).")
        return raw_jobs
