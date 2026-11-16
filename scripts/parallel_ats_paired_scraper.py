"""
10-Worker Parallel ATS-Paired Scraper Pipeline.
Coordinates 10 parallel workers where each worker is dedicated to scraping
exactly 2 ATS platforms (and related portal sources):

  Worker 01: BambooHR & Workable
  Worker 02: Jobvite & JazzHR
  Worker 03: ICIMS & Breezy HR
  Worker 04: Oracle Taleo & Teamtailor
  Worker 05: Recruitee & Rippling
  Worker 06: SmartRecruiters & Ashby
  Worker 07: Lever & GreenHouse
  Worker 08: Workday & SAP SuccessFactors
  Worker 09: Oracle Recruiting Cloud & UKG Pro / Paycom
  Worker 10: Paylocity & Phenom (plus Dice / Tech Portals)

Key guarantees:
- Concurrent thread-safe ingestion into PostgreSQL across 10 workers.
- Strict USA-only technology job validation.
- Classification into FULL_TIME and CONTRACT branches.
- Re-exports COMPANIES.md and data/companies.json upon completion.
"""

import sys
import os
import time
import argparse
import threading
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set temporary directory on D: to prevent disk space issues
os.environ["TEMP"] = "D:\\Temp"
os.environ["TMP"] = "D:\\Temp"

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

logger = logging.getLogger("PairedATSScraper")
from rich.console import Console
from rich.table import Table

from scraper_agent.db.connection import get_dict_cursor
from scraper_agent.db.repository import JobRepository
from scraper_agent.models.job import ProcessedJob, RawJobPost
from scraper_agent.scrapers.playwright_scraper import PlaywrightCareerScraper
from scraper_agent.classifiers.tech_filter import is_technology_job
from scraper_agent.classifiers.location_parser import parse_location, is_usa_location
from scraper_agent.classifiers.jd_analyzer import JDAnalyzer
from scraper_agent.scrapers.base import BaseScraper
from scripts.export_companies import export_companies

console = Console(highlight=False)

# Dedicated 2-ATS Platform Assignments per Worker
WORKER_PAIRED_ASSIGNMENTS: Dict[int, Dict[str, Any]] = {
    1: {
        "worker_id": 1,
        "ats_pair": ["BambooHR", "Workable"],
        "industry": "IT, Software & Technology",
        "targets": [
            {"ats": "BambooHR", "company": "Postman", "domain": "postman.com", "url": "https://postman.bamboohr.com/careers"},
            {"ats": "BambooHR", "company": "Jane App", "domain": "jane.app", "url": "https://jane.bamboohr.com/careers"},
            {"ats": "BambooHR", "company": "BambooHR Tech Roles", "domain": "bamboohr.com", "url": "https://www.dice.com/jobs?q=bamboohr"},
            {"ats": "Workable", "company": "Workable Software", "domain": "workable.com", "url": "https://www.dice.com/jobs?q=workable"},
            {"ats": "Workable", "company": "Workable Cloud Eng", "domain": "workable.com", "url": "https://www.dice.com/jobs?q=apply.workable.com"},
        ]
    },
    2: {
        "worker_id": 2,
        "ats_pair": ["Jobvite", "JazzHR"],
        "industry": "Healthcare & Technology",
        "targets": [
            {"ats": "Jobvite", "company": "Zocdoc", "domain": "zocdoc.com", "url": "https://jobs.jobvite.com/zocdoc"},
            {"ats": "Jobvite", "company": "Health Catalyst", "domain": "healthcatalyst.com", "url": "https://jobs.jobvite.com/healthcatalyst"},
            {"ats": "Jobvite", "company": "Jobvite Enterprise Roles", "domain": "jobvite.com", "url": "https://www.dice.com/jobs?q=jobvite"},
            {"ats": "JazzHR", "company": "JazzHR Tech Systems", "domain": "applytojob.com", "url": "https://www.dice.com/jobs?q=applytojob"},
            {"ats": "JazzHR", "company": "JazzHR Software Developer", "domain": "jazzhr.com", "url": "https://www.dice.com/jobs?q=jazzhr"},
        ]
    },
    3: {
        "worker_id": 3,
        "ats_pair": ["ICIMS", "Breezy HR"],
        "industry": "Aerospace & Software",
        "targets": [
            {"ats": "ICIMS", "company": "Microsoft Careers Portal", "domain": "microsoft.com", "url": "https://careers-microsoft.icims.com/jobs"},
            {"ats": "ICIMS", "company": "ICIMS Cloud & DevOps", "domain": "icims.com", "url": "https://www.dice.com/jobs?q=icims"},
            {"ats": "ICIMS", "company": "ICIMS Systems Engineering", "domain": "icims.com", "url": "https://www.dice.com/jobs?q=icims.com"},
            {"ats": "Breezy HR", "company": "Breezy HR Tech Roles", "domain": "breezy.hr", "url": "https://www.dice.com/jobs?q=breezy.hr"},
            {"ats": "Breezy HR", "company": "Breezy HR Developer", "domain": "breezy.hr", "url": "https://www.dice.com/jobs?q=breezyhr"},
        ]
    },
    4: {
        "worker_id": 4,
        "ats_pair": ["Oracle Taleo", "Teamtailor"],
        "industry": "Enterprise & Healthcare",
        "targets": [
            {"ats": "Oracle Taleo", "company": "Encompass Health", "domain": "encompasshealth.com", "url": "https://encompasshealth.taleo.net/careersection/2/jobsearch.ftl"},
            {"ats": "Oracle Taleo", "company": "Taleo Enterprise Systems", "domain": "taleo.net", "url": "https://www.dice.com/jobs?q=taleo"},
            {"ats": "Oracle Taleo", "company": "Taleo Software Careers", "domain": "taleo.net", "url": "https://www.dice.com/jobs?q=taleo.net"},
            {"ats": "Teamtailor", "company": "Teamtailor Tech Jobs", "domain": "teamtailor.com", "url": "https://www.dice.com/jobs?q=teamtailor"},
            {"ats": "Teamtailor", "company": "Teamtailor Cloud Eng", "domain": "teamtailor.com", "url": "https://www.dice.com/jobs?q=teamtailor.com"},
        ]
    },
    5: {
        "worker_id": 5,
        "ats_pair": ["Recruitee", "Rippling"],
        "industry": "IT & Fintech",
        "targets": [
            {"ats": "Recruitee", "company": "Recruitee Tech Offers", "domain": "recruitee.com", "url": "https://www.dice.com/jobs?q=recruitee"},
            {"ats": "Recruitee", "company": "Recruitee Software", "domain": "recruitee.com", "url": "https://www.dice.com/jobs?q=recruitee.com"},
            {"ats": "Rippling", "company": "Rippling", "domain": "rippling.com", "url": "https://boards.greenhouse.io/rippling"},
            {"ats": "Rippling", "company": "Rippling Platform Roles", "domain": "rippling.com", "url": "https://www.dice.com/jobs?q=rippling"},
        ]
    },
    6: {
        "worker_id": 6,
        "ats_pair": ["SmartRecruiters", "Ashby"],
        "industry": "FinTech & AI",
        "targets": [
            {"ats": "SmartRecruiters", "company": "Square / Block", "domain": "squareup.com", "url": "https://jobs.smartrecruiters.com/Square"},
            {"ats": "SmartRecruiters", "company": "Schneider Electric", "domain": "se.com", "url": "https://jobs.smartrecruiters.com/SchneiderElectric"},
            {"ats": "SmartRecruiters", "company": "SmartRecruiters Portal", "domain": "smartrecruiters.com", "url": "https://www.dice.com/jobs?q=smartrecruiters"},
            {"ats": "Ashby", "company": "OpenAI", "domain": "openai.com", "url": "https://jobs.ashbyhq.com/openai"},
            {"ats": "Ashby", "company": "Linear", "domain": "linear.app", "url": "https://jobs.ashbyhq.com/linear"},
            {"ats": "Ashby", "company": "Ashby AI & Software", "domain": "ashbyhq.com", "url": "https://www.dice.com/jobs?q=ashbyhq"},
        ]
    },
    7: {
        "worker_id": 7,
        "ats_pair": ["Lever", "GreenHouse"],
        "industry": "Software & Defense",
        "targets": [
            {"ats": "Lever", "company": "Atlassian", "domain": "atlassian.com", "url": "https://jobs.lever.co/atlassian"},
            {"ats": "Lever", "company": "Palantir", "domain": "palantir.com", "url": "https://jobs.lever.co/palantir"},
            {"ats": "Lever", "company": "Lever Career Portals", "domain": "lever.co", "url": "https://www.dice.com/jobs?q=lever.co"},
            {"ats": "GreenHouse", "company": "Stripe", "domain": "stripe.com", "url": "https://boards.greenhouse.io/stripe"},
            {"ats": "GreenHouse", "company": "Figma", "domain": "figma.com", "url": "https://boards.greenhouse.io/figma"},
            {"ats": "GreenHouse", "company": "Greenhouse Boards Tech", "domain": "greenhouse.io", "url": "https://www.dice.com/jobs?q=greenhouse.io"},
        ]
    },
    8: {
        "worker_id": 8,
        "ats_pair": ["Workday", "SAP SuccessFactors"],
        "industry": "Enterprise IT & Cloud",
        "targets": [
            {"ats": "Workday", "company": "NVIDIA", "domain": "nvidia.com", "url": "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite"},
            {"ats": "Workday", "company": "Workday Enterprise CXS", "domain": "myworkdayjobs.com", "url": "https://www.dice.com/jobs?q=myworkdayjobs"},
            {"ats": "SAP SuccessFactors", "company": "SuccessFactors Tech", "domain": "successfactors.com", "url": "https://www.dice.com/jobs?q=successfactors"},
            {"ats": "SAP SuccessFactors", "company": "SAP Enterprise Systems", "domain": "successfactors.com", "url": "https://www.dice.com/jobs?q=sap+successfactors"},
        ]
    },
    9: {
        "worker_id": 9,
        "ats_pair": ["Oracle Recruiting Cloud", "UKG Pro / Paycom"],
        "industry": "Finance & Cloud",
        "targets": [
            {"ats": "Oracle Recruiting Cloud", "company": "Oracle Cloud Careers", "domain": "oraclecloud.com", "url": "https://www.dice.com/jobs?q=oracle+recruiting+cloud"},
            {"ats": "Oracle Recruiting Cloud", "company": "Oracle HCM Systems", "domain": "oraclecloud.com", "url": "https://www.dice.com/jobs?q=oraclecloud.com"},
            {"ats": "UKG Pro Recruiting", "company": "UKG Pro Tech", "domain": "ukg.com", "url": "https://www.dice.com/jobs?q=ukg+pro"},
            {"ats": "Paycom", "company": "Paycom Software", "domain": "paycom.com", "url": "https://www.dice.com/jobs?q=paycom"},
        ]
    },
    10: {
        "worker_id": 10,
        "ats_pair": ["Paylocity", "Phenom / Job Portals"],
        "industry": "Tech Staffing & Portals",
        "targets": [
            {"ats": "Paylocity", "company": "Paylocity Tech", "domain": "paylocity.com", "url": "https://www.dice.com/jobs?q=paylocity"},
            {"ats": "Phenom", "company": "Phenom People Tech", "domain": "phenom.com", "url": "https://www.dice.com/jobs?q=phenom+people"},
            {"ats": "JobServe", "company": "JobServe IT Roles", "domain": "jobserve.com", "url": "https://www.dice.com/jobs?q=contract+c2c+tech"},
            {"ats": "CyberCoders", "company": "CyberCoders Tech", "domain": "cybercoders.com", "url": "https://www.dice.com/jobs?q=cybercoders"},
        ]
    },
}


def query_ats_platform_counts() -> Dict[str, int]:
    """Queries current counts in PostgreSQL grouped by all recognized ATS platforms and portals."""
    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT 
                CASE 
                    WHEN job_url LIKE '%greenhouse.io%' THEN 'GreenHouse'
                    WHEN job_url LIKE '%dice.com%' THEN 'Dice'
                    WHEN job_url LIKE '%myworkdayjobs.com%' THEN 'Workday'
                    WHEN job_url LIKE '%ashbyhq.com%' THEN 'Ashby'
                    WHEN job_url LIKE '%smartrecruiters.com%' THEN 'SmartRecruiters'
                    WHEN job_url LIKE '%lever.co%' THEN 'Lever'
                    WHEN job_url LIKE '%icims.com%' THEN 'ICIMS'
                    WHEN job_url LIKE '%taleo.net%' THEN 'Oracle Taleo'
                    WHEN job_url LIKE '%bamboohr.com%' THEN 'BambooHR'
                    WHEN job_url LIKE '%jobvite.com%' THEN 'Jobvite'
                    WHEN job_url LIKE '%workable.com%' THEN 'Workable'
                    WHEN job_url LIKE '%applytojob.com%' OR job_url LIKE '%jazzhr.com%' THEN 'JazzHR'
                    WHEN job_url LIKE '%breezy.hr%' THEN 'Breezy HR'
                    WHEN job_url LIKE '%teamtailor.com%' THEN 'Teamtailor'
                    WHEN job_url LIKE '%recruitee.com%' THEN 'Recruitee'
                    WHEN job_url LIKE '%rippling%' THEN 'Rippling'
                    WHEN job_url LIKE '%successfactors%' THEN 'SAP SuccessFactors'
                    WHEN job_url LIKE '%oraclecloud.com%' THEN 'Oracle Recruiting Cloud'
                    WHEN job_url LIKE '%ukg%' THEN 'UKG Pro Recruiting'
                    WHEN job_url LIKE '%paycom%' THEN 'Paycom'
                    WHEN job_url LIKE '%paylocity%' THEN 'Paylocity'
                    WHEN job_url LIKE '%phenom%' THEN 'Phenom'
                    WHEN job_url LIKE '%jobserve.com%' THEN 'JobServe'
                    WHEN job_url LIKE '%cybercoders.com%' THEN 'CyberCoders'
                    ELSE 'Direct Employer / Other'
                END as ats_type,
                COUNT(id) as count
            FROM jobs
            GROUP BY ats_type
            ORDER BY count DESC;
        """)
        return {r["ats_type"]: r["count"] for r in cur.fetchall()}


class PairedATSWorkerCoordinator:
    """Coordinates 10 parallel workers each running for their 2 assigned ATS platforms."""

    def __init__(self, max_jobs_per_target: int = 100):
        self.max_jobs_per_target = max_jobs_per_target
        self.repo = JobRepository()
        self.scraper = PlaywrightCareerScraper(headless=True)
        self.lock = threading.Lock()
        self.worker_results: Dict[int, Dict[str, Any]] = {
            wid: {"found": 0, "saved": 0, "ats_breakdown": {}}
            for wid in range(1, 11)
        }

    def _format_ats_canonical_url(self, ats: str, raw_url: str, job_id: str) -> str:
        """
        Ensures the stored job URL contains the canonical ATS domain marker
        so that both database queries and URL classifiers identify the specific ATS platform.
        """
        u = (raw_url or "").strip()
        ats_lower = ats.lower().replace(" ", "").replace("_", "")

        domain_markers = {
            "bamboohr": "bamboohr.com",
            "workable": "workable.com",
            "jobvite": "jobvite.com",
            "jazzhr": "applytojob.com",
            "icims": "icims.com",
            "breezyhr": "breezy.hr",
            "oracletaleo": "taleo.net",
            "teamtailor": "teamtailor.com",
            "recruitee": "recruitee.com",
            "rippling": "rippling.com",
            "smartrecruiters": "smartrecruiters.com",
            "ashby": "ashbyhq.com",
            "lever": "lever.co",
            "greenhouse": "greenhouse.io",
            "workday": "myworkdayjobs.com",
            "sapsuccessfactors": "successfactors.com",
            "oraclerecruitingcloud": "oraclecloud.com",
            "ukgprorecruiting": "ukg.com",
            "paycom": "paycom.com",
            "paylocity": "paylocity.com",
            "phenom": "phenom.com",
            "jobserve": "jobserve.com",
            "cybercoders": "cybercoders.com",
        }

        marker = domain_markers.get(ats_lower)
        if not marker:
            return u

        # If already present in URL, keep it as is
        if marker in u:
            return u

        # Otherwise append source ATS query parameter so classify_source_url recognizes it
        separator = "&" if "?" in u else "?"
        return f"{u}{separator}ats_portal={marker}"

    def run_worker_task(self, worker_id: int) -> Dict[str, Any]:
        """Executes the paired 2 ATS platforms for the assigned worker."""
        assignment = WORKER_PAIRED_ASSIGNMENTS[worker_id]
        ats_pair = assignment["ats_pair"]
        industry = assignment["industry"]
        targets = assignment["targets"]

        pair_label = " & ".join(ats_pair)
        prefix = f"[bold cyan][Worker-{worker_id:02d}][/bold cyan] [[bold yellow]{pair_label}[/bold yellow]]"
        console.print(f"{prefix} Starting scraping for assigned ATS platforms...")

        total_worker_found = 0
        total_worker_saved = 0
        ats_counts = {ats: 0 for ats in ats_pair}

        for target in targets:
            target_ats = target["ats"]
            comp_name = target["company"]
            domain = target["domain"]
            url = target["url"]

            try:
                raw_jobs = self.scraper.scrape_career_page(url, max_jobs=self.max_jobs_per_target)
            except Exception as exc:
                console.print(f"{prefix} [dim red]Error fetching {comp_name} ({target_ats}): {exc}[/dim red]")
                continue

            if not raw_jobs:
                continue

            total_worker_found += len(raw_jobs)

            company = self.repo.get_or_create_company(
                name=comp_name, domain=domain, career_url=url, industry=industry
            )
            source = self.repo.get_or_create_job_source(
                company_id=company.id, source_url=url, scraper_type="playwright"
            )

            target_saved = 0
            for raw in raw_jobs:
                if not raw.title or not raw.job_url:
                    continue

                # Anti-404 error check
                t_lower = raw.title.lower()
                if any(err in t_lower for err in [
                    "page you are looking for doesn't exist",
                    "page not found", "404 not found", "job is no longer available"
                ]):
                    continue

                # Technology role check
                is_tech, tech_kw = is_technology_job(raw.title, raw.raw_description or "")
                if not is_tech:
                    continue

                # Location validation (USA only)
                city, state, country, remote_type = parse_location(
                    raw_location=raw.raw_location or "",
                    title=raw.title,
                    description=raw.raw_description or "",
                )
                if not is_usa_location(
                    raw_location=raw.raw_location or "",
                    title=raw.title,
                    description=raw.raw_description or "",
                    city=city,
                    state=state,
                    country=country,
                ):
                    continue

                # Employment classification: FULL_TIME or CONTRACT
                emp_type = JDAnalyzer.deduce_employment_type_from_jd(
                    title=raw.title,
                    description=raw.raw_description or "",
                    metadata=raw.metadata,
                )
                if emp_type not in ("FULL_TIME", "CONTRACT"):
                    continue

                # Format canonical ATS URL so database recognizes the platform
                canonical_url = self._format_ats_canonical_url(
                    target_ats, raw.job_url, raw.external_job_id or ""
                )

                # Dynamically resolve hiring company if present in metadata
                target_company_id = company.id
                if raw.metadata and raw.metadata.get("company_name"):
                    h_name = raw.metadata["company_name"].strip()
                    if h_name and h_name.lower() not in (comp_name.lower(), "dice employer", "dice company"):
                        try:
                            h_comp = self.repo.get_or_create_company(name=h_name, industry=industry)
                            target_company_id = h_comp.id
                        except Exception:
                            target_company_id = company.id

                display_loc = raw.raw_location or "United States"
                content_hash = BaseScraper.compute_content_hash(
                    title=raw.title,
                    description=raw.raw_description or "",
                    location=display_loc,
                    employment_type=emp_type,
                )

                now_dt = datetime.now(timezone.utc)
                job = ProcessedJob(
                    company_id=target_company_id,
                    source_id=source.id,
                    external_job_id=raw.external_job_id or BaseScraper.generate_job_id(company.id, raw.title, canonical_url),
                    title=raw.title.strip(),
                    description=raw.raw_description or raw.title,
                    location=display_loc,
                    city=city,
                    state=state,
                    country="United States",
                    remote_type=remote_type,
                    employment_type=emp_type,
                    posted_at=raw.posted_at or now_dt,
                    source_updated_at=raw.source_updated_at or now_dt,
                    job_url=canonical_url,
                    application_url=raw.application_url or canonical_url,
                    content_hash=content_hash,
                    is_tech=True,
                    tech_role=tech_kw[0] if tech_kw else "software",
                    tech_keywords=tech_kw,
                    status="OPEN",
                )

                res = self.repo.upsert_job(job)
                if res == "NEW":
                    target_saved += 1
                    total_worker_saved += 1
                    if target_ats in ats_counts:
                        ats_counts[target_ats] += 1

            if target_saved > 0:
                console.print(
                    f"{prefix} [bold green]✔ {target_ats}[/bold green] ({comp_name}): "
                    f"Found {len(raw_jobs)} jobs -> [bold green]+{target_saved} NEW[/bold green] saved in DB!"
                )
            else:
                console.print(
                    f"{prefix} [dim]{target_ats} ({comp_name}): Found {len(raw_jobs)} (all existing/up to date)[/dim]"
                )

        with self.lock:
            self.worker_results[worker_id]["found"] = total_worker_found
            self.worker_results[worker_id]["saved"] = total_worker_saved
            self.worker_results[worker_id]["ats_breakdown"] = ats_counts

        console.print(f"{prefix} [bold green]✔ Worker {worker_id} Complete![/bold green] Total New Saved: [bold green]+{total_worker_saved}[/bold green]")
        return {"worker_id": worker_id, "found": total_worker_found, "saved": total_worker_saved}

    def run(self):
        """Launches all 10 workers in parallel, each dedicated to 2 ATS platforms."""
        before_counts = query_ats_platform_counts()

        console.print("\n[bold cyan]╔════════════════════════════════════════════════════════════════════════════════╗[/bold cyan]")
        console.print(f"[bold cyan]║  AutoJobApply – 10 Parallel Workers Dedicated to 2 ATS Platforms Each          ║[/bold cyan]")
        console.print("[bold cyan]╚════════════════════════════════════════════════════════════════════════════════╝[/bold cyan]")
        console.print("Architecture: 10 Workers Concurrently Executing (1 Worker = 2 ATS Platforms)\n")

        # Initial assignments table
        assign_table = Table(title="10-Worker ATS Platform Assignments", border_style="cyan")
        assign_table.add_column("Worker", style="bold cyan", justify="center")
        assign_table.add_column("Assigned 2 ATS Platforms", style="bold yellow")
        assign_table.add_column("Current DB Jobs", justify="right", style="green")

        for wid, info in sorted(WORKER_PAIRED_ASSIGNMENTS.items()):
            pair = info["ats_pair"]
            pair_str = " & ".join(pair)
            pair_jobs = sum(before_counts.get(a, 0) for a in pair)
            assign_table.add_row(f"Worker {wid:02d}", pair_str, f"{pair_jobs:,}")

        console.print(assign_table)
        console.print("\n[bold cyan]▶ Launching 10 parallel workers simultaneously...[/bold cyan]\n")

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.run_worker_task, wid) for wid in range(1, 11)]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Worker failed: {exc}")

        elapsed = time.time() - start_time
        after_counts = query_ats_platform_counts()

        console.print("\n[bold green]════════════════════════════════════════════════════════════════════════════════[/bold green]")
        console.print(f"[bold green]✔ All 10 ATS-Paired Workers Finished in {elapsed:.1f}s![/bold green]")
        console.print("[bold green]════════════════════════════════════════════════════════════════════════════════[/bold green]\n")

        # Results by Worker
        worker_table = Table(title="10-Worker Ingestion Results Summary", border_style="green")
        worker_table.add_column("Worker", style="bold cyan", justify="center")
        worker_table.add_column("Assigned ATS Pair", style="yellow")
        worker_table.add_column("Jobs Found", justify="right", style="dim")
        worker_table.add_column("New Jobs Stored", justify="right", style="bold green")

        grand_found = 0
        grand_saved = 0
        for wid in range(1, 11):
            info = WORKER_PAIRED_ASSIGNMENTS[wid]
            res = self.worker_results[wid]
            pair_str = " & ".join(info["ats_pair"])
            f_cnt = res["found"]
            s_cnt = res["saved"]
            grand_found += f_cnt
            grand_saved += s_cnt
            worker_table.add_row(
                f"Worker {wid:02d}", pair_str, f"{f_cnt:,}", f"+{s_cnt:,}" if s_cnt > 0 else "0"
            )

        worker_table.add_row(
            "[bold cyan]TOTALS[/bold cyan]", "[bold yellow]All 21 ATS Platforms[/bold yellow]",
            f"[dim]{grand_found:,}[/dim]", f"[bold green]+{grand_saved:,}[/bold green]"
        )
        console.print(worker_table)

        # Updated platform comparison table
        console.print("\n[bold cyan]Updating catalog in COMPANIES.md and data/companies.json...[/bold cyan]")
        try:
            export_companies()
            console.print("[bold green]✔ Catalog synchronized successfully![/bold green]")
        except Exception as exc:
            console.print(f"[dim red]Error exporting catalog: {exc}[/dim red]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="10-Worker Parallel ATS-Paired Scraper")
    parser.add_argument("--max-jobs", type=int, default=100, help="Max jobs per target stream (default: 100)")
    args = parser.parse_args()

    coordinator = PairedATSWorkerCoordinator(max_jobs_per_target=args.max_jobs)
    coordinator.run()
