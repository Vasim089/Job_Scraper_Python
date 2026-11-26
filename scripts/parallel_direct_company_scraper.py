"""
10-Worker Parallel Direct Company Career Pages Scraper.
Coordinates 10 concurrent workers scraping jobs directly from verified company
career portals (e.g., https://company.com/careers) across all target industries:

  Worker 01: IT, Software & Technology Direct Career Portals
  Worker 02: Aerospace & Defense Direct Career Portals
  Worker 03: Finance & Banking Direct Career Portals
  Worker 04: Healthcare & MedTech Direct Career Portals
  Worker 05: Solar, Energy & Utilities Direct Career Portals
  Worker 06: Retail, E-Commerce & Tech Direct Career Portals
  Worker 07: Pharma & Biotechnology Direct Career Portals
  Worker 08: Enterprise IT, Cloud & Systems Direct Portals
  Worker 09: Defense, Security & Hardware Direct Portals
  Worker 10: High-Growth Startups & Multi-Industry Direct Portals

Key guarantees:
- Concurrent thread-safe ingestion into PostgreSQL across 10 workers.
- Strict USA-only technology job validation.
- Classification into FULL_TIME and CONTRACT branches.
- Re-exports COMPANIES.md and data/companies.json upon completion.
"""

import sys
import os
import time
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

logger = logging.getLogger("DirectCompanyScraper")
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

# Industry assignments for the 10 workers
WORKER_INDUSTRY_MAPPING = {
    1: {"name": "Worker 01", "industry": "IT, Software & Technology", "label": "IT & Software Direct Portals"},
    2: {"name": "Worker 02", "industry": "Aerospace & Defense", "label": "Aerospace & Defense Direct Portals"},
    3: {"name": "Worker 03", "industry": "Finance & Banking", "label": "Finance & Banking Direct Portals"},
    4: {"name": "Worker 04", "industry": "Healthcare", "label": "Healthcare & MedTech Direct Portals"},
    5: {"name": "Worker 05", "industry": "Solar, Energy & Utilities", "label": "Solar & Energy Direct Portals"},
    6: {"name": "Worker 06", "industry": "Retail & Technology", "label": "Retail Tech Direct Portals"},
    7: {"name": "Worker 07", "industry": "Pharma & Biotechnology", "label": "Pharma & Biotech Direct Portals"},
    8: {"name": "Worker 08", "industry": "Enterprise IT & Cloud", "label": "Enterprise IT Direct Portals"},
    9: {"name": "Worker 09", "industry": "Defense & Systems", "label": "Defense & Systems Direct Portals"},
    10: {"name": "Worker 10", "industry": "ALL", "label": "Multi-Industry Direct Portals"},
}


class DirectCompanyWorkerCoordinator:
    """Orchestrates 10 parallel workers scraping directly from company career pages."""

    def __init__(self, max_workers: int = 10, max_jobs_per_company: int = 100):
        self.max_workers = max_workers
        self.max_jobs_per_company = max_jobs_per_company
        self.repo = JobRepository()
        self.scraper = PlaywrightCareerScraper()
        self.lock = threading.Lock()
        self.worker_results: Dict[int, Dict[str, Any]] = {
            i: {"found": 0, "saved": 0, "companies_checked": 0} for i in range(1, max_workers + 1)
        }

    def load_direct_companies(self) -> List[Dict[str, Any]]:
        """Loads all companies with direct career portal URLs from data/companies.json or PostgreSQL."""
        json_path = ROOT_DIR / "data" / "companies.json"
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                direct = [
                    d for d in data
                    if d.get("career_url") and (
                        d.get("ats_platform") in ("Direct Career Portal", "Direct Employer / Other", "Direct")
                        or not any(a in d.get("career_url", "").lower() for a in [
                            "greenhouse.io", "lever.co", "myworkdayjobs.com", "ashbyhq.com",
                            "smartrecruiters.com", "bamboohr.com", "jobvite.com", "icims.com",
                            "taleo.net", "dice.com"
                        ])
                    )
                ]
                if direct:
                    return direct
            except Exception as e:
                logger.warning(f"Failed to read data/companies.json: {e}")

        # Fallback to database
        with get_dict_cursor() as cur:
            cur.execute("""
                SELECT id, name, domain, career_url, industry, active
                FROM companies
                WHERE active = TRUE AND career_url IS NOT NULL AND career_url != ''
                ORDER BY id ASC
            """)
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def shard_companies_to_workers(self, companies: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        """Distributes direct companies among the 10 workers based on industry and round-robin."""
        shards: Dict[int, List[Dict[str, Any]]] = {i: [] for i in range(1, self.max_workers + 1)}

        # Map by industry first
        industry_map = {
            "it, software & technology": 1,
            "aerospace & defense": 2,
            "finance & banking": 3,
            "healthcare": 4,
            "solar, energy & utilities": 5,
            "retail & technology": 6,
            "pharma & biotechnology": 7,
            "enterprise it & cloud": 8,
            "defense & systems": 9,
        }

        unassigned = []
        for comp in companies:
            ind = (comp.get("industry") or "").strip().lower()
            assigned_worker = industry_map.get(ind)
            if assigned_worker and assigned_worker <= self.max_workers:
                shards[assigned_worker].append(comp)
            else:
                unassigned.append(comp)

        # Distribute remaining/unassigned companies evenly (Worker 10 + round-robin)
        for i, comp in enumerate(unassigned):
            wid = (i % self.max_workers) + 1
            shards[wid].append(comp)

        return shards

    def run_worker_task(self, worker_id: int, assigned_companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Worker task executing direct career page scraping for assigned companies."""
        meta = WORKER_INDUSTRY_MAPPING.get(worker_id, {"label": f"Worker {worker_id}", "industry": "General"})
        prefix = f"[bold cyan][Worker-{worker_id:02d}][/bold cyan] [[bold yellow]{meta['label']}[/bold yellow]]"

        console.print(f"{prefix} Starting scraping across {len(assigned_companies)} direct company career pages...")

        total_found = 0
        total_saved = 0
        companies_checked = 0

        for comp in assigned_companies:
            comp_name = comp.get("name") or "Unknown"
            career_url = comp.get("career_url")
            domain = comp.get("domain") or ""
            industry = comp.get("industry") or meta.get("industry") or "General Technology"

            if not career_url:
                continue

            companies_checked += 1

            try:
                raw_jobs = self.scraper.scrape_career_page(career_url, max_jobs=self.max_jobs_per_company)
            except Exception as exc:
                logger.debug(f"{prefix} Error scraping {comp_name} ({career_url}): {exc}")
                continue

            if not raw_jobs:
                continue

            total_found += len(raw_jobs)

            # Ensure company and job_source exist in DB
            db_company = self.repo.get_or_create_company(
                name=comp_name, domain=domain, career_url=career_url, industry=industry
            )
            source = self.repo.get_or_create_job_source(
                company_id=db_company.id, source_url=career_url, scraper_type="playwright"
            )

            comp_saved = 0
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

                canonical_url = raw.job_url
                content_hash = BaseScraper.compute_content_hash(
                    title=raw.title,
                    description=raw.raw_description or "",
                    location=f"{city}, {state}, USA" if city and state else "USA",
                    employment_type=emp_type,
                )

                now_dt = datetime.now(timezone.utc)
                job = ProcessedJob(
                    company_id=db_company.id,
                    source_id=source.id,
                    external_job_id=raw.external_job_id or BaseScraper.generate_job_id(db_company.id, raw.title, canonical_url),
                    title=raw.title.strip(),
                    description=raw.raw_description or raw.title,
                    location=f"{city}, {state}, USA" if city and state else "USA",
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
                    comp_saved += 1
                    total_saved += 1

            if comp_saved > 0:
                console.print(
                    f"{prefix} [bold green]✔ Direct Career Page[/bold green] ({comp_name}): "
                    f"Found {len(raw_jobs)} jobs -> [bold green]+{comp_saved} NEW[/bold green] saved in DB!"
                )

        with self.lock:
            self.worker_results[worker_id]["found"] = total_found
            self.worker_results[worker_id]["saved"] = total_saved
            self.worker_results[worker_id]["companies_checked"] = companies_checked

        console.print(
            f"{prefix} [bold green]✔ Worker {worker_id} Complete![/bold green] "
            f"Checked {companies_checked} companies, Saved [bold green]+{total_saved}[/bold green] NEW jobs."
        )
        return {"worker_id": worker_id, "found": total_found, "saved": total_saved}

    def run(self):
        """Launches all 10 workers in parallel across direct company career pages."""
        all_direct = self.load_direct_companies()

        console.print("\n[bold cyan]╔════════════════════════════════════════════════════════════════════════════════╗[/bold cyan]")
        console.print(f"[bold cyan]║  AutoJobApply – 10 Parallel Workers Direct Company Career Pages Scraper        ║[/bold cyan]")
        console.print("[bold cyan]╚════════════════════════════════════════════════════════════════════════════════╝[/bold cyan]")
        console.print(f"Architecture: 10 Workers Concurrently Scraping {len(all_direct):,} Direct Employer Portals\n")

        shards = self.shard_companies_to_workers(all_direct)

        # Initial assignments table
        assign_table = Table(title="10-Worker Direct Company Assignments", border_style="cyan")
        assign_table.add_column("Worker", style="bold cyan", justify="center")
        assign_table.add_column("Target Sector / Scope", style="bold yellow")
        assign_table.add_column("Assigned Direct Companies", justify="right", style="green")

        for wid in range(1, self.max_workers + 1):
            info = WORKER_INDUSTRY_MAPPING.get(wid, {"label": f"Worker {wid}"})
            comp_count = len(shards.get(wid, []))
            assign_table.add_row(f"Worker {wid:02d}", info["label"], f"{comp_count:,}")

        console.print(assign_table)
        console.print("\n[bold cyan]▶ Launching 10 parallel direct company workers simultaneously...[/bold cyan]\n")

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="DirectWorker") as executor:
            futures = {
                executor.submit(self.run_worker_task, wid, shards.get(wid, [])): wid
                for wid in range(1, self.max_workers + 1)
            }
            for fut in as_completed(futures):
                wid = futures[fut]
                try:
                    fut.result()
                except Exception as exc:
                    console.print(f"[bold red]Worker {wid:02d} encountered unexpected error: {exc}[/bold red]")

        elapsed = time.time() - start_time

        # Summary Table
        summary_table = Table(title="Direct Company Parallel Scraper Final Summary", border_style="green")
        summary_table.add_column("Worker", style="bold cyan", justify="center")
        summary_table.add_column("Sector Scope", style="bold yellow")
        summary_table.add_column("Companies Checked", justify="right", style="cyan")
        summary_table.add_column("Total Found", justify="right", style="blue")
        summary_table.add_column("New Saved in DB", justify="right", style="bold green")

        grand_found = 0
        grand_saved = 0
        grand_checked = 0

        for wid in range(1, self.max_workers + 1):
            info = WORKER_INDUSTRY_MAPPING.get(wid, {"label": f"Worker {wid}"})
            res = self.worker_results[wid]
            grand_found += res["found"]
            grand_saved += res["saved"]
            grand_checked += res["companies_checked"]
            summary_table.add_row(
                f"Worker {wid:02d}",
                info["label"],
                f"{res['companies_checked']:,}",
                f"{res['found']:,}",
                f"+{res['saved']:,}",
            )

        summary_table.add_section()
        summary_table.add_row("TOTAL", "All 10 Workers", f"{grand_checked:,}", f"{grand_found:,}", f"+{grand_saved:,}")
        console.print("\n")
        console.print(summary_table)
        console.print(f"\n[bold green]✔ Direct Scraper Run Completed in {elapsed:.1f}s. Grand Total New Jobs Saved: +{grand_saved:,}[/bold green]")

        # Automatically re-export COMPANIES.md and data/companies.json
        console.print("[dim]Updating COMPANIES.md and data/companies.json with fresh counts...[/dim]")
        try:
            export_companies()
            console.print("[bold green]✔ Successfully refreshed COMPANIES.md and data/companies.json![/bold green]")
        except Exception as e:
            logger.warning(f"Could not re-export companies: {e}")


if __name__ == "__main__":
    DirectCompanyWorkerCoordinator(max_workers=10, max_jobs_per_company=100).run()
