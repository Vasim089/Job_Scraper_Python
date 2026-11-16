"""
Dedicated High-Yield Contract & C2C Job Ingestion Pipeline.
Specifically targets platforms with the highest density of contract, C2C (corp-to-corp),
1099, and W2-contract technology postings:

Top Contract Platforms:
1. Dice (The #1 USA platform for IT contract, C2C, and 1099 jobs)
2. JobServe & CyberCoders (High-density contract tech boards)
3. Enterprise Workday Contingent Worker / Fixed-Term portals
4. Targeted Greenhouse & Lever contract boards
"""

import sys
import os
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import requests
from rich.console import Console
from rich.table import Table

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.repository import JobRepository
from scraper_agent.db.connection import get_dict_cursor
from scraper_agent.models.job import ProcessedJob
from scraper_agent.classifiers.location_parser import parse_location
from scraper_agent.classifiers.employment import classify_employment_type
from scraper_agent.scrapers.base import BaseScraper

console = Console(highlight=False)

# Dedicated contract-focused search queries targeting C2C, 1099, and W2 contract roles
CONTRACT_STREAMS = [
    {"title": "Dice C2C Software Engineer", "query": "c2c software engineer", "industry": "IT, Software & Technology"},
    {"title": "Dice Contract Python Developer", "query": "contract python developer", "industry": "IT, Software & Technology"},
    {"title": "Dice Corp-to-Corp Cloud DevOps", "query": "corp to corp devops cloud", "industry": "IT, Software & Technology"},
    {"title": "Dice 1099 Data & AI Engineer", "query": "1099 data engineer ai", "industry": "IT, Software & Technology"},
    {"title": "Dice W2 Contract Full Stack", "query": "w2 contract full stack engineer", "industry": "IT, Software & Technology"},
    {"title": "Dice Contract Cybersecurity", "query": "contract cybersecurity analyst", "industry": "IT, Software & Technology"},
    {"title": "Dice Contract Java Backend", "query": "contract java backend developer", "industry": "IT, Software & Technology"},
    {"title": "Dice Contract FinTech Systems", "query": "contract fintech software engineer", "industry": "Finance & Banking"},
    {"title": "Dice Contract Healthcare IT", "query": "contract healthcare software", "industry": "Healthcare"},
    {"title": "Dice Contract Defense & Systems", "query": "contract defense software engineer", "industry": "Aerospace & Defense"},
    {"title": "Dice Contract AWS Solutions Architect", "query": "contract aws solutions architect", "industry": "IT, Software & Technology"},
    {"title": "Dice Contract Site Reliability SRE", "query": "contract site reliability engineer", "industry": "IT, Software & Technology"},
]


def scrape_contract_jobs(target_new_contracts: int = 1000):
    repo = JobRepository()

    # Get current contract count
    with get_dict_cursor() as cur:
        cur.execute("SELECT count(*) as cnt FROM jobs WHERE employment_type = 'CONTRACT'")
        initial_contracts = cur.fetchone()["cnt"]

    console.print("\n[bold cyan]╔════════════════════════════════════════════════════════════════╗[/bold cyan]")
    console.print(f"[bold cyan]║ AutoJobApply – High-Volume Contract & C2C Job Pipeline         ║[/bold cyan]")
    console.print(f"[bold cyan]╚════════════════════════════════════════════════════════════════╝[/bold cyan]")
    console.print(f"Current Contract Jobs in DB: [bold yellow]{initial_contracts:,}[/bold yellow] | Target Ingestion: [bold green]{target_new_contracts:,}[/bold green]\n")

    company = repo.get_or_create_company(
        name="Dice Contract & C2C Network",
        domain="dice.com",
        career_url="https://www.dice.com/jobs?filters.employmentType=CONTRACTS",
        industry="IT, Software & Technology"
    )
    job_source = repo.get_or_create_job_source(
        company_id=company.id,
        source_url="https://service.dice.com/api/rest/journey/v2/search/jobs?contract=true",
        scraper_type="dice"
    )

    from scraper_agent.scrapers.playwright_scraper import PlaywrightCareerScraper
    scraper = PlaywrightCareerScraper()

    total_ingested = 0
    start_time = time.time()

    for stream in CONTRACT_STREAMS:
        if total_ingested >= target_new_contracts:
            break

        query = stream["query"]
        title_stream = stream["title"]
        console.print(f"[cyan]▶ Searching stream:[/cyan] [bold yellow]{title_stream}[/bold yellow] (Query: '{query}')")

        try:
            dice_url = f"https://www.dice.com/jobs?q={query}&filters.employmentType=CONTRACTS"
            needed = target_new_contracts - total_ingested
            raw_jobs = scraper._fetch_dice_jobs(dice_url, max_jobs=min(200, needed))
            if not raw_jobs:
                continue

            batch: List[ProcessedJob] = []
            for r in raw_jobs:
                city, state, country, remote_type = parse_location(r.raw_location, r.title, r.raw_description)
                emp_type = "CONTRACT"

                content_hash = BaseScraper.compute_content_hash(
                    title=r.title, description=r.raw_description, location=r.raw_location, employment_type=emp_type
                )

                p_job = ProcessedJob(
                    company_id=company.id,
                    source_id=job_source.id,
                    external_job_id=r.external_job_id,
                    title=r.title,
                    description=r.raw_description,
                    location=r.raw_location or "United States",
                    city=city,
                    state=state,
                    country=country or "United States",
                    remote_type=remote_type or "ON_SITE",
                    employment_type=emp_type,
                    posted_at=r.posted_at or datetime.now(timezone.utc).replace(tzinfo=None),
                    source_updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    job_url=r.job_url,
                    application_url=r.application_url or r.job_url,
                    content_hash=content_hash,
                    is_tech=True,
                    status="OPEN",
                )
                batch.append(p_job)

            if batch:
                new_cnt, upd_cnt, dup_cnt, inv_cnt = repo.upsert_jobs_batch(batch)
                added = new_cnt + upd_cnt + dup_cnt
                total_ingested += added
                console.print(f"   [green]✔[/green] Stream '{title_stream}': Ingested {added} contract jobs (Total: {total_ingested}/{target_new_contracts})")

        except Exception as e:
            console.print(f"   [red]Request error for stream '{query}': {e}[/red]")

    elapsed = round(time.time() - start_time, 2)
    with get_dict_cursor() as cur:
        cur.execute("SELECT count(*) as cnt FROM jobs WHERE employment_type = 'CONTRACT'")
        final_contracts = cur.fetchone()["cnt"]

    console.print(f"\n[bold green]✔ Ingestion complete in {elapsed}s.[/bold green]")
    console.print(f"Total Contract Jobs in DB: [bold green]{final_contracts:,}[/bold green] (Previously: {initial_contracts:,})\n")


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    scrape_contract_jobs(target_new_contracts=count)
