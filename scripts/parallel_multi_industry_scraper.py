"""
10-Worker Parallel Multi-Industry Scraper.
Runs 10 concurrent workers scraping in parallel across all 7 Target Industries:
  1. Insurance
  2. Pharma & Biotechnology
  3. Solar, Energy & Utilities
  4. Healthcare
  5. Finance & Banking
  6. Aerospace & Defense
  7. IT, Software & Technology

Features:
- Thread-safe concurrent execution across 10 workers using ThreadPoolExecutor.
- Round-robin task distribution guaranteeing balanced job count growth across all 7 industries.
- Full USA-only tech filtering, contract/full-time classification, and anti-404 error rejection.
- Automatically refreshes COMPANIES.md and data/companies.json upon completion.
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

logger = logging.getLogger("MultiIndustryWorker")
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

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

# The 7 Canonical Target Industries
TARGET_INDUSTRIES = [
    "Insurance",
    "Pharma & Biotechnology",
    "Solar, Energy & Utilities",
    "Healthcare",
    "Finance & Banking",
    "Aerospace & Defense",
    "IT, Software & Technology",
]

# Curated High-Yield Targets for all 7 Target Industries
MULTI_INDUSTRY_TARGETS = [
    # ── 1. Insurance ────────────────────────────────────────────────────────
    {"company": "Dice InsurTech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=insurance+software", "industry": "Insurance"},
    {"company": "Dice Insurance Systems", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=insurtech", "industry": "Insurance"},
    {"company": "Dice Actuarial Tech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=actuarial+developer", "industry": "Insurance"},
    {"company": "Dice Claims Software", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=claims+systems", "industry": "Insurance"},
    {"company": "Lemonade", "domain": "lemonade.com", "url": "https://boards.greenhouse.io/lemonade", "industry": "Insurance"},
    {"company": "Root Insurance", "domain": "joinroot.com", "url": "https://boards.greenhouse.io/joinroot", "industry": "Insurance"},
    {"company": "Hippo Insurance", "domain": "hippo.com", "url": "https://boards.greenhouse.io/hippo", "industry": "Insurance"},
    {"company": "Policygenius", "domain": "policygenius.com", "url": "https://boards.greenhouse.io/policygenius", "industry": "Insurance"},
    {"company": "Kin Insurance", "domain": "kin.com", "url": "https://boards.greenhouse.io/kininsurance", "industry": "Insurance"},
    {"company": "Next Insurance", "domain": "nextinsurance.com", "url": "https://boards.greenhouse.io/nextinsurance", "industry": "Insurance"},
    {"company": "Pie Insurance", "domain": "pieinsurance.com", "url": "https://boards.greenhouse.io/pieinsurance", "industry": "Insurance"},
    {"company": "Clearcover", "domain": "clearcover.com", "url": "https://boards.greenhouse.io/clearcover", "industry": "Insurance"},
    {"company": "Ethos Life", "domain": "ethoslife.com", "url": "https://boards.greenhouse.io/ethoslife", "industry": "Insurance"},
    {"company": "Corvus Insurance", "domain": "corvusinsurance.com", "url": "https://boards.greenhouse.io/corvus", "industry": "Insurance"},
    {"company": "Openly", "domain": "openly.com", "url": "https://boards.greenhouse.io/openly", "industry": "Insurance"},
    {"company": "At-Bay", "domain": "at-bay.com", "url": "https://boards.greenhouse.io/atbay", "industry": "Insurance"},
    {"company": "AgentSync", "domain": "agentsync.io", "url": "https://boards.greenhouse.io/agentsync", "industry": "Insurance"},
    {"company": "Vouch", "domain": "vouch.us", "url": "https://boards.greenhouse.io/vouch", "industry": "Insurance"},
    {"company": "Boost Insurance", "domain": "boostinsurance.com", "url": "https://boards.greenhouse.io/boostinsurance", "industry": "Insurance"},
    {"company": "Coalition", "domain": "coalitioninc.com", "url": "https://jobs.lever.co/coalition", "industry": "Insurance"},
    {"company": "Cowbell Cyber", "domain": "cowbell.insure", "url": "https://jobs.lever.co/cowbellcyber", "industry": "Insurance"},
    {"company": "Branch Insurance", "domain": "ourbranch.com", "url": "https://jobs.lever.co/ourbranch", "industry": "Insurance"},
    {"company": "Newfront", "domain": "newfront.com", "url": "https://jobs.ashbyhq.com/newfront", "industry": "Insurance"},

    # ── 2. Pharma & Biotechnology ──────────────────────────────────────────
    {"company": "Dice BioTech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=bioinformatics", "industry": "Pharma & Biotechnology"},
    {"company": "Dice Pharma Tech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=biotech+software", "industry": "Pharma & Biotechnology"},
    {"company": "Dice Clinical Data", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=clinical+data", "industry": "Pharma & Biotechnology"},
    {"company": "Dice Life Sciences", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=computational+biology", "industry": "Pharma & Biotechnology"},
    {"company": "Ginkgo Bioworks", "domain": "ginkgobioworks.com", "url": "https://boards.greenhouse.io/ginkgobioworks", "industry": "Pharma & Biotechnology"},
    {"company": "Twist Bioscience", "domain": "twistbioscience.com", "url": "https://boards.greenhouse.io/twistbioscience", "industry": "Pharma & Biotechnology"},
    {"company": "Prime Medicine", "domain": "primemedicine.com", "url": "https://boards.greenhouse.io/primemedicine", "industry": "Pharma & Biotechnology"},
    {"company": "Beam Therapeutics", "domain": "beamtx.com", "url": "https://boards.greenhouse.io/beamtherapeutics", "industry": "Pharma & Biotechnology"},
    {"company": "Recursion Pharmaceuticals", "domain": "recursion.com", "url": "https://boards.greenhouse.io/recursionpharma", "industry": "Pharma & Biotechnology"},
    {"company": "Tempus AI", "domain": "tempus.com", "url": "https://boards.greenhouse.io/tempus", "industry": "Pharma & Biotechnology"},
    {"company": "Resilience", "domain": "resilience.com", "url": "https://boards.greenhouse.io/resilience", "industry": "Pharma & Biotechnology"},
    {"company": "10x Genomics", "domain": "10xgenomics.com", "url": "https://boards.greenhouse.io/10xgenomics", "industry": "Pharma & Biotechnology"},
    {"company": "Mammoth Biosciences", "domain": "mammoth.bio", "url": "https://boards.greenhouse.io/mammothbiosci", "industry": "Pharma & Biotechnology"},
    {"company": "Sana Biotechnology", "domain": "sana.com", "url": "https://boards.greenhouse.io/sanabiotechnology", "industry": "Pharma & Biotechnology"},
    {"company": "Relay Therapeutics", "domain": "relaytx.com", "url": "https://boards.greenhouse.io/relaytx", "industry": "Pharma & Biotechnology"},
    {"company": "Benchling", "domain": "benchling.com", "url": "https://boards.greenhouse.io/benchling", "industry": "Pharma & Biotechnology"},
    {"company": "Schrodinger", "domain": "schrodinger.com", "url": "https://boards.greenhouse.io/schrodinger", "industry": "Pharma & Biotechnology"},
    {"company": "Insitro", "domain": "insitro.com", "url": "https://jobs.lever.co/insitro", "industry": "Pharma & Biotechnology"},
    {"company": "Altos Labs", "domain": "altoslabs.com", "url": "https://jobs.lever.co/altoslabs", "industry": "Pharma & Biotechnology"},
    {"company": "Arsenal Biosciences", "domain": "arsenalbio.com", "url": "https://jobs.lever.co/arsenalbio", "industry": "Pharma & Biotechnology"},

    # ── 3. Solar, Energy & Utilities ─────────────────────────────────────────
    {"company": "Dice CleanTech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=energy+software", "industry": "Solar, Energy & Utilities"},
    {"company": "Dice Renewable Energy", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=renewable+energy+software", "industry": "Solar, Energy & Utilities"},
    {"company": "Dice Utilities Tech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=smart+grid", "industry": "Solar, Energy & Utilities"},
    {"company": "Schneider Electric", "domain": "se.com", "url": "https://jobs.smartrecruiters.com/SchneiderElectric", "industry": "Solar, Energy & Utilities"},
    {"company": "ChargePoint", "domain": "chargepoint.com", "url": "https://boards.greenhouse.io/chargepoint", "industry": "Solar, Energy & Utilities"},
    {"company": "Redwood Materials", "domain": "redwoodmaterials.com", "url": "https://boards.greenhouse.io/redwoodmaterials", "industry": "Solar, Energy & Utilities"},
    {"company": "Aurora Solar", "domain": "aurorasolar.com", "url": "https://boards.greenhouse.io/aurorasolar", "industry": "Solar, Energy & Utilities"},
    {"company": "Form Energy", "domain": "formenergy.com", "url": "https://boards.greenhouse.io/formenergy", "industry": "Solar, Energy & Utilities"},
    {"company": "Palmetto", "domain": "palmetto.com", "url": "https://boards.greenhouse.io/palmetto", "industry": "Solar, Energy & Utilities"},
    {"company": "Stem Inc", "domain": "stem.com", "url": "https://boards.greenhouse.io/steminc", "industry": "Solar, Energy & Utilities"},
    {"company": "Arcadia", "domain": "arcadia.com", "url": "https://boards.greenhouse.io/arcadia", "industry": "Solar, Energy & Utilities"},
    {"company": "Sunnova", "domain": "sunnova.com", "url": "https://boards.greenhouse.io/sunnova", "industry": "Solar, Energy & Utilities"},
    {"company": "Nextracker", "domain": "nextracker.com", "url": "https://boards.greenhouse.io/nextracker", "industry": "Solar, Energy & Utilities"},
    {"company": "Span", "domain": "span.io", "url": "https://jobs.lever.co/span", "industry": "Solar, Energy & Utilities"},
    {"company": "Fluence Energy", "domain": "fluenceenergy.com", "url": "https://jobs.lever.co/fluence", "industry": "Solar, Energy & Utilities"},
    {"company": "Crusoe Energy", "domain": "crusoeenergy.com", "url": "https://jobs.lever.co/crusoeenergy", "industry": "Solar, Energy & Utilities"},
    {"company": "Ample", "domain": "ample.com", "url": "https://jobs.lever.co/ample", "industry": "Solar, Energy & Utilities"},
    {"company": "Electric Hydrogen", "domain": "eh2.com", "url": "https://jobs.lever.co/eh2", "industry": "Solar, Energy & Utilities"},
    {"company": "Commonwealth Fusion", "domain": "cfs.energy", "url": "https://jobs.lever.co/cfs", "industry": "Solar, Energy & Utilities"},
    {"company": "Helion Energy", "domain": "helionenergy.com", "url": "https://jobs.lever.co/helionenergy", "industry": "Solar, Energy & Utilities"},
    {"company": "Antora Energy", "domain": "antoraenergy.com", "url": "https://jobs.lever.co/antoraenergy", "industry": "Solar, Energy & Utilities"},
    {"company": "Fervo Energy", "domain": "fervoenergy.com", "url": "https://jobs.ashbyhq.com/fervoenergy", "industry": "Solar, Energy & Utilities"},

    # ── 4. Healthcare ───────────────────────────────────────────────────────
    {"company": "Dice HealthTech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=healthcare+software", "industry": "Healthcare"},
    {"company": "Dice Medical Systems", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=healthtech", "industry": "Healthcare"},
    {"company": "Dice Epic Systems", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=epic+analyst", "industry": "Healthcare"},
    {"company": "Oscar Health", "domain": "hioscar.com", "url": "https://boards.greenhouse.io/oscar", "industry": "Healthcare"},
    {"company": "Komodo Health", "domain": "komodohealth.com", "url": "https://boards.greenhouse.io/komodohealth", "industry": "Healthcare"},
    {"company": "Ro", "domain": "ro.co", "url": "https://boards.greenhouse.io/ro", "industry": "Healthcare"},
    {"company": "Datavant", "domain": "datavant.com", "url": "https://boards.greenhouse.io/datavant", "industry": "Healthcare"},
    {"company": "GoodRx", "domain": "goodrx.com", "url": "https://boards.greenhouse.io/goodrx", "industry": "Healthcare"},
    {"company": "Hinge Health", "domain": "hingehealth.com", "url": "https://boards.greenhouse.io/hingehealth", "industry": "Healthcare"},
    {"company": "Maven Clinic", "domain": "mavenclinic.com", "url": "https://boards.greenhouse.io/mavenclinic", "industry": "Healthcare"},
    {"company": "Flatiron Health", "domain": "flatiron.com", "url": "https://boards.greenhouse.io/flatironhealth", "industry": "Healthcare"},
    {"company": "Cedar", "domain": "cedar.com", "url": "https://boards.greenhouse.io/cedar", "industry": "Healthcare"},
    {"company": "Lyra Health", "domain": "lyrahealth.com", "url": "https://boards.greenhouse.io/lyrahealth", "industry": "Healthcare"},
    {"company": "Omada Health", "domain": "omadahealth.com", "url": "https://boards.greenhouse.io/omadahealth", "industry": "Healthcare"},
    {"company": "Clover Health", "domain": "cloverhealth.com", "url": "https://boards.greenhouse.io/cloverhealth", "industry": "Healthcare"},
    {"company": "HealthVerity", "domain": "healthverity.com", "url": "https://boards.greenhouse.io/healthverity", "industry": "Healthcare"},
    {"company": "Cityblock Health", "domain": "cityblock.com", "url": "https://boards.greenhouse.io/cityblockhealth", "industry": "Healthcare"},
    {"company": "Carbon Health", "domain": "carbonhealth.com", "url": "https://boards.greenhouse.io/carbonhealth", "industry": "Healthcare"},
    {"company": "Alto Pharmacy", "domain": "alto.com", "url": "https://boards.greenhouse.io/altopharmacy", "industry": "Healthcare"},
    {"company": "Headway", "domain": "headway.co", "url": "https://jobs.ashbyhq.com/headway", "industry": "Healthcare"},
    {"company": "Spring Health", "domain": "springhealth.com", "url": "https://jobs.ashbyhq.com/springhealth", "industry": "Healthcare"},
    {"company": "Color Health", "domain": "color.com", "url": "https://jobs.lever.co/color", "industry": "Healthcare"},
    {"company": "Forward Health", "domain": "goforward.com", "url": "https://jobs.lever.co/forward", "industry": "Healthcare"},

    # ── 5. Finance & Banking ────────────────────────────────────────────────
    {"company": "Dice FinTech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=fintech", "industry": "Finance & Banking"},
    {"company": "Dice Financial Engineer", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=financial+software+engineer", "industry": "Finance & Banking"},
    {"company": "Dice Banking Developer", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=banking+developer", "industry": "Finance & Banking"},
    {"company": "Robinhood", "domain": "robinhood.com", "url": "https://boards.greenhouse.io/robinhood", "industry": "Finance & Banking"},
    {"company": "Coinbase", "domain": "coinbase.com", "url": "https://boards.greenhouse.io/coinbase", "industry": "Finance & Banking"},
    {"company": "Affirm", "domain": "affirm.com", "url": "https://boards.greenhouse.io/affirm", "industry": "Finance & Banking"},
    {"company": "Brex", "domain": "brex.com", "url": "https://boards.greenhouse.io/brex", "industry": "Finance & Banking"},
    {"company": "Upstart", "domain": "upstart.com", "url": "https://boards.greenhouse.io/upstart", "industry": "Finance & Banking"},
    {"company": "Chime", "domain": "chime.com", "url": "https://boards.greenhouse.io/chime", "industry": "Finance & Banking"},
    {"company": "SoFi", "domain": "sofi.com", "url": "https://boards.greenhouse.io/sofi", "industry": "Finance & Banking"},
    {"company": "Wealthfront", "domain": "wealthfront.com", "url": "https://boards.greenhouse.io/wealthfront", "industry": "Finance & Banking"},
    {"company": "Carta", "domain": "carta.com", "url": "https://boards.greenhouse.io/carta", "industry": "Finance & Banking"},
    {"company": "Marqeta", "domain": "marqeta.com", "url": "https://boards.greenhouse.io/marqeta", "industry": "Finance & Banking"},
    {"company": "Toast", "domain": "toasttab.com", "url": "https://boards.greenhouse.io/toast", "industry": "Finance & Banking"},
    {"company": "Addepar", "domain": "addepar.com", "url": "https://boards.greenhouse.io/addepar", "industry": "Finance & Banking"},
    {"company": "Block", "domain": "block.xyz", "url": "https://jobs.lever.co/block", "industry": "Finance & Banking"},
    {"company": "Plaid", "domain": "plaid.com", "url": "https://jobs.lever.co/plaid", "industry": "Finance & Banking"},
    {"company": "Mercury", "domain": "mercury.com", "url": "https://jobs.lever.co/mercury", "industry": "Finance & Banking"},
    {"company": "Ramp", "domain": "ramp.com", "url": "https://jobs.ashbyhq.com/ramp", "industry": "Finance & Banking"},
    {"company": "Modern Treasury", "domain": "moderntreasury.com", "url": "https://jobs.ashbyhq.com/moderntreasury", "industry": "Finance & Banking"},

    # ── 6. Aerospace & Defense ──────────────────────────────────────────────
    {"company": "Dice DefenseTech", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=defense+software", "industry": "Aerospace & Defense"},
    {"company": "Dice Aerospace Engineer", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=aerospace+software", "industry": "Aerospace & Defense"},
    {"company": "Dice Avionics Developer", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=avionics", "industry": "Aerospace & Defense"},
    {"company": "SpaceX", "domain": "spacex.com", "url": "https://boards.greenhouse.io/spacex", "industry": "Aerospace & Defense"},
    {"company": "Rocket Lab", "domain": "rocketlabusa.com", "url": "https://boards.greenhouse.io/rocketlab", "industry": "Aerospace & Defense"},
    {"company": "Applied Intuition", "domain": "appliedintuition.com", "url": "https://boards.greenhouse.io/appliedintuition", "industry": "Aerospace & Defense"},
    {"company": "Two Six Technologies", "domain": "twosixtech.com", "url": "https://boards.greenhouse.io/twosixtechnologies", "industry": "Aerospace & Defense"},
    {"company": "BlackSky Technology", "domain": "blacksky.com", "url": "https://boards.greenhouse.io/blacksky", "industry": "Aerospace & Defense"},
    {"company": "Planet Labs", "domain": "planet.com", "url": "https://boards.greenhouse.io/planetlabs", "industry": "Aerospace & Defense"},
    {"company": "Archer Aviation", "domain": "archer.com", "url": "https://boards.greenhouse.io/archer", "industry": "Aerospace & Defense"},
    {"company": "HawkEye 360", "domain": "he360.com", "url": "https://boards.greenhouse.io/hawkeye360", "industry": "Aerospace & Defense"},
    {"company": "Anduril Industries", "domain": "anduril.com", "url": "https://jobs.lever.co/anduril", "industry": "Aerospace & Defense"},
    {"company": "Shield AI", "domain": "shield.ai", "url": "https://jobs.lever.co/shieldai", "industry": "Aerospace & Defense"},
    {"company": "Relativity Space", "domain": "relativityspace.com", "url": "https://jobs.lever.co/relativityspace", "industry": "Aerospace & Defense"},
    {"company": "Skydio", "domain": "skydio.com", "url": "https://jobs.lever.co/skydio", "industry": "Aerospace & Defense"},
    {"company": "Apex Space", "domain": "apextechnology.com", "url": "https://jobs.ashbyhq.com/apex", "industry": "Aerospace & Defense"},
    {"company": "Impulse Space", "domain": "impulsespace.com", "url": "https://jobs.ashbyhq.com/impulsespace", "industry": "Aerospace & Defense"},
    {"company": "Hadrian", "domain": "hadrian.co", "url": "https://jobs.ashbyhq.com/hadrian", "industry": "Aerospace & Defense"},

    # ── 7. IT, Software & Technology ─────────────────────────────────────────
    {"company": "Dice Software Engineer", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=software+engineer", "industry": "IT, Software & Technology"},
    {"company": "Dice Cloud & DevOps", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=cloud+devops", "industry": "IT, Software & Technology"},
    {"company": "Dice Python Developer", "domain": "dice.com", "url": "https://www.dice.com/jobs?q=python+engineer", "industry": "IT, Software & Technology"},
    {"company": "Stripe", "domain": "stripe.com", "url": "https://boards.greenhouse.io/stripe", "industry": "IT, Software & Technology"},
    {"company": "Databricks", "domain": "databricks.com", "url": "https://boards.greenhouse.io/databricks", "industry": "IT, Software & Technology"},
    {"company": "Figma", "domain": "figma.com", "url": "https://boards.greenhouse.io/figma", "industry": "IT, Software & Technology"},
    {"company": "Cloudflare", "domain": "cloudflare.com", "url": "https://boards.greenhouse.io/cloudflare", "industry": "IT, Software & Technology"},
    {"company": "Datadog", "domain": "datadoghq.com", "url": "https://boards.greenhouse.io/datadog", "industry": "IT, Software & Technology"},
    {"company": "Snowflake", "domain": "snowflake.com", "url": "https://boards.greenhouse.io/snowflake", "industry": "IT, Software & Technology"},
    {"company": "Reddit", "domain": "reddit.com", "url": "https://boards.greenhouse.io/reddit", "industry": "IT, Software & Technology"},
    {"company": "OpenAI", "domain": "openai.com", "url": "https://jobs.ashbyhq.com/openai", "industry": "IT, Software & Technology"},
    {"company": "Linear", "domain": "linear.app", "url": "https://jobs.ashbyhq.com/linear", "industry": "IT, Software & Technology"},
    {"company": "Notion", "domain": "notion.so", "url": "https://jobs.ashbyhq.com/notion", "industry": "IT, Software & Technology"},
    {"company": "Wiz", "domain": "wiz.io", "url": "https://jobs.ashbyhq.com/wiz", "industry": "IT, Software & Technology"},
    {"company": "Anthropic", "domain": "anthropic.com", "url": "https://jobs.ashbyhq.com/anthropic", "industry": "IT, Software & Technology"},
    {"company": "Atlassian", "domain": "atlassian.com", "url": "https://jobs.lever.co/atlassian", "industry": "IT, Software & Technology"},
    {"company": "Palantir", "domain": "palantir.com", "url": "https://jobs.lever.co/palantir", "industry": "IT, Software & Technology"},
    {"company": "Dropbox", "domain": "dropbox.com", "url": "https://jobs.lever.co/dropbox", "industry": "IT, Software & Technology"},
]


def query_industry_job_counts() -> Dict[str, int]:
    """Queries current job counts per industry in PostgreSQL."""
    counts = {ind: 0 for ind in TARGET_INDUSTRIES}
    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT COALESCE(c.industry, 'Unknown') as industry, COUNT(j.id) as cnt
            FROM companies c
            JOIN jobs j ON j.company_id = c.id
            GROUP BY c.industry;
        """)
        for r in cur.fetchall():
            ind = r["industry"]
            if ind in counts:
                counts[ind] = r["cnt"]
    return counts


class MultiIndustryWorkerCoordinator:
    """Coordinates 10 parallel workers scraping across the 7 industries."""

    def __init__(self, max_workers: int = 10, max_jobs_per_target: int = 150):
        self.max_workers = max_workers
        self.max_jobs_per_target = max_jobs_per_target
        self.repo = JobRepository()
        self.scraper = PlaywrightCareerScraper(headless=True)
        self.lock = threading.Lock()
        
        # Thread-safe stats
        self.industry_stats = {ind: {"targets_processed": 0, "jobs_found": 0, "new_jobs": 0} for ind in TARGET_INDUSTRIES}
        self.total_new_jobs = 0
        self.total_jobs_found = 0

    def process_target(self, worker_id: int, target: Dict[str, Any]) -> Dict[str, Any]:
        """Worker task execution function for a single target."""
        comp_name = target["company"]
        domain = target["domain"]
        url = target["url"]
        industry = target["industry"]

        prefix = f"[bold cyan][Worker-{worker_id:02d}][/bold cyan] [[yellow]{industry[:14]}[/yellow]]"

        # 1. Fetch raw jobs using high-speed ATS API router
        try:
            raw_jobs = self.scraper.scrape_career_page(url, max_jobs=self.max_jobs_per_target)
        except Exception as exc:
            console.print(f"{prefix} [dim red]Error fetching {comp_name}: {exc}[/dim red]")
            return {"industry": industry, "found": 0, "new": 0}

        if not raw_jobs:
            return {"industry": industry, "found": 0, "new": 0}

        # 2. Get or create company & source
        company = self.repo.get_or_create_company(
            name=comp_name, domain=domain, career_url=url, industry=industry
        )
        source = self.repo.get_or_create_job_source(
            company_id=company.id, source_url=url, scraper_type="playwright"
        )

        # 3. Filter tech roles, validate USA location, classify employment type
        saved_count = 0
        duplicate_count = 0

        # Pre-check existing jobs in DB to skip redundant work
        external_ids = [j.external_job_id for j in raw_jobs if j.external_job_id]
        existing_map = self.repo.check_existing_jobs(company.id, external_ids)

        for raw in raw_jobs:
            if not raw.title or not raw.job_url:
                continue

            # Anti-404 error filtering
            t_lower = raw.title.lower()
            if any(err in t_lower for err in [
                "page you are looking for doesn't exist",
                "page you are looking for does not exist",
                "page not found", "404 not found", "job is no longer available"
            ]):
                continue

            # Check tech relevance
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

            # Classification: FULL_TIME or CONTRACT only (strict user requirement)
            emp_type = JDAnalyzer.deduce_employment_type_from_jd(
                title=raw.title,
                description=raw.raw_description or "",
                metadata=raw.metadata,
            )
            if emp_type not in ("FULL_TIME", "CONTRACT"):
                continue

            # Resolve hiring company if provided by platform metadata
            target_company_id = company.id
            if raw.metadata and raw.metadata.get("company_name"):
                hiring_name = raw.metadata["company_name"].strip()
                if hiring_name and hiring_name.lower() not in (comp_name.lower(), "dice employer", "dice company"):
                    try:
                        h_comp = self.repo.get_or_create_company(name=hiring_name, industry=industry)
                        target_company_id = h_comp.id
                    except Exception:
                        target_company_id = company.id

            # Content hash & display location
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
                external_job_id=raw.external_job_id or BaseScraper.generate_job_id(company.id, raw.title, raw.job_url),
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
                job_url=raw.job_url.strip(),
                application_url=raw.application_url or raw.job_url,
                content_hash=content_hash,
                is_tech=True,
                tech_role=tech_kw[0] if tech_kw else "software",
                tech_keywords=tech_kw,
                status="OPEN",
            )

            # Insert / update in DB
            result = self.repo.upsert_job(job)
            if result == "NEW":
                saved_count += 1
            elif result == "DUPLICATE":
                duplicate_count += 1

        # Thread-safe stat update
        with self.lock:
            self.industry_stats[industry]["targets_processed"] += 1
            self.industry_stats[industry]["jobs_found"] += len(raw_jobs)
            self.industry_stats[industry]["new_jobs"] += saved_count
            self.total_new_jobs += saved_count
            self.total_jobs_found += len(raw_jobs)

        if saved_count > 0:
            console.print(
                f"{prefix} [bold green]✔ {comp_name}[/bold green]: "
                f"Found [bold cyan]{len(raw_jobs)}[/bold cyan] jobs -> "
                f"Stored [bold green]+{saved_count} NEW[/bold green] in DB! "
                f"[dim]({duplicate_count} seen)[/dim]"
            )
        else:
            console.print(
                f"{prefix} [dim]{comp_name}: Found {len(raw_jobs)} ({duplicate_count} already up to date)[/dim]"
            )

        return {"industry": industry, "found": len(raw_jobs), "new": saved_count}

    def run(self, selected_industry: Optional[str] = None):
        """Launches the 10 concurrent workers across all targets."""
        # Query before stats
        before_counts = query_industry_job_counts()

        console.print("\n[bold cyan]╔══════════════════════════════════════════════════════════════════════════╗[/bold cyan]")
        console.print(f"[bold cyan]║  AutoJobApply – 10 Parallel Workers Multi-Industry Scraper Pipeline      ║[/bold cyan]")
        console.print("[bold cyan]╚══════════════════════════════════════════════════════════════════════════╝[/bold cyan]")
        console.print(f"Workers: [bold green]{self.max_workers}[/bold green] | Mode: [bold magenta]Concurrent Multi-Industry Ingestion[/bold magenta]\n")

        # Display initial baseline table
        init_table = Table(title="Baseline Job Count Across All 7 Industries", border_style="cyan")
        init_table.add_column("Target Industry", style="bold yellow")
        init_table.add_column("Jobs in PostgreSQL", justify="right", style="bold green")
        total_before = 0
        for ind in TARGET_INDUSTRIES:
            cnt = before_counts.get(ind, 0)
            init_table.add_row(ind, f"{cnt:,}")
            total_before += cnt
        init_table.add_row("[bold cyan]TOTAL ALL 7 INDUSTRIES[/bold cyan]", f"[bold green]{total_before:,}[/bold green]")
        console.print(init_table)
        console.print("\n[bold cyan]▶ Dispatching tasks to 10 parallel workers...[/bold cyan]\n")

        # Filter target list if requested
        targets = MULTI_INDUSTRY_TARGETS
        if selected_industry and selected_industry.upper() != "ALL":
            targets = [t for t in targets if t["industry"].lower() == selected_industry.lower()]

        # Interleave targets across industries so workers process diverse industries in parallel
        # Group by industry first
        by_ind: Dict[str, List[Dict[str, Any]]] = {ind: [] for ind in TARGET_INDUSTRIES}
        for t in targets:
            by_ind[t["industry"]].append(t)

        # Round-robin interleave
        interleaved_targets = []
        max_len = max(len(v) for v in by_ind.values()) if by_ind else 0
        for i in range(max_len):
            for ind in TARGET_INDUSTRIES:
                if i < len(by_ind[ind]):
                    interleaved_targets.append(by_ind[ind][i])

        start_time = time.time()

        # Execute across 10 parallel workers
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for idx, target in enumerate(interleaved_targets):
                worker_id = (idx % self.max_workers) + 1
                futures.append(executor.submit(self.process_target, worker_id, target))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Worker task error: {exc}")

        elapsed = time.time() - start_time

        # Query after stats
        after_counts = query_industry_job_counts()

        # Display final results summary
        console.print("\n[bold green]══════════════════════════════════════════════════════════════════════════[/bold green]")
        console.print(f"[bold green]✔ Parallel Multi-Industry Scrape Complete in {elapsed:.1f}s![/bold green]")
        console.print("[bold green]══════════════════════════════════════════════════════════════════════════[/bold green]\n")

        summary_table = Table(title="Final Job Count Comparison Across All 7 Industries", border_style="green")
        summary_table.add_column("Industry", style="bold yellow")
        summary_table.add_column("Before", justify="right", style="dim")
        summary_table.add_column("New Added", justify="right", style="bold green")
        summary_table.add_column("Current Total", justify="right", style="bold cyan")

        total_after = 0
        total_delta = 0
        for ind in TARGET_INDUSTRIES:
            b = before_counts.get(ind, 0)
            a = after_counts.get(ind, 0)
            delta = a - b
            total_after += a
            total_delta += delta
            delta_str = f"+{delta:,}" if delta > 0 else "0"
            summary_table.add_row(ind, f"{b:,}", delta_str, f"{a:,}")

        summary_table.add_row(
            "[bold cyan]TOTALS[/bold cyan]",
            f"[dim]{total_before:,}[/dim]",
            f"[bold green]+{total_delta:,}[/bold green]",
            f"[bold cyan]{total_after:,}[/bold cyan]",
        )
        console.print(summary_table)

        # Refresh COMPANIES.md and data/companies.json
        console.print("\n[bold cyan]Syncing updated catalog to COMPANIES.md and data/companies.json...[/bold cyan]")
        try:
            export_companies()
            console.print("[bold green]✔ Catalog synchronized successfully![/bold green]")
        except Exception as exc:
            console.print(f"[dim red]Error exporting catalog: {exc}[/dim red]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="10-Worker Parallel Multi-Industry Scraper")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers (default: 10)")
    parser.add_argument("--industry", type=str, default="ALL", help="Target specific industry or ALL")
    parser.add_argument("--max-jobs", type=int, default=150, help="Max jobs per target (default: 150)")
    args = parser.parse_args()

    coordinator = MultiIndustryWorkerCoordinator(
        max_workers=args.workers,
        max_jobs_per_target=args.max_jobs,
    )
    coordinator.run(selected_industry=args.industry)
