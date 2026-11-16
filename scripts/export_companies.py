"""
Company Catalog Exporter.
Exports the full company list from PostgreSQL into:
1. `data/companies.json` (machine-readable)
2. `COMPANIES.md` (human-readable Markdown catalog)
"""

import sys
import json
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scraper_agent.db.connection import get_dict_cursor

def export_companies():
    data_dir = ROOT_DIR / "data"
    data_dir.mkdir(exist_ok=True)
    json_path = data_dir / "companies.json"
    md_path = ROOT_DIR / "COMPANIES.md"

    with get_dict_cursor() as cur:
        # Fetch companies with their active job sources and job counts
        cur.execute("""
            SELECT 
                c.id,
                c.name,
                c.domain,
                c.career_url,
                COALESCE(c.industry, 'Unclassified') as industry,
                c.active,
                COUNT(j.id) as active_jobs_count,
                MAX(s.scraper_type) as primary_scraper_type,
                MAX(s.source_url) as primary_source_url
            FROM companies c
            LEFT JOIN jobs j ON j.company_id = c.id
            LEFT JOIN job_sources s ON s.company_id = c.id
            GROUP BY c.id, c.name, c.domain, c.career_url, c.industry, c.active
            ORDER BY c.industry ASC, active_jobs_count DESC, c.name ASC;
        """)
        companies = cur.fetchall()

    print(f"Exporting {len(companies)} companies from PostgreSQL...")

    from scraper_agent.ats_platforms import classify_source_url

    # Classify ATS platform using the comprehensive 21 ATS & 14 Job Portals registry
    def infer_ats(career_url, source_url):
        for candidate in (career_url, source_url):
            if candidate:
                cfg = classify_source_url(candidate)
                if cfg:
                    return cfg.name
        return "Direct Career Portal"

    export_list = []
    by_industry = {}

    for c in companies:
        ats = infer_ats(c["career_url"], c["primary_source_url"])
        comp_obj = {
            "id": c["id"],
            "name": c["name"],
            "domain": c["domain"],
            "career_url": c["career_url"] or c["primary_source_url"],
            "industry": c["industry"],
            "ats_platform": ats,
            "active": c["active"],
            "active_jobs_count": c["active_jobs_count"],
        }
        export_list.append(comp_obj)

        ind = c["industry"]
        by_industry.setdefault(ind, []).append(comp_obj)

    # 1. Write JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(export_list, f, indent=2, default=str)
    print(f"Saved machine-readable JSON to {json_path}")

    # 2. Write Markdown
    md_lines = [
        "# Tracked Companies Directory\n",
        f"This document catalogs all **{len(companies):,} companies** tracked in the database, ",
        "organized by target industry, career URL, ATS platform, and current active jobs inventory in PostgreSQL.\n\n",
        "## Summary by Industry\n\n",
        "| Industry | Total Companies | Companies with Active Jobs | Total Jobs in DB |\n",
        "| :--- | :--- | :--- | :--- |\n",
    ]

    for ind, c_list in sorted(by_industry.items(), key=lambda x: -len(x[1])):
        total_comps = len(c_list)
        comps_with_jobs = sum(1 for x in c_list if x["active_jobs_count"] > 0)
        jobs_in_ind = sum(x["active_jobs_count"] for x in c_list)
        md_lines.append(f"| {ind} | {total_comps:,} | {comps_with_jobs:,} | {jobs_in_ind:,} |\n")

    md_lines.append("\n---\n\n")

    # Detailed tables for top companies per industry
    for ind, c_list in sorted(by_industry.items()):
        md_lines.append(f"## {ind} ({len(c_list)} Companies)\n\n")
        md_lines.append("| Company Name | Domain | ATS Platform | Active Jobs in DB | Career URL |\n")
        md_lines.append("| :--- | :--- | :--- | :--- | :--- |\n")
        
        # Sort by jobs count descending, then name
        sorted_comps = sorted(c_list, key=lambda x: (-x["active_jobs_count"], x["name"]))
        # Output top entries (or all)
        for c in sorted_comps:
            name = c["name"]
            domain = c["domain"] or "-"
            ats = c["ats_platform"]
            cnt = f"**{c['active_jobs_count']:,}**" if c["active_jobs_count"] > 0 else "0"
            curl = c["career_url"] or "-"
            link = f"[Careers]({curl})" if curl.startswith("http") else "-"
            md_lines.append(f"| {name} | `{domain}` | {ats} | {cnt} | {link} |\n")
        md_lines.append("\n")

    with open(md_path, "w", encoding="utf-8") as f:
        f.writelines(md_lines)
    print(f"Saved human-readable Markdown to {md_path}")

if __name__ == "__main__":
    export_companies()
