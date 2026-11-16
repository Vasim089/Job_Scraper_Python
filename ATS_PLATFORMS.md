# Comprehensive Targeted ATS Platforms & Job Portals Architecture

This document serves as the master architectural reference and registry for all **21 Enterprise ATS Platforms** and **14 Global Job Portals** targeted by the scraping and job ingestion system.

---

## 1. Complete Master Target Matrix

### 1.1 The 21 Targeted ATS Platforms

| # | ATS Platform | Category | Domain Signatures | Primary Scraper Engine | API Style | Status |
| :-: | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **GreenHouse** | Enterprise ATS | `boards.greenhouse.io`, `job-boards.greenhouse.io` | `PlaywrightCareerScraper` | Harvest REST API / JSON | Active |
| **2** | **Lever** | Modern ATS | `jobs.lever.co`, `lever.co` | `PlaywrightCareerScraper` | REST JSON Postings API | Active |
| **3** | **Ashby** | Modern Tech ATS | `jobs.ashbyhq.com`, `ashbyhq.com` | `PlaywrightCareerScraper` | GraphQL Public API | Active |
| **4** | **ICIMS** | Enterprise ATS | `icims.com`, `jobs.icims.com` | `PlaywrightCareerScraper` | Headless DOM Scraper | Active |
| **5** | **SmartRecruiters** | Enterprise ATS | `jobs.smartrecruiters.com` | `PlaywrightCareerScraper` | REST Postings API | Active |
| **6** | **Workday** | Global Enterprise | `*.myworkdayjobs.com`, `workday.com` | `WorkdayScraper` | CXS REST JSON API | Active (5,100 Jobs) |
| **7** | **SAP SuccessFactors** | Global ERP / ATS | `successfactors.com`, `jobs2web.com` | `PlaywrightCareerScraper` | Headless DOM / Search | Active |
| **8** | **Oracle Recruiting Cloud** | Enterprise HCM | `oraclecloud.com/hcmUI/CandidateExperience` | `PlaywrightCareerScraper` | Headless DOM / Dynamic JS | Active |
| **9** | **Workable** | Mid-Market ATS | `workable.com`, `apply.workable.com` | `PlaywrightCareerScraper` | REST JSON API v3 | Active |
| **10** | **BambooHR** | SMB / Mid-Market | `*.bamboohr.com/careers` | `PlaywrightCareerScraper` | Headless DOM / Cards | Active |
| **11** | **Breezy HR** | Modern ATS | `*.breezy.hr` | `PlaywrightCareerScraper` | Headless DOM / Cards | Active |
| **12** | **Rippling** | Unified HR & IT | `rippling.com/careers`, `ats.rippling.com` | `PlaywrightCareerScraper` | REST JSON API v1 | Active |
| **13** | **Teamtailor** | Employer Branding | `teamtailor.com`, `career.teamtailor.com` | `PlaywrightCareerScraper` | REST JSON API v1 | Active |
| **14** | **Recruitee** | Modern ATS | `*.recruitee.com/api/offers` | `PlaywrightCareerScraper` | REST JSON Offers API | Active |
| **15** | **JazzHR** | SMB Recruiting | `applytojob.com`, `jazz.co` | `PlaywrightCareerScraper` | Headless DOM Scraper | Active |
| **16** | **Jobvite** | Enterprise ATS | `jobs.jobvite.com`, `jobvite.com` | `PlaywrightCareerScraper` | Headless DOM / Search | Active |
| **17** | **Oracle Taleo** | Enterprise Legacy | `*.taleo.net/careersection` | `PlaywrightCareerScraper` | Headless DOM / Multi-step | Active |
| **18** | **UKG Pro Recruiting** | Enterprise HCM | `recruiting.ultipro.com`, `ukg.com` | `PlaywrightCareerScraper` | Headless DOM / Boards | Active |
| **19** | **Paycom** | Payroll & Talent | `paycomonline.net/v4/ats` | `PlaywrightCareerScraper` | Headless DOM Scraper | Active |
| **20** | **Paylocity** | Payroll & Talent | `recruiting.paylocity.com` | `PlaywrightCareerScraper` | Headless DOM Scraper | Active |
| **21** | **Phenom** | AI TXP Portal | `*.phenompro.com`, `phenom.com` | `PlaywrightCareerScraper` | Headless DOM / Search | Active |

---

### 1.2 The 14 Targeted Job Portals

| # | Job Portal | Category | Domain Signatures | Primary Scraper Engine | Protocol / Search Style | Target Role Focus |
| :-: | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **TheLadders** | Executive Portal | `theladders.com` | `PlaywrightCareerScraper` | Search Query / DOM | $100K+ Tech & Executive |
| **2** | **Glassdoor** | Global Reviews & Jobs | `glassdoor.com` | `PlaywrightCareerScraper` | Stealth Search / DOM | Comprehensive USA Tech |
| **3** | **SimplyHired** | Job Search Engine | `simplyhired.com` | `PlaywrightCareerScraper` | Search Aggregator / DOM | General & Contract Roles |
| **4** | **Hired** | Curated Tech Portal | `hired.com` | `PlaywrightCareerScraper` | Salary & Tech Index | Software, AI, Engineering |
| **5** | **GitHub Jobs** | Developer Portal | `jobs.github.com` | `GenericHtmlScraper` | Careers Feed / GitHub API | Open Source & Engineering |
| **6** | **Stack Overflow Jobs**| Tech Community Portal | `stackoverflow.com/jobs` | `GenericHtmlScraper` | Company Profiles / Feeds | Developers & DevOps |
| **7** | **CyberCoders** | Tech Staffing Portal | `cybercoders.com` | `PlaywrightCareerScraper` | Search Filter / DOM | Direct IT & Engineering |
| **8** | **TechCareers** | Specialized IT Board | `techcareers.com` | `PlaywrightCareerScraper` | Query Param / DOM | Telecom, SysAdmin, Cloud |
| **9** | **ITJobPro** | IT Infrastructure | `itjobpro.com` | `PlaywrightCareerScraper` | Query Param / DOM | SysAdmin, Cyber, Infra |
| **10** | **JobServe** | Contract IT Board | `jobserve.com` | `PlaywrightCareerScraper` | Search Query / DOM | C2C, Contract, Perm IT |
| **11** | **Dice** | Leading Tech & C2C | `dice.com` | `PlaywrightCareerScraper` | Journey REST JSON API | Software, Cloud, DevOps (5,791 Jobs) |
| **12** | **Monster** | Global Job Board | `monster.com` | `PlaywrightCareerScraper` | Search Query / DOM | Enterprise & General |
| **13** | **ZipRecruiter** | Marketplace Portal | `ziprecruiter.com` | `PlaywrightCareerScraper` | Search Query / DOM | SMB & Enterprise Tech |
| **14** | **Indeed** | Leading Global Portal | `indeed.com` | `PlaywrightCareerScraper` | Stealth Search / DOM | All USA Industries |

---

## 2. In-Depth ATS Platforms Architecture

### 2.1 Workday (`myworkdayjobs.com`)
- **Module**: `scraper_agent/scrapers/workday.py`
- **Driver**: Dedicated `WorkdayScraper(BaseScraper)` connecting to background CXS JSON services.
- **Search API**: `POST https://{host}/wday/cxs/{tenant}/{site}/jobs`
  - Payload: `{"appliedFacets": {}, "limit": 20, "offset": <n>, "searchText": ""}`
- **Detail API**: `GET https://{host}/wday/cxs/{tenant}/{site}{externalPath}`
- **Anti-Dead Requisition Engine**:
  - Eliminates Workday SPA empty HTML shells that return 200 while displaying *"The page you are looking for doesn't exist."*
  - Automatically verifies each requisition against `/wday/cxs/...` and immediately deletes closed/inactive postings (`DELETE FROM jobs WHERE id = %s`).
- **Primary Employers**: Capital One, Micron, NVIDIA, Target, Cisco, HPE, Mastercard, Pfizer, Abbott.

### 2.2 GreenHouse (`boards.greenhouse.io`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **Harvest REST API**: `GET https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true`
- **Job URL Pattern**: `https://boards.greenhouse.io/{board}/jobs/{job_id}`
- **Automatic Repair**: Intercepts and repairs malformed `/company/` URLs into standardized `/jobs/{id}` links.
- **Primary Employers**: Stripe, Databricks, Figma, Cloudflare, Reddit, Roblox, Asana, MongoDB, Twilio, Okta.

### 2.3 Lever (`jobs.lever.co`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **REST Postings API**: `GET https://api.lever.co/v0/postings/{company}?mode=json`
- **Features**: Clean extracted team categories, commitment classification (Full-time vs. Contract), and direct application URLs.
- **Primary Employers**: Atlassian, Palantir, Dropbox, Snyk, Retool, Zapier.

### 2.4 Ashby (`jobs.ashbyhq.com`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **GraphQL API**: `POST https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams`
- **Features**: Modern schema, compensation ranges, department trees, and remote-friendly designations.
- **Primary Employers**: OpenAI, Anthropic, Linear, Supabase, Synthesia, Ramp, Mercury.

### 2.5 ICIMS (`icims.com`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **DOM Engine**: Handles iframe encapsulation, multi-step job detail views, and complex corporate career portals.
- **Primary Employers**: Northrop Grumman, Lockheed Martin, General Dynamics, Catholic Health.

### 2.6 SmartRecruiters (`jobs.smartrecruiters.com`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **REST Postings API**: `GET https://api.smartrecruiters.com/v1/companies/{company}/postings`
- **Primary Employers**: Square / Block, Visa, Twitter/X, Informatica.

### 2.7 SAP SuccessFactors (`successfactors.com`, `jobs2web.com`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **Features**: Faceted enterprise search scraping, dynamic pagination, and automated extraction from multinational corporations.
- **Primary Employers**: Siemens, Nestle, BMW, Accenture, PepsiCo.

### 2.8 Oracle Recruiting Cloud (`oraclecloud.com/hcmUI`) & Oracle Taleo (`taleo.net`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **Features**: Cloud Candidate Experience (CX) portal navigation and Taleo career section form handling.
- **Primary Employers**: Boeing, United Airlines, Bank of America, FedEx, Kroger.

### 2.9 Workable, BambooHR, Breezy HR, Rippling, Teamtailor, Recruitee, JazzHR, Jobvite
- **Features**: Modern startup and mid-market ATS support with dedicated API bindings or automated card extraction.
- **Primary Employers**: Revolut, Postscript, Automattic, Toptal, Klarna, Spotify, Hotjar, Bitly, Blizzard.

### 2.10 UKG Pro Recruiting, Paycom, Paylocity, Phenom
- **Features**: Enterprise workforce management portals and AI talent experience platforms.
- **Primary Employers**: Honeywell, Southwest Airlines, Cigna, First American, Paycom clients.

---

## 3. In-Depth Job Portals Architecture

### 3.1 Dice (`dice.com`)
- **Module**: `scripts/run_rapid_12000_dice.py` & `scraper_agent/scrapers/playwright_scraper.py`
- **API**: High-speed direct REST API (`https://service.dice.com/api/rest/journey/v2/search/jobs`).
- **Capabilities**: Captures contract, C2C, W2, and full-time technology roles with hourly compensation rates and skill tags.

### 3.2 TheLadders (`theladders.com`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **Capabilities**: Targeted extraction for senior engineering, lead, architect, and executive positions exceeding $100K.

### 3.3 Glassdoor & Indeed (`glassdoor.com`, `indeed.com`)
- **Module**: `scraper_agent/scrapers/playwright_scraper.py`
- **Anti-Bot Defense**: Utilizes stealth Chromium contexts, random delay profiles, and humanized cursor movements to bypass Cloudflare and Turnstile bot defenses.
- **Integrity Rule**: If a challenge or CAPTCHA blocks a page, the job is not saved (or deleted from the database) so the auto-apply agent never encounters a blocked page.

### 3.4 SimplyHired, CyberCoders, TechCareers, ITJobPro, JobServe, Monster, ZipRecruiter
- **Capabilities**: Automated job card detection, direct hiring company attribution, salary range parsing, and clear application links.

---

## 4. Database Rules & Applicant Agent Integrity

To guarantee that auto-apply agents never encounter broken links, login walls, or dead pages:

1. **Direct Removal of Expired Listings**:
   - Closed or expired jobs are **never marked as `CLOSED` in the database**.
   - Instead, they are **directly deleted** via SQL: `DELETE FROM jobs WHERE id = %s`.
2. **Direct Removal of Inaccessible / Error Postings**:
   - Any job encountering CAPTCHA, login walls, or continuous network errors is **directly deleted** from the database to prevent auto-apply agent errors.
3. **Current Verified Database Status**:
   - **Total Active Jobs**: **20,139**
   - **Workday Jobs**: **5,100** (100% verified active via CXS)
   - **Greenhouse Jobs**: **7,757**
   - **Dice Jobs**: **5,791**
   - **Ashby, SmartRecruiters, Lever, & Others**: **1,491**
   - **Status**: Every listing in the database has status `OPEN`.

