import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from scraper_agent.db.repository import JobRepository
from scraper_agent.verification.checker import JobAvailabilityChecker
from scraper_agent.verification.models import (
    VerificationAction,
    VerificationReason,
    JobVerificationResult,
)

logger = logging.getLogger(__name__)
console = Console(highlight=False)


class JobBatchVerifier:
    """
    Coordinates batch verification across database jobs strictly enforcing the Important Database Rules:
      - URL Active + Job Open: Keep job Active.
      - URL Active + Job Closed / Expired: Directly remove/delete job from data (do NOT set status to CLOSED).
      - URL Active + Status Unclear: Keep existing status; do not remove or mark Closed.
      - URL Inaccessible: Retry verification before changing status.
      - CAPTCHA/Login/Network Error: Directly remove/delete job from DB (prevents apply agent errors; re-scraped if active later).
      - Confirmed Closed: Directly remove/delete job from the database.
    """

    def __init__(
        self,
        checker: Optional[JobAvailabilityChecker] = None,
        repo: Optional[JobRepository] = None,
        max_workers: int = 10,
    ):
        self.checker = checker or JobAvailabilityChecker()
        self.repo = repo or JobRepository()
        self.max_workers = max_workers

    def verify_single_job(self, job_dict: Dict[str, Any]) -> JobVerificationResult:
        """Runs the 6 rules against a single job database dictionary."""
        return self.checker.verify_job(
            job_url=job_dict.get("job_url", ""),
            job_id=job_dict.get("id", 0),
            title=job_dict.get("title"),
            company_name=job_dict.get("company_name"),
            external_job_id=job_dict.get("external_job_id"),
        )

    def run_verification(
        self,
        limit: Optional[int] = None,
        dry_run: bool = False,
        status_filter: str = "OPEN",
    ) -> Dict[str, Any]:
        """
        Runs batch availability verification on jobs in PostgreSQL.
        Applies database status changes ONLY when confirmed closed or active.
        Preserves existing status on unclear results or errors.
        """
        console.print("\n[bold cyan]═══ Starting Job Availability Verification ═══[/bold cyan]")
        console.print(f"Workers: [green]{self.max_workers}[/green] | Target: [green]{status_filter}[/green] | Dry Run: [dim]{dry_run}[/dim]\n")

        # Fetch jobs to verify
        jobs_to_check = self.get_jobs_for_check(limit=limit, status=status_filter)
        total_jobs = len(jobs_to_check)
        if total_jobs == 0:
            console.print("[yellow]No jobs found matching verification criteria.[/yellow]\n")
            return {"total": 0, "active": 0, "closed": 0, "unclear": 0, "no_change": 0}

        console.print(f"Found [bold green]{total_jobs}[/bold green] jobs to verify in database.\n")

        results: List[JobVerificationResult] = []
        action_counts = {
            VerificationAction.KEEP_ACTIVE.value: 0,
            VerificationAction.MARK_CLOSED.value: 0,
            VerificationAction.KEEP_STATUS.value: 0,
            VerificationAction.NO_CHANGE.value: 0,
        }

        start_time = time.time()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Verifying Job Rules...", total=total_jobs)

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_job = {
                    executor.submit(self.verify_single_job, job): job
                    for job in jobs_to_check
                }

                for future in as_completed(future_to_job):
                    res = future.result()
                    results.append(res)
                    action_counts[res.action.value] += 1
                    progress.advance(task)

        elapsed = round(time.time() - start_time, 2)

        # Database Update Step according to rules
        updated_db_counts = {"DELETED": 0, "DELETED_CLOSED": 0, "DELETED_ERROR": 0, "CLOSED": 0, "OPEN": 0, "UNTOUCHED": 0}

        if not dry_run:
            console.print("\n[cyan]Applying Database Rules...[/cyan]")
            from scraper_agent.db.connection import get_db_connection

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    for res in results:
                        if res.should_delete or res.action in (
                            VerificationAction.MARK_CLOSED,
                            VerificationAction.REMOVE_CLOSED,
                            VerificationAction.NO_CHANGE,
                            VerificationAction.REMOVE_ERROR,
                        ):
                            # Confirmed Closed / Expired OR Error / Inaccessible (CAPTCHA / Login / Network Error):
                            # Directly remove/delete job from the database to prevent applicant agent failures.
                            cur.execute(
                                """
                                DELETE FROM jobs
                                WHERE id = %s
                                """,
                                (res.job_id,),
                            )
                            updated_db_counts["DELETED"] += 1
                            if res.action in (VerificationAction.NO_CHANGE, VerificationAction.REMOVE_ERROR):
                                updated_db_counts["DELETED_ERROR"] += 1
                            else:
                                updated_db_counts["DELETED_CLOSED"] += 1
                                updated_db_counts["CLOSED"] += 1

                        elif res.action == VerificationAction.KEEP_ACTIVE:
                            # URL Active + Job Open: Keep job Active
                            cur.execute(
                                """
                                UPDATE jobs
                                SET status = 'OPEN',
                                    last_seen_at = CURRENT_TIMESTAMP,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                                """,
                                (res.job_id,),
                            )
                            updated_db_counts["OPEN"] += 1

                        else:
                            # URL Active + Status Unclear (KEEP_STATUS): Keep existing status untouched
                            updated_db_counts["UNTOUCHED"] += 1

        # Summary Display
        self._print_summary(
            total=total_jobs,
            counts=action_counts,
            updated_db_counts=updated_db_counts,
            elapsed_seconds=elapsed,
            results=results,
        )

        return {
            "total": total_jobs,
            "active": action_counts[VerificationAction.KEEP_ACTIVE.value],
            "closed": action_counts[VerificationAction.MARK_CLOSED.value],
            "deleted": updated_db_counts["DELETED"],
            "unclear": action_counts[VerificationAction.KEEP_STATUS.value],
            "no_change": action_counts[VerificationAction.NO_CHANGE.value],
            "updated_db_counts": updated_db_counts,
            "elapsed_seconds": elapsed,
        }

    def get_jobs_for_check(
        self,
        limit: Optional[int] = None,
        status: str = "OPEN",
    ) -> List[Dict[str, Any]]:
        """Retrieves jobs from PostgreSQL to be audited."""
        from scraper_agent.db.connection import get_dict_cursor
        with get_dict_cursor() as cur:
            query = """
                SELECT j.id, j.company_id, c.name as company_name, j.title, j.job_url,
                       j.application_url, j.external_job_id, j.status, j.first_seen_at, j.last_seen_at
                FROM jobs j
                LEFT JOIN companies c ON c.id = j.company_id
            """
            params: List[Any] = []
            if status and status.upper() != "ALL":
                query += " WHERE j.status = %s"
                params.append(status.upper())

            query += " ORDER BY j.last_seen_at ASC, j.id ASC"

            if limit and limit > 0:
                query += " LIMIT %s"
                params.append(limit)

            cur.execute(query, tuple(params))
            return cur.fetchall()

    @staticmethod
    def _print_summary(
        total: int,
        counts: Dict[str, int],
        updated_db_counts: Dict[str, int],
        elapsed_seconds: float,
        results: List[JobVerificationResult],
    ) -> None:
        """Prints a rich formatted table matching the Important Database Rules."""
        table = Table(title="Database Rules Execution Summary", border_style="cyan")
        table.add_column("Rule / Condition", style="yellow")
        table.add_column("Jobs Evaluated", style="bold green", justify="right")
        table.add_column("Database Action Taken", style="magenta")

        table.add_row(
            "URL Active + Job Open",
            str(counts[VerificationAction.KEEP_ACTIVE.value]),
            "Keep job Active (status = OPEN maintained)",
        )
        table.add_row(
            "URL Active + Job Closed / Expired",
            str(counts[VerificationAction.MARK_CLOSED.value]),
            "[bold red]Directly Removed / Deleted from DB (no CLOSED status)[/bold red]",
        )
        table.add_row(
            "CAPTCHA / Login / Network Error",
            str(counts[VerificationAction.NO_CHANGE.value]),
            "[bold red]Directly Removed / Deleted from DB (prevents apply errors)[/bold red]",
        )
        table.add_row(
            "URL Active + Status Unclear",
            str(counts[VerificationAction.KEEP_STATUS.value]),
            "[bold green]Kept existing status; NOT deleted[/bold green]",
        )
        table.add_row(
            "Total Jobs Processed",
            str(total),
            f"DB Actions: {updated_db_counts.get('DELETED', 0)} Deleted (Removed) | {updated_db_counts.get('OPEN', 0)} Active Refreshed | {updated_db_counts.get('UNTOUCHED', 0)} Untouched",
        )
        table.add_row(
            "Execution Time",
            f"{elapsed_seconds:.1f}s",
            f"~{total / max(elapsed_seconds, 0.01):.1f} jobs/sec",
        )
        console.print(table)

        # Print sample removed/deleted jobs if any
        deleted_samples = [r for r in results if r.should_delete][:5]
        if deleted_samples:
            ct = Table(title="Sample Removed / Deleted Jobs (Closed or Inaccessible)", border_style="red")
            ct.add_column("ID", style="dim")
            ct.add_column("Company / Title", style="cyan")
            ct.add_column("Reason", style="yellow")
            for ds in deleted_samples:
                disp = f"{ds.company_name or 'Company'} - {ds.title or 'Job'}"
                ct.add_row(str(ds.job_id), disp[:45], ds.details.notes[:60])
            console.print(ct)
        console.print()

