from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any


class VerificationAction(str, Enum):
    """Action to take in the database based on the verification rules."""
    KEEP_ACTIVE = "KEEP_ACTIVE"        # URL Active + Job Open -> Keep job Active (status = OPEN)
    MARK_CLOSED = "MARK_CLOSED"        # URL Active + Job Closed / Expired -> Remove/Delete job directly from DB
    REMOVE_CLOSED = "MARK_CLOSED"      # Alias: Directly remove expired/closed job from database
    KEEP_STATUS = "KEEP_STATUS"        # URL Active + Status Unclear -> Keep existing status; do not remove or mark Closed
    NO_CHANGE = "NO_CHANGE"            # CAPTCHA / Login / Network Error -> Remove/Delete job directly from DB (prevents apply errors)
    REMOVE_ERROR = "NO_CHANGE"         # Alias: Remove inaccessible/error jobs from DB


class VerificationReason(str, Enum):
    """Categorized explanation for verification outcomes."""
    JOB_OPEN = "JOB_OPEN"
    JOB_CLOSED_MESSAGE = "JOB_CLOSED_MESSAGE"
    SCHEMA_EXPIRED = "SCHEMA_EXPIRED"
    REDIRECTED_TO_CAREER_HOME = "REDIRECTED_TO_CAREER_HOME"
    URL_NOT_FOUND_404 = "URL_NOT_FOUND_404"
    URL_GONE_410 = "URL_GONE_410"
    STATUS_UNCLEAR = "STATUS_UNCLEAR"
    CAPTCHA_BLOCKED = "CAPTCHA_BLOCKED"
    LOGIN_REQUIRED = "LOGIN_REQUIRED"
    NETWORK_ERROR = "NETWORK_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    SERVER_ERROR = "SERVER_ERROR"


@dataclass
class JobVerificationDetails:
    """Detailed audit information collected during verification."""
    url_opens: bool = False
    status_code: Optional[int] = None
    final_url: Optional[str] = None
    is_redirect_to_portal: bool = False
    closed_message_detected: bool = False
    matched_closed_phrase: Optional[str] = None
    job_status_active: bool = False
    apply_button_found: bool = False
    apply_button_enabled: bool = False
    application_form_active: bool = False
    is_captcha: bool = False
    is_login_required: bool = False
    is_network_error: bool = False
    retries_performed: int = 0
    notes: str = ""


@dataclass
class JobVerificationResult:
    """Outcome of verifying a single job posting against the Important Database Rules."""
    job_id: int
    job_url: str
    action: VerificationAction
    reason: VerificationReason
    details: JobVerificationDetails
    title: Optional[str] = None
    company_name: Optional[str] = None
    external_job_id: Optional[str] = None
    verified_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def target_db_status(self) -> Optional[str]:
        """
        Database status to set.
        Returns 'OPEN' if KEEP_ACTIVE.
        Returns None for deleted jobs (closed/expired or error/inaccessible), and None for KEEP_STATUS.
        """
        if self.action == VerificationAction.KEEP_ACTIVE:
            return "OPEN"
        return None  # Expired and inaccessible error jobs are deleted directly, never set to CLOSED status.

    @property
    def should_delete(self) -> bool:
        """
        Returns True if the job should be deleted directly from DB.
        Includes confirmed closed/expired jobs and inaccessible error jobs
        (CAPTCHA, login wall, network failure) so they do not cause applicant agent failures.
        """
        return self.action in (
            VerificationAction.MARK_CLOSED,
            VerificationAction.REMOVE_CLOSED,
            VerificationAction.NO_CHANGE,
            VerificationAction.REMOVE_ERROR,
        )

