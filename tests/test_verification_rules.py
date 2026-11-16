import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
import requests

from scraper_agent.verification.models import (
    VerificationAction,
    VerificationReason,
    JobVerificationResult,
)
from scraper_agent.verification.checker import JobAvailabilityChecker
from scraper_agent.verification.verifier import JobBatchVerifier


class TestVerificationRules(unittest.TestCase):

    def setUp(self):
        self.checker = JobAvailabilityChecker(request_timeout=2, max_retries=2, backoff_seconds=0.01)

    # -------------------------------------------------------------------------
    # Rule 1: URL Active + Job Open -> Keep job Active
    # -------------------------------------------------------------------------
    def test_rule_1_url_active_job_open(self):
        html_doc = """
        <html>
            <body>
                <h1>Senior Backend Software Engineer</h1>
                <p>Build scalable Python services.</p>
                <a href="#apply" class="btn btn-primary">Apply Now</a>
                <div id="apply">
                    <form action="/apply">
                        <input type="text" name="first_name" />
                        <input type="email" name="email" />
                        <input type="file" name="resume" />
                        <button type="submit">Submit</button>
                    </form>
                </div>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.KEEP_ACTIVE)
            self.assertEqual(res.reason, VerificationReason.JOB_OPEN)
            self.assertEqual(res.target_db_status, "OPEN")
            self.assertTrue(res.details.url_opens)
            self.assertTrue(res.details.job_status_active)

    # -------------------------------------------------------------------------
    # Rule 2: URL Active + Job Closed -> Mark job Closed
    # -------------------------------------------------------------------------
    def test_rule_2_closed_phrase_job_closed(self):
        html_doc = """
        <html>
            <body>
                <h1>Frontend Engineer</h1>
                <div class="alert alert-warning">This job is closed</div>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.JOB_CLOSED_MESSAGE)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_rule_2_closed_phrase_no_longer_accepting(self):
        html_doc = """
        <html>
            <body>
                <h1>Machine Learning Engineer</h1>
                <p>We are no longer accepting applications for this role.</p>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.JOB_CLOSED_MESSAGE)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_rule_2_closed_phrase_position_filled(self):
        html_doc = """
        <html>
            <body>
                <h1>Data Scientist</h1>
                <div class="banner">This position has been filled.</div>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.JOB_CLOSED_MESSAGE)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_rule_2_closed_via_redirect_to_career_home(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/careers"
        mock_resp.text = "<html><body><h1>Careers at Company</h1></body></html>"

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/careers/staff-engineer-5432")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.REDIRECTED_TO_CAREER_HOME)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_rule_2_closed_via_schema_expiration(self):
        expired = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        html_doc = f"""
        <html>
            <head>
                <script type="application/ld+json">
                {{
                    "@context": "https://schema.org",
                    "@type": "JobPosting",
                    "title": "Site Reliability Engineer",
                    "validThrough": "{expired}"
                }}
                </script>
            </head>
            <body>
                <h1>Site Reliability Engineer</h1>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.SCHEMA_EXPIRED)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    # -------------------------------------------------------------------------
    # Rule 3: URL Active + Status Unclear -> Keep existing status; do not mark Closed
    # -------------------------------------------------------------------------
    def test_rule_3_status_unclear_preserves_status(self):
        # 200 OK, no explicit closed text, but no standard HTML apply button found
        html_doc = """
        <html>
            <body>
                <div id="root">
                    <p>Loading application portal...</p>
                </div>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            # IMPORTANT: Must be KEEP_STATUS, and target_db_status must be None (do NOT mark closed)
            self.assertEqual(res.action, VerificationAction.KEEP_STATUS)
            self.assertEqual(res.reason, VerificationReason.STATUS_UNCLEAR)
            self.assertIsNone(res.target_db_status)
            self.assertNotEqual(res.target_db_status, "CLOSED")

    # -------------------------------------------------------------------------
    # Rule 4: URL Inaccessible -> Retry verification before changing status
    # -------------------------------------------------------------------------
    def test_rule_4_retry_recovers_from_transient_error(self):
        mock_503 = MagicMock()
        mock_503.status_code = 503

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.url = "https://company.com/jobs/1234"
        mock_200.text = "<html><body><h1>DevOps</h1><a href='/apply'>Apply Now</a></body></html>"

        with patch.object(self.checker.session, "get", side_effect=[mock_503, mock_200]):
            resp, retries, exc = self.checker.fetch_with_retry("https://company.com/jobs/1234")
            self.assertIsNotNone(resp)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(retries, 1)

    def test_rule_4_and_6_inaccessible_404_after_retries_marks_closed(self):
        mock_404 = MagicMock()
        mock_404.status_code = 404
        mock_404.url = "https://company.com/jobs/9999"
        mock_404.text = "404 Not Found"

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_404, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/9999")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.URL_NOT_FOUND_404)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    # -------------------------------------------------------------------------
    # Rule 5: CAPTCHA / Login / Network Error -> Inaccessible for apply agent -> Remove from DB
    # -------------------------------------------------------------------------
    def test_rule_5_captcha_blocked_does_not_change_status(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.url = "https://company.com/jobs/1234"
        mock_resp.text = "<html><head><title>Attention Required! | Cloudflare</title></head><body>Verify you are human</body></html>"

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.NO_CHANGE)
            self.assertEqual(res.reason, VerificationReason.CAPTCHA_BLOCKED)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_rule_5_login_required_does_not_change_status(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.url = "https://company.com/login?redirect=/jobs/1234"
        mock_resp.text = "<html><body>Please sign in to apply</body></html>"

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.NO_CHANGE)
            self.assertEqual(res.reason, VerificationReason.LOGIN_REQUIRED)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_rule_5_network_error_after_retries_does_not_change_status(self):
        exc = requests.exceptions.Timeout("Connection timed out after retries")
        with patch.object(self.checker, "fetch_with_retry", return_value=(None, 3, exc)):
            res = self.checker.verify_job("https://company.com/jobs/1234")
            self.assertEqual(res.action, VerificationAction.NO_CHANGE)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    # -------------------------------------------------------------------------
    # Workday CXS API Verification Tests
    # -------------------------------------------------------------------------
    def test_workday_cxs_closed_marks_closed(self):
        mock_404 = MagicMock()
        mock_404.status_code = 404

        with patch.object(self.checker.session, "get", return_value=mock_404):
            res = self.checker.verify_job("https://cisco.wd5.myworkdayjobs.com/Cisco_Careers/job/Fulton-MD/Software-Engineer_12345")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.URL_NOT_FOUND_404)
            self.assertTrue(res.should_delete)
            self.assertIsNone(res.target_db_status)

    def test_workday_cxs_active_keeps_active(self):
        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"jobPostingInfo": {"title": "Software Engineer"}}

        with patch.object(self.checker.session, "get", return_value=mock_200):
            res = self.checker.verify_job("https://cisco.wd5.myworkdayjobs.com/Cisco_Careers/job/Fulton-MD/Software-Engineer_12345")
            self.assertEqual(res.action, VerificationAction.KEEP_ACTIVE)
            self.assertEqual(res.reason, VerificationReason.JOB_OPEN)
            self.assertEqual(res.target_db_status, "OPEN")

    # -------------------------------------------------------------------------
    # Batch Verifier and Repository Direct Deletion Tests
    # -------------------------------------------------------------------------
    def test_batch_verifier_deletes_expired_job_directly(self):
        from scraper_agent.verification.models import JobVerificationDetails
        verifier = JobBatchVerifier(checker=self.checker)
        sample_jobs = [{"id": 101, "job_url": "https://company.com/jobs/101"}]

        mock_result = JobVerificationResult(
            job_id=101,
            job_url="https://company.com/jobs/101",
            action=VerificationAction.MARK_CLOSED,
            reason=VerificationReason.URL_NOT_FOUND_404,
            details=JobVerificationDetails(notes="404 Not Found"),
        )

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch.object(verifier, "get_jobs_for_check", return_value=sample_jobs), \
             patch.object(verifier, "verify_single_job", return_value=mock_result), \
             patch("scraper_agent.db.connection.get_db_connection", return_value=mock_conn):
            stats = verifier.run_verification(dry_run=False)

            self.assertEqual(stats["deleted"], 1)
            # Ensure DELETE query is issued and NOT an UPDATE to CLOSED
            mock_cur.execute.assert_called_once()
            called_sql = mock_cur.execute.call_args[0][0]
            self.assertIn("DELETE FROM jobs", called_sql)
            self.assertNotIn("SET status = 'CLOSED'", called_sql)
            self.assertEqual(mock_cur.execute.call_args[0][1], (101,))

    def test_batch_verifier_deletes_error_jobs_directly(self):
        from scraper_agent.verification.models import JobVerificationDetails
        verifier = JobBatchVerifier(checker=self.checker)
        sample_jobs = [{"id": 202, "job_url": "https://company.com/jobs/202"}]

        mock_result = JobVerificationResult(
            job_id=202,
            job_url="https://company.com/jobs/202",
            action=VerificationAction.NO_CHANGE,
            reason=VerificationReason.CAPTCHA_BLOCKED,
            details=JobVerificationDetails(notes="Cloudflare CAPTCHA blocked"),
        )

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch.object(verifier, "get_jobs_for_check", return_value=sample_jobs), \
             patch.object(verifier, "verify_single_job", return_value=mock_result), \
             patch("scraper_agent.db.connection.get_db_connection", return_value=mock_conn):
            stats = verifier.run_verification(dry_run=False)

            self.assertEqual(stats["deleted"], 1)
            mock_cur.execute.assert_called_once()
            called_sql = mock_cur.execute.call_args[0][0]
            self.assertIn("DELETE FROM jobs", called_sql)
            self.assertEqual(mock_cur.execute.call_args[0][1], (202,))

    def test_repository_delete_closed_jobs(self):
        from scraper_agent.db.repository import JobRepository
        mock_cur = MagicMock()
        mock_cur.rowcount = 42
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("scraper_agent.db.repository.get_db_connection", return_value=mock_conn):
            deleted = JobRepository.delete_closed_jobs()
            self.assertEqual(deleted, 42)
            mock_cur.execute.assert_called_once_with("DELETE FROM jobs WHERE LOWER(status) = 'closed';")

    def test_repository_purge_invalid_error_jobs(self):
        from scraper_agent.db.repository import JobRepository
        mock_cur = MagicMock()
        mock_cur.rowcount = 12
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("scraper_agent.db.repository.get_db_connection", return_value=mock_conn):
            purged = JobRepository.purge_invalid_error_jobs()
            self.assertEqual(purged, 12)
            self.assertTrue(mock_cur.execute.called)
            sql = mock_cur.execute.call_args[0][0]
            self.assertIn("DELETE FROM jobs", sql)
            self.assertIn("ILIKE ANY", sql)

    def test_rule_2_workday_page_does_not_exist_phrase(self):
        html_doc = """
        <html>
            <body>
                <h1>Error</h1>
                <div class="notice">The page you are looking for doesn't exist.</div>
            </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/job/12345"
        mock_resp.text = html_doc

        with patch.object(self.checker, "fetch_with_retry", return_value=(mock_resp, 0, None)):
            res = self.checker.verify_job("https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/job/12345")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertEqual(res.reason, VerificationReason.JOB_CLOSED_MESSAGE)
            self.assertTrue(res.should_delete)

    def test_workday_cxs_empty_jobpostinginfo_marks_closed(self):
        mock_200 = MagicMock()
        mock_200.status_code = 200
        # Workday returns 200 without jobPostingInfo when requisition is closed
        mock_200.json.return_value = {"error": "Requisition not found"}

        with patch.object(self.checker.session, "get", return_value=mock_200):
            res = self.checker.verify_job("https://cisco.wd5.myworkdayjobs.com/Cisco_Careers/job/Fulton-MD/Software-Engineer_12345")
            self.assertEqual(res.action, VerificationAction.MARK_CLOSED)
            self.assertTrue(res.should_delete)


if __name__ == "__main__":
    unittest.main()

