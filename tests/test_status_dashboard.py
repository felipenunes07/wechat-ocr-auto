import tempfile
import unittest
from pathlib import Path

from wechat_receipt_daemon import StateDB
from wechat_status_dashboard import (
    IGNORE_ITEM_REASON,
    IGNORED_BY_USER_STATE,
    RELEASED_AFTER_IGNORE_REASON,
    ignore_selected_queue_item,
    load_snapshot,
    message_state_label,
    queue_filter_bucket,
    wait_reason_label,
)


class DashboardDBTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.temp_dir.name)
        self.db_path = self.base_dir / "wechat_receipt_state.db"
        self.db = StateDB(self.db_path)

    def tearDown(self) -> None:
        try:
            self.db._conn.close()
        except Exception:
            pass
        self.temp_dir.cleanup()

    def insert_file(
        self,
        *,
        file_id: str,
        path: str,
        status: str,
        last_error: str | None,
        msg_svr_id: str | None = None,
        talker: str = "grupo-a",
        source_kind: str = "msgattach_thumb_dat",
        attempts: int = 0,
        first_seen: float = 100.0,
        next_attempt: float = 120.0,
    ) -> None:
        self.db._conn.execute(
            """
            INSERT INTO files(
                file_id, path, source_kind, ext, size, mtime, ctime, status,
                attempts, next_attempt, first_seen, last_seen, msg_svr_id,
                talker, msg_create_time, manual_session_id, session_release_at,
                processed_at, sha256, last_error
            )
            VALUES(?, ?, ?, '.dat', 1, 100, 100, ?, ?, ?, ?, ?, ?, ?, 1000, NULL, 0, NULL, '', ?)
            """,
            (
                file_id,
                path,
                source_kind,
                status,
                int(attempts),
                float(next_attempt),
                float(first_seen),
                float(first_seen),
                msg_svr_id,
                talker,
                last_error,
            ),
        )
        self.db._conn.commit()

    def insert_message_job(
        self,
        *,
        msg_svr_id: str,
        state: str,
        talker: str = "grupo-a",
        talker_display: str = "Grupo A",
        thumb_path: str | None = None,
        expected_image_path: str | None = None,
        last_ui_result: str = "",
        ui_force_attempts: int = 0,
    ) -> None:
        self.db._conn.execute(
            """
            INSERT INTO message_jobs(
                msg_svr_id, talker, talker_display, thumb_path, expected_image_path,
                create_time, state, first_seen_at, last_seen_at, ui_force_requested_at,
                ui_force_completed_at, ui_force_attempts, next_ui_attempt_at,
                last_ui_result, batch_id, manual_session_id, activation_seen_at
            )
            VALUES(?, ?, ?, ?, ?, 1000, ?, 100, 100, NULL, NULL, ?, 0, ?, NULL, NULL, 100)
            """,
            (
                msg_svr_id,
                talker,
                talker_display,
                thumb_path,
                expected_image_path,
                state,
                int(ui_force_attempts),
                last_ui_result,
            ),
        )
        self.db._conn.commit()

    def insert_receipt(
        self,
        *,
        file_id: str,
        source_path: str,
        sheet_status: str,
        sheet_last_error: str | None = None,
        msg_svr_id: str | None = None,
        talker: str = "grupo-a",
    ) -> None:
        self.db._conn.execute(
            """
            INSERT INTO receipts(
                file_id, source_path, source_kind, ingested_at, sha256, txn_date, txn_time,
                client, bank, beneficiary, amount, currency, parse_conf, quality_score,
                ocr_engine, ocr_conf, ocr_chars, review_needed, ocr_text, parser_json,
                msg_svr_id, talker, msg_create_time, manual_session_id, resolved_media_path,
                resolution_source, verification_status, sheet_status, sheet_payload_json,
                sheet_next_attempt, sheet_last_error, sheet_committed_at, excel_sheet, excel_row
            )
            VALUES(?, ?, 'msgattach_thumb_dat', 100, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   0.9, 0.9, 'rapidocr', 0.9, 100, 0, '', '{}', ?, ?, 1000, NULL, NULL, 'direct_image',
                   'CONFIRMADO', ?, '{}', 100, ?, NULL, NULL, NULL)
            """,
            (
                file_id,
                source_path,
                msg_svr_id,
                talker,
                sheet_status,
                sheet_last_error,
            ),
        )
        self.db._conn.commit()


class StatusLabelTests(unittest.TestCase):
    def test_wait_reason_label_uses_friendly_text(self) -> None:
        self.assertEqual(wait_reason_label("WAITING_SESSION_PRIOR_MESSAGE_ORDER:123"), "Bloqueada por mensagem anterior")
        self.assertEqual(wait_reason_label("WAITING_PRIOR_SINK_RECEIPT:file:1"), "Bloqueada por envio anterior")
        self.assertEqual(wait_reason_label("MISSING_CLIENT_MAP:abc"), "Grupo sem cliente mapeado")

    def test_message_state_label_handles_ignored_and_unknown(self) -> None:
        self.assertEqual(message_state_label(IGNORED_BY_USER_STATE), "Ignorada")
        self.assertEqual(message_state_label("CUSTOM_STATE"), "Custom State")


class IgnoreSelectedQueueItemTests(DashboardDBTestCase):
    def test_ignore_selected_item_marks_related_rows_and_releases_blockers(self) -> None:
        blocker_path = str(self.base_dir / "blocker.dat")
        sibling_path = str(self.base_dir / "blocker-thumb.dat")

        self.insert_file(
            file_id="file-blocker",
            path=blocker_path,
            status="retry",
            last_error="WAITING_ORIGINAL_MEDIA",
            msg_svr_id="msg-1",
        )
        self.insert_file(
            file_id="file-sibling",
            path=sibling_path,
            status="pending",
            last_error="WAITING_ORIGINAL_MEDIA",
            msg_svr_id="msg-1",
        )
        self.insert_file(
            file_id="file-dependent",
            path=str(self.base_dir / "dependent.dat"),
            status="retry",
            last_error="WAITING_SESSION_PRIOR_MESSAGE_ORDER:msg-1",
            msg_svr_id="msg-2",
        )
        self.insert_file(
            file_id="file-other",
            path=str(self.base_dir / "other.dat"),
            status="retry",
            last_error="WAITING_SESSION_PRIOR_MESSAGE_ORDER:msg-9",
            msg_svr_id="msg-9",
        )

        self.insert_message_job(
            msg_svr_id="msg-1",
            state="WAITING_ORIGINAL",
            thumb_path=sibling_path,
            expected_image_path=blocker_path,
        )
        self.insert_receipt(
            file_id="file-blocker",
            source_path=blocker_path,
            sheet_status="SINK_PENDING",
            msg_svr_id="msg-1",
        )
        self.insert_receipt(
            file_id="file-sink-blocked",
            source_path=str(self.base_dir / "sink-blocked.dat"),
            sheet_status="SINK_BLOCKED_PRIOR_MSG",
            sheet_last_error="WAITING_PRIOR_SINK_SESSION_MESSAGE:msg-1",
            msg_svr_id="msg-3",
        )
        self.insert_receipt(
            file_id="file-sink-other",
            source_path=str(self.base_dir / "sink-other.dat"),
            sheet_status="SINK_BLOCKED_PRIOR_MSG",
            sheet_last_error="WAITING_PRIOR_SINK_SESSION_MESSAGE:msg-9",
            msg_svr_id="msg-4",
        )

        ok, message = ignore_selected_queue_item(self.base_dir, "file-blocker")

        self.assertTrue(ok, message)

        files = {
            row["file_id"]: row
            for row in self.db._conn.execute(
                "SELECT file_id, status, last_error FROM files ORDER BY file_id"
            ).fetchall()
        }
        self.assertEqual(files["file-blocker"]["status"], "ignored")
        self.assertEqual(files["file-blocker"]["last_error"], IGNORE_ITEM_REASON)
        self.assertEqual(files["file-sibling"]["status"], "ignored")
        self.assertEqual(files["file-dependent"]["status"], "retry")
        self.assertEqual(files["file-dependent"]["last_error"], RELEASED_AFTER_IGNORE_REASON)
        self.assertEqual(files["file-other"]["last_error"], "WAITING_SESSION_PRIOR_MESSAGE_ORDER:msg-9")

        msg_row = self.db._conn.execute(
            "SELECT state, last_ui_result FROM message_jobs WHERE msg_svr_id='msg-1'"
        ).fetchone()
        self.assertEqual(msg_row["state"], IGNORED_BY_USER_STATE)
        self.assertEqual(msg_row["last_ui_result"], IGNORE_ITEM_REASON)

        receipts = {
            row["file_id"]: row
            for row in self.db._conn.execute(
                "SELECT file_id, sheet_status, sheet_last_error FROM receipts ORDER BY file_id"
            ).fetchall()
        }
        self.assertEqual(receipts["file-blocker"]["sheet_status"], "SINK_SKIPPED_BY_USER_ITEM")
        self.assertEqual(receipts["file-blocker"]["sheet_last_error"], IGNORE_ITEM_REASON)
        self.assertEqual(receipts["file-sink-blocked"]["sheet_status"], "SINK_RETRY")
        self.assertEqual(receipts["file-sink-blocked"]["sheet_last_error"], RELEASED_AFTER_IGNORE_REASON)
        self.assertEqual(receipts["file-sink-other"]["sheet_last_error"], "WAITING_PRIOR_SINK_SESSION_MESSAGE:msg-9")


class SnapshotTests(DashboardDBTestCase):
    def test_snapshot_uses_new_fields_and_hides_ignored_message_states(self) -> None:
        visible_path = str(self.base_dir / "visible.dat")
        self.insert_file(
            file_id="file-visible",
            path=visible_path,
            status="retry",
            last_error="WAITING_ORIGINAL_MEDIA",
            msg_svr_id="msg-visible",
        )
        self.insert_file(
            file_id="file-failure",
            path=str(self.base_dir / "failure.dat"),
            status="exception",
            last_error="EXCEPTION_MISSING_CORE_FIELDS",
        )
        self.insert_message_job(
            msg_svr_id="msg-visible",
            state="WAITING_ORIGINAL",
            thumb_path=visible_path,
            expected_image_path=visible_path,
            last_ui_result="WAITING_UI_FORCE_DOWNLOAD",
        )
        self.insert_message_job(
            msg_svr_id="msg-hidden-1",
            state="IGNORED_SESSION_ROLLOVER",
            thumb_path=str(self.base_dir / "hidden1.dat"),
            expected_image_path=str(self.base_dir / "hidden1.dat"),
        )
        self.insert_message_job(
            msg_svr_id="msg-hidden-2",
            state=IGNORED_BY_USER_STATE,
            thumb_path=str(self.base_dir / "hidden2.dat"),
            expected_image_path=str(self.base_dir / "hidden2.dat"),
        )
        self.insert_receipt(
            file_id="receipt-visible",
            source_path=str(self.base_dir / "receipt-visible.dat"),
            sheet_status="SINK_COMMITTED",
            msg_svr_id="msg-finished",
        )

        snapshot = load_snapshot(self.base_dir)

        self.assertIn("Na fila", snapshot.metrics)
        self.assertIn("Precisam atencao", snapshot.metrics)
        self.assertEqual(len(snapshot.queue_rows), 2)
        first_row = snapshot.queue_rows[0]
        self.assertIn("file_id", first_row)
        self.assertIn("msg_svr_id", first_row)
        self.assertIn("arquivo_estado", first_row)
        self.assertIn("mensagem_estado", first_row)
        self.assertIn("motivo", first_row)
        self.assertEqual(queue_filter_bucket(first_row), first_row["queue_bucket"])

        visible_msgs = [row["msg_svr_id"] for row in snapshot.message_rows]
        self.assertEqual(visible_msgs, ["msg-visible"])


if __name__ == "__main__":
    unittest.main()
