import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_receipt_daemon import (
    StateDB,
    WeChatMessageRef,
    backfill_missing_receipt_fields,
    normalize_amount,
    normalize_client_label,
    parse_receipt_fields,
    round_amount_for_output,
    should_ignore_sender,
)


class NormalizeAmountTests(unittest.TestCase):
    def test_brazilian_grouping_uses_thousands_separator(self) -> None:
        self.assertEqual(normalize_amount("30.000"), 30000.0)
        self.assertEqual(normalize_amount("2.525"), 2525.0)

    def test_decimal_values_keep_fraction(self) -> None:
        self.assertEqual(normalize_amount("2,5"), 2.5)
        self.assertEqual(normalize_amount("30.000,00"), 30000.0)

    def test_round_amount_for_output_uses_half_up_rule(self) -> None:
        self.assertEqual(round_amount_for_output(1.52), 2.0)
        self.assertEqual(round_amount_for_output(1.49), 1.0)
        self.assertEqual(round_amount_for_output(0.50), 1.0)


class ParseReceiptFieldsTests(unittest.TestCase):
    def test_ignores_year_token_that_looks_like_currency(self) -> None:
        text = "\n".join(
            [
                "Comprovantedetransferencia",
                "20MAR2026-09:30:50",
                "Valor",
                "R$650,00",
                "Tipodetransferencia",
                "Pix",
                "IDdatransacao",
                "E18236120202603201229s0972ec9cf7",
                "Destino",
                "Nome",
                "CLEENDELETRONICOS",
                "CNPJ",
                "61964978000168",
                "Instituicao",
                "BCOBRADESCOS.A.",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "20/03/2026")
        self.assertEqual(fields["txn_time"], "09:30")
        self.assertEqual(fields["amount"], 650.0)
        self.assertEqual(fields["txn_date_source"], "parsed")
        self.assertEqual(fields["txn_time_source"], "parsed")

    def test_prefers_grouped_brl_amount(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "20/03/2026 as 11:20:00",
                "Valor do pagamento",
                "R$30.000",
                "Destino",
                "Nome",
                "CLEENDELETRONICOS",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 30000.0)
        self.assertEqual(fields["amount_rounded"], 30000.0)

    def test_parses_full_month_and_compact_cent_fix(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "20/marco/2026 as 11h35.",
                "R$ 66804",
                "Banco Bradesco",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "20/03/2026")
        self.assertEqual(fields["txn_time"], "11:35")
        self.assertEqual(fields["amount"], 668.04)
        self.assertEqual(fields["amount_rounded"], 668.0)
        self.assertEqual(fields["amount_source"], "currency_compact_cent_fix")

    def test_parses_compact_alpha_month_datetime(self) -> None:
        text = "\n".join(
            [
                "itau",
                "13mar.2026,15:44:53,viaSISPAGnoappItau",
                "Valor da transferencia",
                "R$1.680,00",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "13/03/2026")
        self.assertEqual(fields["txn_time"], "15:44")
        self.assertEqual(fields["amount"], 1680.0)

    def test_parses_numeric_date_glued_to_time(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pagamento Pix",
                "Realizada em",
                "02/02/202615:31:50",
                "Valor",
                "R$8.727,85",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "02/02/2026")
        self.assertEqual(fields["txn_time"], "15:31")
        self.assertEqual(fields["amount"], 8727.85)

    def test_falls_back_to_today_and_dash_when_datetime_missing(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "Valor do pagamento",
                "R$ 250,00",
            ]
        )

        with patch("wechat_receipt_daemon.today_local_date_str", return_value="20/03/2026"):
            fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "20/03/2026")
        self.assertEqual(fields["txn_time"], "-")
        self.assertEqual(fields["txn_date_source"], "fallback_today")
        self.assertEqual(fields["txn_time_source"], "fallback_dash")


class ClientLabelTests(unittest.TestCase):
    def test_extracts_numeric_identifier_from_group_labels(self) -> None:
        self.assertEqual(normalize_client_label("2026 PP\u7fa4"), ("6", None))
        self.assertEqual(normalize_client_label("2026 65-2\u7fa4"), ("652", None))
        self.assertEqual(normalize_client_label("2026 116A\u7fa4"), ("116A", None))
        self.assertEqual(normalize_client_label("2026 65\u7fa4"), ("65", None))
        self.assertEqual(normalize_client_label("2026 16Boleto"), ("16", None))
        self.assertEqual(normalize_client_label(f"2026{chr(0x2014) * 5}1\u7fa4no\u7fa4\U0001f4b0"), ("1", None))

    def test_ignores_purely_decorative_group_labels(self) -> None:
        strawberries = "2026" + ("\U0001f353" * 6)
        self.assertEqual(normalize_client_label(strawberries), (None, "IGNORED_CLIENT_LABEL_DECORATIVE"))


class SenderIgnoreTests(unittest.TestCase):
    def test_ignores_configured_sender_ids(self) -> None:
        msg_ref = WeChatMessageRef(
            msg_svr_id="1",
            talker="27837425841@chatroom",
            create_time=1.0,
            sender_user_name="wxid_wml3ftd6qpea12",
            sender_display="Arthur Shelby",
            image_rel_path=None,
            thumb_rel_path=None,
            image_abs_path=None,
            thumb_abs_path=None,
        )
        self.assertTrue(should_ignore_sender(msg_ref))

    def test_allows_other_senders(self) -> None:
        msg_ref = WeChatMessageRef(
            msg_svr_id="2",
            talker="27837425841@chatroom",
            create_time=1.0,
            sender_user_name="wxid_cliente_real",
            sender_display="Cliente Real",
            image_rel_path=None,
            thumb_rel_path=None,
            image_abs_path=None,
            thumb_abs_path=None,
        )
        self.assertFalse(should_ignore_sender(msg_ref))


def build_receipt_payload(
    *,
    file_id: str,
    ingested_at: float,
    msg_svr_id: str,
    msg_create_time: float,
    amount: float,
    amount_rounded: float,
) -> dict[str, object]:
    row_payload = {
        "file_id": file_id,
        "client": "65",
        "txn_date": "20/03/2026",
        "txn_time": "11:35",
        "bank": "CLEEND",
        "amount": amount_rounded,
        "verification_status": "CONFIRMADO",
        "msg_svr_id": msg_svr_id,
        "talker": "27837425841@chatroom",
    }
    return {
        "file_id": file_id,
        "source_path": f"C:/fake/{file_id}.dat",
        "source_kind": "msgattach_image_dat",
        "ingested_at": ingested_at,
        "sha256": f"sha-{file_id}",
        "txn_date": "20/03/2026",
        "txn_time": "11:35",
        "txn_date_source": "parsed",
        "txn_time_source": "parsed",
        "client": "65",
        "bank": "CLEEND",
        "beneficiary": "Cliente",
        "amount": amount,
        "amount_raw": str(amount),
        "amount_rounded": amount_rounded,
        "amount_source": "currency",
        "currency": "BRL",
        "parse_conf": 0.99,
        "quality_score": 0.95,
        "ocr_engine": "rapidocr",
        "ocr_conf": 0.99,
        "ocr_chars": 120,
        "review_needed": False,
        "ocr_text": "Comprovante de Pix",
        "parser_json": "{}",
        "msg_svr_id": msg_svr_id,
        "talker": "27837425841@chatroom",
        "msg_create_time": msg_create_time,
        "resolved_media_path": f"C:/fake/{file_id}.dat",
        "resolution_source": "db_image",
        "verification_status": "CONFIRMADO",
        "sheet_status": "SINK_PENDING",
        "sheet_payload_json": json.dumps(row_payload),
        "sheet_next_attempt": 0.0,
        "sheet_last_error": None,
        "sheet_committed_at": None,
        "excel_sheet": None,
        "excel_row": None,
    }


class ManualSessionOrderTests(unittest.TestCase):
    def test_manual_session_ignores_old_pending_message_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.ensure_message_job(
                    msg_svr_id="old-msg",
                    talker="27837425841@chatroom",
                    talker_display="2026 65-2群",
                    thumb_path=Path("C:/fake/old_t.dat"),
                    expected_image_path=Path("C:/fake/old.dat"),
                    create_time=100.0,
                    first_seen_at=1000.0,
                )
                db.set_message_job_state("old-msg", "WAITING_ORIGINAL", note="MANUAL_WAIT_ORIGINAL", next_ui_attempt_at=0.0)

                db.ensure_message_job(
                    msg_svr_id="new-msg",
                    talker="27837425841@chatroom",
                    talker_display="2026 65-2群",
                    thumb_path=Path("C:/fake/new_t.dat"),
                    expected_image_path=Path("C:/fake/new.dat"),
                    create_time=200.0,
                    first_seen_at=2000.0,
                )

                blocker_without_session = db.find_prior_pending_message_job(
                    talker="27837425841@chatroom",
                    create_time=200.0,
                    msg_svr_id="new-msg",
                )
                blocker_with_session = db.find_prior_pending_message_job(
                    talker="27837425841@chatroom",
                    create_time=200.0,
                    msg_svr_id="new-msg",
                    manual_session_started_at=1500.0,
                )

                self.assertIsNotNone(blocker_without_session)
                self.assertIsNone(blocker_with_session)
            finally:
                db.close()

    def test_sink_claim_prioritizes_current_manual_session_and_preserves_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="old-file",
                        ingested_at=1000.0,
                        msg_svr_id="old-msg",
                        msg_create_time=100.0,
                        amount=450.0,
                        amount_rounded=450.0,
                    )
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="current-a",
                        ingested_at=2000.0,
                        msg_svr_id="current-a-msg",
                        msg_create_time=300.0,
                        amount=668.04,
                        amount_rounded=668.0,
                    )
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="current-b",
                        ingested_at=2001.0,
                        msg_svr_id="current-b-msg",
                        msg_create_time=301.0,
                        amount=700.04,
                        amount_rounded=700.0,
                    )
                )

                first_claim = db.claim_next_sink_receipt(manual_session_started_at=1500.0)
                self.assertIsNotNone(first_claim)
                self.assertEqual(first_claim["file_id"], "current-a")
                self.assertEqual(first_claim["row_payload"]["amount"], 668.0)
                db.mark_receipt_sink_committed("current-a", "Plan1", 2, committed_at=2100.0)

                second_claim = db.claim_next_sink_receipt(manual_session_started_at=1500.0)
                self.assertIsNotNone(second_claim)
                self.assertEqual(second_claim["file_id"], "current-b")
                self.assertEqual(second_claim["row_payload"]["amount"], 700.0)
            finally:
                db.close()


class RecordingSink:
    def __init__(self) -> None:
        self.updated_rows: list[tuple[str, int, dict[str, object], bool]] = []

    def append(self, row_payload: dict[str, object], review_needed: bool) -> tuple[str, int]:
        raise NotImplementedError

    def update_row(self, sheet_name: str, row_idx: int, row_payload: dict[str, object], review_needed: bool) -> None:
        self.updated_rows.append((sheet_name, row_idx, row_payload, review_needed))


class ReceiptBackfillTests(unittest.TestCase):
    def test_backfill_updates_committed_receipt_and_sheet_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                payload = build_receipt_payload(
                    file_id="legacy-file",
                    ingested_at=1000.0,
                    msg_svr_id="legacy-msg",
                    msg_create_time=100.0,
                    amount=668.04,
                    amount_rounded=668.0,
                )
                payload.update(
                    {
                        "txn_date": None,
                        "txn_time": None,
                        "txn_date_source": None,
                        "txn_time_source": None,
                        "amount_raw": None,
                        "amount_rounded": None,
                        "amount_source": None,
                        "review_needed": True,
                        "ocr_text": "\n".join(
                            [
                                "Comprovante de Pix",
                                "20/marco/2026 as 11h35.",
                                "R$ 66804",
                                "Banco Bradesco",
                            ]
                        ),
                        "sheet_status": "SINK_COMMITTED",
                        "sheet_payload_json": json.dumps(
                            {
                                "file_id": "legacy-file",
                                "client": "65",
                                "txn_date": "",
                                "txn_time": "",
                                "bank": "CLEEND",
                                "amount": None,
                                "verification_status": "CONFIRMADO",
                                "msg_svr_id": "legacy-msg",
                                "talker": "27837425841@chatroom",
                            }
                        ),
                        "excel_sheet": "Lancamentos",
                        "excel_row": 7,
                    }
                )
                db.insert_receipt(payload)

                sink = RecordingSink()
                cfg = type("Cfg", (), {"min_confidence": 0.8})()

                updated, sheet_updated, sheet_failed = backfill_missing_receipt_fields(db, sink, cfg, limit=10)

                self.assertEqual((updated, sheet_updated, sheet_failed), (1, 1, 0))
                row = db._conn.execute(
                    """
                    SELECT txn_date, txn_time, txn_date_source, txn_time_source,
                           amount, amount_raw, amount_rounded, amount_source,
                           review_needed, sheet_payload_json
                    FROM receipts
                    WHERE file_id='legacy-file'
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["txn_date"], "20/03/2026")
                self.assertEqual(row["txn_time"], "11:35")
                self.assertEqual(row["txn_date_source"], "parsed")
                self.assertEqual(row["txn_time_source"], "parsed")
                self.assertEqual(row["amount"], 668.04)
                self.assertEqual(row["amount_raw"], "66804")
                self.assertEqual(row["amount_rounded"], 668.0)
                self.assertEqual(row["amount_source"], "currency_compact_cent_fix")
                self.assertEqual(row["review_needed"], 1)

                self.assertEqual(len(sink.updated_rows), 1)
                sheet_name, row_idx, row_payload, review_needed = sink.updated_rows[0]
                self.assertEqual(sheet_name, "Lancamentos")
                self.assertEqual(row_idx, 7)
                self.assertEqual(row_payload["txn_date"], "20/03/2026")
                self.assertEqual(row_payload["txn_time"], "11:35")
                self.assertEqual(row_payload["amount"], 668.0)
                self.assertTrue(review_needed)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
