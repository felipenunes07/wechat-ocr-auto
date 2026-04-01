import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_receipt_daemon import (
    IGNORED_SESSION_ROLLOVER_STATE,
    SESSION_PENDING_OPEN_STATE,
    GoogleSheetsSink,
    StateDB,
    WeChatDBResolver,
    WeChatMessageRef,
    backfill_missing_receipt_fields,
    hold_retry_delay_seconds,
    is_candidate,
    normalize_amount,
    normalize_client_label,
    parse_receipt_fields,
    round_amount_for_output,
    runtime_media_resolver,
    seed_ready_manual_session_placeholders,
    should_ignore_sender,
)


class NormalizeAmountTests(unittest.TestCase):
    def test_brazilian_grouping_uses_thousands_separator(self) -> None:
        self.assertEqual(normalize_amount("30.000"), 30000.0)
        self.assertEqual(normalize_amount("2.525"), 2525.0)
        self.assertEqual(normalize_amount("6.60102"), 6601.02)

    def test_decimal_values_keep_fraction(self) -> None:
        self.assertEqual(normalize_amount("2,5"), 2.5)
        self.assertEqual(normalize_amount("30.000,00"), 30000.0)

    def test_round_amount_for_output_uses_half_up_rule(self) -> None:
        self.assertEqual(round_amount_for_output(1.52), 2.0)
        self.assertEqual(round_amount_for_output(1.49), 1.0)
        self.assertEqual(round_amount_for_output(0.50), 1.0)


class HoldRetryDelayTests(unittest.TestCase):
    def test_caps_retry_window_for_fast_manual_rechecks(self) -> None:
        self.assertEqual(hold_retry_delay_seconds(100.0, 107.0), 5)
        self.assertEqual(hold_retry_delay_seconds(100.0, 103.0), 3)
        self.assertEqual(hold_retry_delay_seconds(100.0, 100.4), 2)


class RuntimeMediaResolverTests(unittest.TestCase):
    def test_returns_none_when_resolver_is_degraded(self) -> None:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver._last_error = "merge_failed:timeout"

        self.assertIsNone(runtime_media_resolver(resolver))

    def test_keeps_resolver_when_no_runtime_error(self) -> None:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver._last_error = None

        self.assertIs(runtime_media_resolver(resolver), resolver)


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

    def test_parses_infinitepay_style_destination_and_alpha_month_date(self) -> None:
        text = "\n".join(
            [
                "infinitepay",
                "Comprovante de transferencia Pix",
                "R$ 600,00",
                "28 Mar,2026 14:46",
                "Origem",
                "IRIS PANTOJA SANTIAGO",
                "CPF",
                ".499.782-",
                "Instituicao",
                "CLOUDWALK IP LTDA",
                "Destino",
                "AMD REPRESENTACOES E SERVICOS LTDA",
                "CNPJ",
                "53.356.830/0001-12",
                "Instituicao",
                "BCO DO BRASIL S.A.",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "28/03/2026")
        self.assertEqual(fields["txn_time"], "14:46")
        self.assertEqual(fields["beneficiary"], "AMD REPRESENTACOES E SERVICOS LTDA")
        self.assertEqual(fields["bank"], "AMD")
        self.assertEqual(fields["amount"], 600.0)

    def test_parses_mercado_pago_superscript_cents_amount(self) -> None:
        text = "\n".join(
            [
                "Mercado Pago",
                "Comprovante de Pix",
                "19/marco/2026 as 15h22",
                "R$ 6.60102",
                "Para",
                "Cleend Intermediacao e Atacado Ltda",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "19/03/2026")
        self.assertEqual(fields["txn_time"], "15:22")
        self.assertEqual(fields["amount"], 6601.02)
        self.assertEqual(fields["amount_rounded"], 6601.0)

    def test_ignores_cpf_chunk_when_value_line_was_lost_by_ocr(self) -> None:
        text = "\n".join(
            [
                "Comprovante de",
                "transferencia",
                "19 MAR2026-18:12:43",
                "Valoi",
                "Tipo de transferencia",
                "ID da transacao",
                "E182361202026031",
                "92112s16e16aa8b0",
                "Nome",
                "AMD REPRESENTACOES E",
                "SERVICOSLTDA",
                "CNPJ",
                "53356830000112",
                "Instituicao",
                "BCO DO BRASIL S.A.",
                "Chave Pix",
                "53356830000112",
                "Origem",
                "Nome",
                "Gleisson Silva",
                "Instituicao",
                "NU PAGAMENTOS-IP",
                "CPF",
                "...300.956...",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertIsNone(fields["amount"])
        self.assertIsNone(fields["amount_rounded"])
        self.assertEqual(fields["amount_source"], "missing")

    def test_prefers_real_value_over_cpf_chunk_with_same_digits(self) -> None:
        text = "\n".join(
            [
                "Comprovante de",
                "transferencia",
                "19 MAR2026-18:12:43",
                "Valor",
                "R$29.99",
                "Tipo de transferencia",
                "Pix",
                "ID da transacao",
                "92112s16e16aa8b0",
                "Destino",
                "AMD REPRESENTACOES E",
                "SERVICOS LTDA",
                "CPF",
                "*.300.956**",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 29.99)
        self.assertEqual(fields["amount_rounded"], 30.0)
        self.assertEqual(fields["amount_source"], "currency")

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
    manual_session_id: str | None = None,
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
        "manual_session_id": manual_session_id,
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


def insert_file_row(
    db: StateDB,
    *,
    file_id: str,
    path: str,
    source_kind: str,
    status: str,
    first_seen: float,
    last_error: str | None,
    msg_svr_id: str | None = None,
    talker: str | None = None,
    msg_create_time: float | None = None,
    manual_session_id: str | None = None,
    session_release_at: float = 0.0,
) -> None:
    db._conn.execute(
        """
        INSERT INTO files(
            file_id, path, source_kind, ext, size, mtime, ctime, status,
            attempts, next_attempt, first_seen, last_seen, msg_svr_id, talker, msg_create_time,
            manual_session_id, session_release_at, processed_at, sha256, last_error
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
        """,
        (
            file_id,
            path,
            source_kind,
            Path(path).suffix.lower(),
            10,
            first_seen,
            first_seen,
            status,
            1,
            first_seen + 5.0,
            first_seen,
            first_seen,
            msg_svr_id,
            talker,
            msg_create_time,
            manual_session_id,
            session_release_at,
            last_error,
        ),
    )
    db._conn.commit()


class FakeMediaResolver:
    def __init__(self, messages: list[WeChatMessageRef]) -> None:
        self.messages = messages

    def list_image_messages_for_talker(
        self,
        talker: str | None,
        start_create_time: float,
        end_create_time: float,
        limit: int = 240,
    ) -> list[WeChatMessageRef]:
        talker_value = str(talker or "").strip()
        out = [
            msg
            for msg in self.messages
            if str(msg.talker or "").strip() == talker_value
            and float(start_create_time) <= float(msg.create_time) <= float(end_create_time)
        ]
        return out[:limit]

    def resolve_talker_display_name(self, talker: str | None) -> str | None:
        return str(talker or "").strip() or None


class CandidateFilterTests(unittest.TestCase):
    def test_thumb_is_ignored_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            thumb_path = root / "MsgAttach" / "gid" / "Thumb" / "2026-03" / "receipt_t.dat"
            image_path = root / "MsgAttach" / "gid" / "Image" / "2026-03" / "receipt.dat"
            temp_path = root / "FileStorage" / "Temp" / "receipt.png"
            for path in (thumb_path, image_path, temp_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"x")

            self.assertFalse(is_candidate(thumb_path, thumb_candidates_enabled=False))
            self.assertTrue(is_candidate(thumb_path, thumb_candidates_enabled=True))
            self.assertTrue(is_candidate(image_path, thumb_candidates_enabled=False))
            self.assertTrue(is_candidate(temp_path, thumb_candidates_enabled=False))


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

    def test_realtime_image_event_refreshes_manual_session_but_reconcile_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            image_path = root / "MsgAttach" / "gid" / "Image" / "2026-03" / "manual.dat"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            image_path.write_bytes(b"manual-open")

            db = StateDB(root / "state.db")
            try:
                db.start_manual_session(100.0)

                with patch("wechat_receipt_daemon.time.time", return_value=200.0):
                    db.upsert_candidate(
                        image_path,
                        settle_seconds=5,
                        source_event="reconcile",
                        thumb_candidates_enabled=False,
                    )
                self.assertEqual(db.get_manual_session_started_at(), 100.0)

                with patch("wechat_receipt_daemon.time.time", return_value=300.0):
                    db.upsert_candidate(
                        image_path,
                        settle_seconds=5,
                        source_event="modified",
                        thumb_candidates_enabled=False,
                    )
                self.assertEqual(db.get_manual_session_started_at(), 300.0)
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

    def test_sink_claim_exposes_source_first_seen_for_latency_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="latency-file",
                        ingested_at=2100.0,
                        msg_svr_id="latency-msg",
                        msg_create_time=400.0,
                        amount=900.0,
                        amount_rounded=900.0,
                    )
                )
                insert_file_row(
                    db,
                    file_id="latency-file",
                    path="C:/fake/latency-file.dat",
                    source_kind="msgattach_image_dat",
                    status="done",
                    first_seen=2000.0,
                    last_error=None,
                )

                claimed = db.claim_next_sink_receipt()

                self.assertIsNotNone(claimed)
                self.assertEqual(claimed["source_first_seen"], 2000.0)
                self.assertEqual(claimed["ingested_at"], 2100.0)
            finally:
                db.close()

    def test_claim_next_orders_current_manual_session_by_message_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="newer-file",
                    path="C:/fake/newer-file.dat",
                    source_kind="msgattach_image_dat",
                    status="pending",
                    first_seen=1000.0,
                    last_error=None,
                    msg_svr_id="newer-msg",
                    talker="27837425841@chatroom",
                    msg_create_time=301.0,
                    manual_session_id="session-a",
                    session_release_at=1005.0,
                )
                insert_file_row(
                    db,
                    file_id="older-file",
                    path="C:/fake/older-file.dat",
                    source_kind="msgattach_image_dat",
                    status="pending",
                    first_seen=1002.0,
                    last_error=None,
                    msg_svr_id="older-msg",
                    talker="27837425841@chatroom",
                    msg_create_time=300.0,
                    manual_session_id="session-a",
                    session_release_at=1005.0,
                )

                with patch("wechat_receipt_daemon.time.time", return_value=1010.0):
                    first_claim = db.claim_next(manual_session_id="session-a")
                self.assertIsNotNone(first_claim)
                self.assertEqual(first_claim.file_id, "older-file")

                db.mark_done("older-file", sha256="sha-old", processed_at=1010.0)
                with patch("wechat_receipt_daemon.time.time", return_value=1011.0):
                    second_claim = db.claim_next(manual_session_id="session-a")
                self.assertIsNotNone(second_claim)
                self.assertEqual(second_claim.file_id, "newer-file")
            finally:
                db.close()

    def test_claim_next_prefers_direct_image_before_temp_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="temp-file",
                    path="C:/fake/temp-file.jpg",
                    source_kind="temp_image",
                    status="pending",
                    first_seen=1000.0,
                    last_error=None,
                )
                insert_file_row(
                    db,
                    file_id="direct-file",
                    path="C:/fake/direct-file.dat",
                    source_kind="msgattach_image_dat",
                    status="pending",
                    first_seen=1001.0,
                    last_error=None,
                )

                with patch("wechat_receipt_daemon.time.time", return_value=1010.0):
                    claimed = db.claim_next()

                self.assertIsNotNone(claimed)
                self.assertEqual(claimed.file_id, "direct-file")
            finally:
                db.close()

    def test_seed_ready_manual_session_placeholders_only_within_burst_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                with patch("wechat_receipt_daemon.time.time", return_value=10.0):
                    session = db.start_or_extend_manual_order_session(
                        talker="27837425841@chatroom",
                        create_time=100.0,
                        event_ts=10.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                    )
                self.assertIsNotNone(session)
                with patch("wechat_receipt_daemon.time.time", return_value=11.0):
                    db.start_or_extend_manual_order_session(
                        talker="27837425841@chatroom",
                        create_time=104.0,
                        event_ts=11.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                        preferred_session_id=str(session["session_id"]),
                    )

                resolver = FakeMediaResolver(
                    [
                        WeChatMessageRef(
                            msg_svr_id="msg-99",
                            talker="27837425841@chatroom",
                            create_time=99.0,
                            sender_user_name=None,
                            sender_display=None,
                            image_rel_path=None,
                            thumb_rel_path=None,
                            image_abs_path=Path("C:/fake/msg-99.dat"),
                            thumb_abs_path=None,
                        ),
                        WeChatMessageRef(
                            msg_svr_id="msg-102",
                            talker="27837425841@chatroom",
                            create_time=102.0,
                            sender_user_name=None,
                            sender_display=None,
                            image_rel_path=None,
                            thumb_rel_path=None,
                            image_abs_path=Path("C:/fake/msg-102.dat"),
                            thumb_abs_path=None,
                        ),
                    ]
                )

                class Cfg:
                    manual_order_guard_enabled = True

                seeded = seed_ready_manual_session_placeholders(db, resolver, Cfg())

                self.assertEqual(seeded, 1)
                self.assertIsNone(db.get_message_job("msg-99"))
                placeholder = db.get_message_job("msg-102")
                self.assertIsNotNone(placeholder)
                self.assertEqual(placeholder["state"], SESSION_PENDING_OPEN_STATE)
                self.assertEqual(placeholder["manual_session_id"], session["session_id"])
            finally:
                db.close()

    def test_new_talker_rolls_previous_session_placeholders_and_releases_file_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                with patch("wechat_receipt_daemon.time.time", return_value=10.0):
                    first_session = db.start_or_extend_manual_order_session(
                        talker="27837425841@chatroom",
                        create_time=100.0,
                        event_ts=10.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                    )
                self.assertIsNotNone(first_session)
                db.ensure_message_job(
                    msg_svr_id="old-msg",
                    talker="27837425841@chatroom",
                    talker_display="Grupo 65",
                    thumb_path=None,
                    expected_image_path=Path("C:/fake/old-msg.dat"),
                    create_time=100.0,
                    first_seen_at=10.0,
                    manual_session_id=str(first_session["session_id"]),
                    state=SESSION_PENDING_OPEN_STATE,
                    activation_seen_at=0.0,
                )
                insert_file_row(
                    db,
                    file_id="held-file",
                    path="C:/fake/held-file.dat",
                    source_kind="msgattach_image_dat",
                    status="retry",
                    first_seen=12.0,
                    last_error="WAITING_SESSION_PRIOR_MESSAGE_ORDER:old-msg",
                    msg_svr_id="new-msg",
                    talker="27837425841@chatroom",
                    msg_create_time=101.0,
                    manual_session_id=str(first_session["session_id"]),
                )

                with patch("wechat_receipt_daemon.time.time", return_value=20.0):
                    second_session = db.start_or_extend_manual_order_session(
                        talker="wxid_other_chat",
                        create_time=200.0,
                        event_ts=20.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                    )

                self.assertIsNotNone(second_session)
                self.assertNotEqual(first_session["session_id"], second_session["session_id"])
                rolled_job = db.get_message_job("old-msg")
                self.assertIsNotNone(rolled_job)
                self.assertEqual(rolled_job["state"], IGNORED_SESSION_ROLLOVER_STATE)
                held_file = db.get_file("held-file")
                self.assertIsNotNone(held_file)
                self.assertEqual(held_file["status"], "retry")
                self.assertEqual(held_file["last_error"], IGNORED_SESSION_ROLLOVER_STATE)
            finally:
                db.close()

    def test_sink_claim_waits_for_prior_session_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.ensure_message_job(
                    msg_svr_id="older-msg",
                    talker="27837425841@chatroom",
                    talker_display="Grupo 65",
                    thumb_path=None,
                    expected_image_path=Path("C:/fake/older-msg.dat"),
                    create_time=300.0,
                    first_seen_at=1000.0,
                    manual_session_id="session-a",
                    state=SESSION_PENDING_OPEN_STATE,
                    activation_seen_at=0.0,
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="newer-file",
                        ingested_at=2000.0,
                        msg_svr_id="newer-msg",
                        msg_create_time=301.0,
                        amount=668.04,
                        amount_rounded=668.0,
                        manual_session_id="session-a",
                    )
                )

                claimed = db.claim_next_sink_receipt(manual_session_id="session-a")

                self.assertIsNone(claimed)
                row = db._conn.execute(
                    """
                    SELECT sheet_status, sheet_last_error
                    FROM receipts
                    WHERE file_id='newer-file'
                    """
                ).fetchone()
                self.assertEqual(row["sheet_status"], "SINK_BLOCKED_PRIOR_MSG")
                self.assertEqual(row["sheet_last_error"], "WAITING_PRIOR_SINK_SESSION_MESSAGE:older-msg")
            finally:
                db.close()


class ManualOpenOnlyCleanupTests(unittest.TestCase):
    def test_cleanup_ignores_only_legacy_thumb_and_temp_waits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="thumb-wait",
                    path="C:/fake/thumb-wait_t.dat",
                    source_kind="msgattach_thumb_dat",
                    status="retry",
                    first_seen=1000.0,
                    last_error="MANUAL_WAIT_ORIGINAL",
                )
                insert_file_row(
                    db,
                    file_id="temp-wait",
                    path="C:/fake/temp-wait.png",
                    source_kind="temp_image",
                    status="pending",
                    first_seen=1000.0,
                    last_error="WAITING_TEMP_CONTEXT",
                )
                insert_file_row(
                    db,
                    file_id="image-keep",
                    path="C:/fake/image-keep.dat",
                    source_kind="msgattach_image_dat",
                    status="retry",
                    first_seen=1000.0,
                    last_error="WAITING_ORIGINAL_MEDIA",
                )
                insert_file_row(
                    db,
                    file_id="temp-keep",
                    path="C:/fake/temp-keep.png",
                    source_kind="temp_image",
                    status="retry",
                    first_seen=1000.0,
                    last_error="OTHER_REASON",
                )

                ignored = db.ignore_manual_open_only_waits()

                self.assertEqual(ignored, 2)
                rows = db._conn.execute(
                    """
                    SELECT file_id, status, last_error
                    FROM files
                    ORDER BY file_id ASC
                    """
                ).fetchall()
                mapped = {row["file_id"]: (row["status"], row["last_error"]) for row in rows}
                self.assertEqual(mapped["thumb-wait"], ("ignored", "IGNORED_MANUAL_OPEN_ONLY"))
                self.assertEqual(mapped["temp-wait"], ("ignored", "IGNORED_MANUAL_OPEN_ONLY"))
                self.assertEqual(mapped["image-keep"], ("retry", "WAITING_ORIGINAL_MEDIA"))
                self.assertEqual(mapped["temp-keep"], ("retry", "OTHER_REASON"))
            finally:
                db.close()


class WeChatDBResolverMergeRunnerTests(unittest.TestCase):
    def test_parse_merge_runner_output_reads_prefixed_json(self) -> None:
        output = "\n".join(
            [
                "warning line",
                "__WXMERGE__{\"code\": true, \"ret\": \"C:/tmp/merge.db\"}",
            ]
        )

        payload = WeChatDBResolver._parse_merge_runner_output(output)

        self.assertEqual(payload, {"code": True, "ret": "C:/tmp/merge.db"})

    def test_parse_merge_runner_output_returns_none_without_marker(self) -> None:
        self.assertIsNone(WeChatDBResolver._parse_merge_runner_output("warning only"))

    def _build_resolver(self, tmp_dir: str) -> WeChatDBResolver:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver.watch_roots = []
        resolver.wx_dirs = []
        resolver.wechat_root = None
        resolver.merge_path = Path(tmp_dir) / "merge.db"
        resolver.refresh_seconds = 10
        resolver.merge_timeout_seconds = 12
        resolver.failure_backoff_seconds = 60
        resolver._pywxdump = object()
        resolver._decode_bytes_extra = object()
        resolver._wx_key = "key"
        resolver._wx_dir = Path(tmp_dir)
        resolver._last_refresh = 0.0
        resolver._last_failure = 0.0
        resolver._last_error = None
        resolver._lock = threading.Lock()
        resolver._load_account_info = lambda force=False: True
        return resolver

    def test_refresh_if_due_backs_off_after_failed_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = self._build_resolver(tmp_dir)
            merge_calls: list[str] = []

            def fake_merge() -> tuple[bool, str]:
                merge_calls.append("merge")
                return False, "timeout"

            resolver._merge_real_time_db_with_timeout = fake_merge

            with patch("wechat_receipt_daemon.time.time", side_effect=[100.0, 105.0, 170.0]):
                self.assertFalse(resolver.refresh_if_due())
                self.assertFalse(resolver.refresh_if_due())
                self.assertFalse(resolver.refresh_if_due())

            self.assertEqual(merge_calls, ["merge", "merge"])
            self.assertEqual(resolver.last_error, "merge_failed:timeout")
            self.assertEqual(resolver._last_failure, 170.0)

    def test_refresh_if_due_clears_failure_after_successful_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = self._build_resolver(tmp_dir)
            merge_calls: list[str] = []
            results = [(False, "timeout"), (True, str(resolver.merge_path))]

            def fake_merge() -> tuple[bool, str]:
                merge_calls.append("merge")
                ok, ret = results.pop(0)
                if ok:
                    resolver.merge_path.write_text("ok", encoding="utf-8")
                return ok, ret

            resolver._merge_real_time_db_with_timeout = fake_merge

            with patch("wechat_receipt_daemon.time.time", side_effect=[100.0, 170.0, 175.0]):
                self.assertFalse(resolver.refresh_if_due())
                self.assertTrue(resolver.refresh_if_due())
                self.assertTrue(resolver.refresh_if_due())

            self.assertEqual(merge_calls, ["merge", "merge"])
            self.assertEqual(resolver._last_failure, 0.0)
            self.assertIsNone(resolver.last_error)


class StaleProcessingRecoveryTests(unittest.TestCase):
    def test_requeues_processing_row_without_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="stale-processing",
                    path="C:/fake/stale-processing.dat",
                    source_kind="msgattach_image_dat",
                    status="processing",
                    first_seen=100.0,
                    last_error=None,
                )

                with patch("wechat_receipt_daemon.time.time", return_value=500.0):
                    retry_count, done_count = db.recover_stale_processing(max_age_sec=120)

                self.assertEqual((retry_count, done_count), (1, 0))
                row = db.get_file("stale-processing")
                self.assertIsNotNone(row)
                self.assertEqual(row["status"], "retry")
                self.assertEqual(row["last_error"], "RECOVERED_STALE_PROCESSING")
                self.assertEqual(row["next_attempt"], 500.0)
            finally:
                db.close()

    def test_marks_processing_row_done_when_receipt_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="stale-with-receipt",
                    path="C:/fake/stale-with-receipt.dat",
                    source_kind="msgattach_image_dat",
                    status="processing",
                    first_seen=100.0,
                    last_error=None,
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="stale-with-receipt",
                        ingested_at=230.0,
                        msg_svr_id="msg-1",
                        msg_create_time=200.0,
                        amount=668.0,
                        amount_rounded=668.0,
                    )
                )

                with patch("wechat_receipt_daemon.time.time", return_value=500.0):
                    retry_count, done_count = db.recover_stale_processing(max_age_sec=120)

                self.assertEqual((retry_count, done_count), (0, 1))
                row = db.get_file("stale-with-receipt")
                self.assertIsNotNone(row)
                self.assertEqual(row["status"], "done")
                self.assertEqual(row["last_error"], "RECOVERED_PROCESSING_WITH_RECEIPT")
                self.assertEqual(row["processed_at"], 230.0)
            finally:
                db.close()


class RecordingSink:
    def __init__(self) -> None:
        self.updated_rows: list[tuple[str, int, dict[str, object], bool]] = []

    def append(self, row_payload: dict[str, object], review_needed: bool) -> tuple[str, int]:
        raise NotImplementedError

    def update_row(self, sheet_name: str, row_idx: int, row_payload: dict[str, object], review_needed: bool) -> None:
        self.updated_rows.append((sheet_name, row_idx, row_payload, review_needed))


class GoogleSheetsSinkTargetSheetTests(unittest.TestCase):
    def test_review_items_use_review_sheet_when_configured(self) -> None:
        sink = GoogleSheetsSink.__new__(GoogleSheetsSink)
        sink.review_worksheet = "Revisar"
        sink._main_sheet_title = "Pagina1"

        self.assertEqual(sink._target_sheet(review_needed=False), "Pagina1")
        self.assertEqual(sink._target_sheet(review_needed=True), "Revisar")

    def test_review_items_fall_back_to_main_sheet_when_review_sheet_matches_main(self) -> None:
        sink = GoogleSheetsSink.__new__(GoogleSheetsSink)
        sink.review_worksheet = "Pagina1"
        sink._main_sheet_title = "Pagina1"

        self.assertEqual(sink._target_sheet(review_needed=True), "Pagina1")

    def test_review_items_use_main_sheet_when_review_sheet_disabled(self) -> None:
        sink = GoogleSheetsSink.__new__(GoogleSheetsSink)
        sink.review_worksheet = None
        sink._main_sheet_title = "Pagina1"

        self.assertEqual(sink._target_sheet(review_needed=True), "Pagina1")


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
