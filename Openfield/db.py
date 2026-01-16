import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime

DB_NAME = "openfield.db"


def _db_path() -> str:
    base = os.path.dirname(__file__)
    return os.path.join(base, DB_NAME)


@contextmanager
def get_conn():
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _columns(conn: sqlite3.Connection, table: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, col_def: str):
    cols = _columns(conn, table)
    if col not in cols:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
        conn.commit()


def init_db():
    """
    Creates tables if missing and applies safe schema migrations (ALTER TABLE) if you upgrade code later.
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # ---------------- Facilities ----------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS facilities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                facility_type TEXT,
                address TEXT,
                lga TEXT,
                state TEXT,
                contact_name TEXT,
                contact_phone TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # ---------------- Survey Templates ----------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS survey_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                created_at TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS template_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                question_text TEXT NOT NULL,
                question_type TEXT NOT NULL DEFAULT 'TEXT',   -- TEXT | YESNO | NUMBER
                order_no INTEGER NOT NULL DEFAULT 1,
                is_required INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (template_id) REFERENCES survey_templates(id)
            )
        """)

        # ---------------- Surveys ----------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS surveys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                facility_id INTEGER NOT NULL,
                template_id INTEGER,
                survey_type TEXT NOT NULL,
                enumerator_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'DRAFT',        -- DRAFT | COMPLETED
                created_at TEXT NOT NULL,
                FOREIGN KEY (facility_id) REFERENCES facilities(id),
                FOREIGN KEY (template_id) REFERENCES survey_templates(id)
            )
        """)

        # ---------------- Survey Answers ----------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS survey_answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                survey_id INTEGER NOT NULL,
                template_question_id INTEGER,
                question TEXT NOT NULL,
                answer TEXT,
                answer_source TEXT,                          -- OBSERVATION | INTERVIEW | RECORD | ESTIMATE
                confidence_level TEXT,                       -- HIGH | MEDIUM | LOW
                is_missing INTEGER NOT NULL DEFAULT 0,
                missing_reason TEXT,                         -- NOT_APPLICABLE | REFUSED | UNAVAILABLE | UNSURE | TIME_CONSTRAINT
                created_at TEXT NOT NULL,
                FOREIGN KEY (survey_id) REFERENCES surveys(id),
                FOREIGN KEY (template_question_id) REFERENCES template_questions(id)
            )
        """)

        # ---------------- Indexes ----------------
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities(name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_facility ON surveys(facility_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_status ON surveys(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_enum ON surveys(enumerator_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_answers_survey ON survey_answers(survey_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tplq_tpl ON template_questions(template_id)")

        conn.commit()

        # ---------------- Safe migrations (if older DB exists) ----------------
        # facilities
        for col, col_def in [
            ("facility_type", "TEXT"),
            ("address", "TEXT"),
            ("lga", "TEXT"),
            ("state", "TEXT"),
            ("contact_name", "TEXT"),
            ("contact_phone", "TEXT"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ]:
            _add_column_if_missing(conn, "facilities", col, col_def)

        # survey_templates
        for col, col_def in [
            ("description", "TEXT"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ]:
            _add_column_if_missing(conn, "survey_templates", col, col_def)

        # template_questions
        for col, col_def in [
            ("question_type", "TEXT NOT NULL DEFAULT 'TEXT'"),
            ("order_no", "INTEGER NOT NULL DEFAULT 1"),
            ("is_required", "INTEGER NOT NULL DEFAULT 0"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ]:
            _add_column_if_missing(conn, "template_questions", col, col_def)

        # surveys
        for col, col_def in [
            ("template_id", "INTEGER"),
            ("status", "TEXT NOT NULL DEFAULT 'DRAFT'"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ]:
            _add_column_if_missing(conn, "surveys", col, col_def)

        # survey_answers
        for col, col_def in [
            ("template_question_id", "INTEGER"),
            ("answer_source", "TEXT"),
            ("confidence_level", "TEXT"),
            ("is_missing", "INTEGER NOT NULL DEFAULT 0"),
            ("missing_reason", "TEXT"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ]:
            _add_column_if_missing(conn, "survey_answers", col, col_def)

        # Backfill created_at if empty
        now = _now_iso()
        cur = conn.cursor()
        cur.execute("UPDATE facilities SET created_at=? WHERE created_at IS NULL OR created_at=''", (now,))
        cur.execute("UPDATE survey_templates SET created_at=? WHERE created_at IS NULL OR created_at=''", (now,))
        cur.execute("UPDATE template_questions SET created_at=? WHERE created_at IS NULL OR created_at=''", (now,))
        cur.execute("UPDATE surveys SET created_at=? WHERE created_at IS NULL OR created_at=''", (now,))
        cur.execute("UPDATE survey_answers SET created_at=? WHERE created_at IS NULL OR created_at=''", (now,))
        conn.commit()