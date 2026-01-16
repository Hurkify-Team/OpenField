from datetime import datetime
from typing import Optional, List, Tuple, Any, Dict

from db import get_conn


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _table_columns(conn, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [r[1] for r in cur.fetchall()]


def create_survey(
    facility_id: int,
    survey_type: str,
    enumerator_name: str,
    template_id: Optional[int] = None
) -> int:
    """
    Creates a survey header row.
    Status defaults to DRAFT.
    template_id can be None (manual) or int (template-driven).
    """
    with get_conn() as conn:
        cur = conn.cursor()

        cols = _table_columns(conn, "surveys")
        if "template_id" not in cols:
            # If DB is older, still create survey without template_id
            cur.execute("""
                INSERT INTO surveys (facility_id, survey_type, enumerator_name, status, created_at)
                VALUES (?, ?, ?, 'DRAFT', ?)
            """, (int(facility_id), survey_type, enumerator_name, _now_iso()))
            conn.commit()
            return int(cur.lastrowid)

        cur.execute("""
            INSERT INTO surveys (facility_id, template_id, survey_type, enumerator_name, status, created_at)
            VALUES (?, ?, ?, ?, 'DRAFT', ?)
        """, (int(facility_id), template_id, survey_type, enumerator_name, _now_iso()))
        conn.commit()
        return int(cur.lastrowid)


def add_answer(
    survey_id: int,
    question: str,
    answer: str,
    template_question_id: Optional[int] = None,
    answer_source: Optional[str] = None,
    confidence_level: Optional[str] = None,
    is_missing: int = 0,
    missing_reason: Optional[str] = None,
) -> int:
    """
    Adds an answer row.

    Backward compatible:
      add_answer(sid, "Q", "A") works for manual surveys.

    Template-aware:
      add_answer(sid, "Q", "A", template_question_id=1, answer_source="INTERVIEW",
                 confidence_level="HIGH", is_missing=0, missing_reason=None)
    """
    q = (question or "").strip()
    a = (answer or "").strip()
    if not q:
        raise ValueError("question is required")
    if a == "" and not is_missing:
        raise ValueError("answer is required unless is_missing=1")

    with get_conn() as conn:
        cur = conn.cursor()
        cols = _table_columns(conn, "survey_answers")

        # Build dynamic insert based on existing schema
        fields = ["survey_id", "question", "answer", "created_at"]
        values: List[Any] = [int(survey_id), q, a, _now_iso()]

        if "template_question_id" in cols:
            fields.append("template_question_id")
            values.append(template_question_id)

        if "answer_source" in cols:
            fields.append("answer_source")
            values.append(answer_source)

        if "confidence_level" in cols:
            fields.append("confidence_level")
            values.append(confidence_level)

        if "is_missing" in cols:
            fields.append("is_missing")
            values.append(int(is_missing))

        if "missing_reason" in cols:
            fields.append("missing_reason")
            values.append(missing_reason)

        placeholders = ",".join(["?"] * len(fields))
        sql = f"INSERT INTO survey_answers ({','.join(fields)}) VALUES ({placeholders})"
        cur.execute(sql, values)
        conn.commit()
        return int(cur.lastrowid)


def complete_survey(survey_id: int) -> None:
    """
    Marks a survey COMPLETED.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE surveys
            SET status='COMPLETED'
            WHERE id = ?
        """, (int(survey_id),))
        conn.commit()


def list_surveys(limit: int = 50) -> List[Tuple]:
    """
    Returns latest surveys (simple list).
    Note: supervision.filter_surveys is the richer query; this is kept for compatibility.
    """
    with get_conn() as conn:
        cur = conn.cursor()

        cols = _table_columns(conn, "surveys")
        if "template_id" in cols:
            cur.execute("""
                SELECT s.id, f.name, s.template_id, s.survey_type, s.enumerator_name, s.status, s.created_at
                FROM surveys s
                JOIN facilities f ON f.id = s.facility_id
                ORDER BY s.id DESC
                LIMIT ?
            """, (int(limit),))
        else:
            cur.execute("""
                SELECT s.id, f.name, NULL as template_id, s.survey_type, s.enumerator_name, s.status, s.created_at
                FROM surveys s
                JOIN facilities f ON f.id = s.facility_id
                ORDER BY s.id DESC
                LIMIT ?
            """, (int(limit),))

        return cur.fetchall()