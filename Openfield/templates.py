from datetime import datetime
from typing import List, Tuple, Optional

from db import get_conn


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _table_columns(table: str) -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [r[1] for r in cur.fetchall()]


def create_template(name: str, description: str = "") -> int:
    """
    Create a survey template.
    If a template with the same name already exists, returns its ID.
    """
    n = (name or "").strip()
    if not n:
        raise ValueError("Template name is required.")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO survey_templates (name, description, created_at)
            VALUES (?, ?, ?)
            """,
            (n, (description or "").strip(), _now_iso()),
        )
        conn.commit()

        cur.execute("SELECT id FROM survey_templates WHERE name = ? LIMIT 1", (n,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Failed to create or retrieve template.")
        return int(row["id"])


def list_templates(limit: int = 100) -> List[Tuple[int, str, str, str]]:
    """
    Returns: (id, name, description, created_at)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, description, created_at
            FROM survey_templates
            ORDER BY name ASC
            LIMIT ?
            """,
            (int(limit),),
        )
        return [(int(r["id"]), r["name"], r["description"], r["created_at"]) for r in cur.fetchall()]


def add_template_question(
    template_id: int,
    question_text: str,
    question_type: str = "TEXT",
    order_no: int = 1,
    is_required: int = 0,
) -> int:
    """
    Adds a question to a template.
    Handles both schemas:
      - order_no
      - display_order (NOT NULL in some older versions)
    """
    q = (question_text or "").strip()
    if not q:
        raise ValueError("question_text is required.")

    qt = (question_type or "TEXT").strip().upper()
    if qt not in ("TEXT", "YESNO", "NUMBER"):
        qt = "TEXT"

    cols = _table_columns("template_questions")
    order_col = "display_order" if "display_order" in cols else "order_no"

    # Some older schemas might name these differently; we keep the main ones.
    required_col = "is_required" if "is_required" in cols else None
    type_col = "question_type" if "question_type" in cols else None

    # Build a column-safe insert
    fields = ["template_id", "question_text", order_col, "created_at"]
    values = [int(template_id), q, int(order_no), _now_iso()]

    if type_col:
        fields.insert(2, type_col)
        values.insert(2, qt)

    if required_col:
        fields.append(required_col)
        values.append(int(is_required))

    placeholders = ",".join(["?"] * len(fields))
    sql = f"INSERT INTO template_questions ({','.join(fields)}) VALUES ({placeholders})"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        return int(cur.lastrowid)


def get_template_questions(template_id: int) -> List[Tuple[int, str, str, int, int]]:
    """
    Returns a list of template questions in the exact shape required by app.py:
      (id, question_text, question_type, order_no, is_required)

    If DB uses display_order, we map it into order_no in output.
    """
    cols = _table_columns("template_questions")
    order_col = "display_order" if "display_order" in cols else "order_no"

    # In case a legacy schema doesn't store type/required, default them safely.
    has_type = "question_type" in cols
    has_required = "is_required" in cols

    select_cols = ["id", "question_text", order_col]
    if has_type:
        select_cols.append("question_type")
    else:
        select_cols.append("'TEXT' AS question_type")

    if has_required:
        select_cols.append("is_required")
    else:
        select_cols.append("0 AS is_required")

    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM template_questions
        WHERE template_id = ?
        ORDER BY {order_col} ASC, id ASC
    """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (int(template_id),))
        rows = cur.fetchall()

        out = []
        for r in rows:
            out.append((
                int(r["id"]),
                r["question_text"],
                (r["question_type"] or "TEXT").upper(),
                int(r[order_col]),
                int(r["is_required"] or 0),
            ))
        return out


def get_template(template_id: int) -> Optional[Tuple[int, str, str, str]]:
    """
    Returns: (id, name, description, created_at) or None
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, description, created_at
            FROM survey_templates
            WHERE id = ?
            """,
            (int(template_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (int(r["id"]), r["name"], r["description"], r["created_at"])