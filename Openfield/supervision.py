from typing import List, Tuple, Optional, Dict, Any

from db import get_conn


def filter_surveys(
    status: Optional[str] = None,
    template_id: Optional[int] = None,
    enumerator: Optional[str] = None,
    limit: int = 50
) -> List[Tuple]:
    """
    Returns tuples:
      (survey_id, facility_name, template_id, survey_type, enumerator_name, status, created_at)

    This is used by:
      - API: GET /surveys
      - UI: /ui/surveys
    """
    where = []
    params = []

    if status:
        where.append("s.status = ?")
        params.append(status.strip().upper())

    if template_id:
        where.append("s.template_id = ?")
        params.append(int(template_id))

    if enumerator:
        where.append("s.enumerator_name LIKE ?")
        params.append(f"%{enumerator.strip()}%")

    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT s.id,
                   f.name AS facility_name,
                   s.template_id,
                   s.survey_type,
                   s.enumerator_name,
                   s.status,
                   s.created_at
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            {where_sql}
            ORDER BY s.id DESC
            LIMIT ?
            """,
            (*params, int(limit)),
        )
        rows = cur.fetchall()

        return [
            (
                int(r["id"]),
                r["facility_name"],
                r["template_id"],
                r["survey_type"],
                r["enumerator_name"],
                r["status"],
                r["created_at"],
            )
            for r in rows
        ]


def get_survey_details(survey_id: int):
    """
    Returns:
      header tuple:
        (sid, facility_id, facility_name, template_id, survey_type, enumerator, status, created_at)

      answers list of tuples:
        (answer_id, template_question_id, question, answer, answer_source, confidence_level, is_missing, missing_reason)

      qa dict:
        total_answers, missing_count, low_confidence_count, no_source_count, no_confidence_count
    """
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT s.id,
                   s.facility_id,
                   f.name AS facility_name,
                   s.template_id,
                   s.survey_type,
                   s.enumerator_name,
                   s.status,
                   s.created_at
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            WHERE s.id = ?
            """,
            (int(survey_id),),
        )
        h = cur.fetchone()
        if not h:
            raise ValueError("Survey not found.")

        header = (
            int(h["id"]),
            int(h["facility_id"]),
            h["facility_name"],
            h["template_id"],
            h["survey_type"],
            h["enumerator_name"],
            h["status"],
            h["created_at"],
        )

        cur.execute(
            """
            SELECT id,
                   template_question_id,
                   question,
                   answer,
                   answer_source,
                   confidence_level,
                   is_missing,
                   missing_reason
            FROM survey_answers
            WHERE survey_id = ?
            ORDER BY id ASC
            """,
            (int(survey_id),),
        )
        answers_rows = cur.fetchall()

        answers = []
        for r in answers_rows:
            answers.append(
                (
                    int(r["id"]),
                    r["template_question_id"],
                    r["question"],
                    r["answer"],
                    r["answer_source"],
                    r["confidence_level"],
                    int(r["is_missing"] or 0),
                    r["missing_reason"],
                )
            )

    qa = {
        "total_answers": len(answers),
        "missing_count": sum(1 for a in answers if a[6] == 1),
        "low_confidence_count": sum(1 for a in answers if (a[5] or "").strip().upper() == "LOW"),
        "no_source_count": sum(1 for a in answers if not (a[4] or "").strip()),
        "no_confidence_count": sum(1 for a in answers if not (a[5] or "").strip()),
    }

    return header, answers, qa


def qa_alerts_dashboard(
    status_filter: str = "COMPLETED",
    missing_threshold_pct: float = 20.0,
    low_conf_threshold_pct: float = 20.0,
    no_source_threshold_pct: float = 10.0,
    no_conf_threshold_pct: float = 10.0,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Produces a list of flagged surveys with severity score.

    Output dict fields:
      survey_id, facility_id, facility_name, template_id, survey_type, enumerator_name,
      status, created_at, total_answers, flags, severity
    """
    status_filter = (status_filter or "COMPLETED").strip().upper()

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, facility_id, template_id, survey_type, enumerator_name, status, created_at
            FROM surveys
            WHERE status = ?
            ORDER BY id DESC
            """,
            (status_filter,),
        )
        surveys = cur.fetchall()

        alerts = []

        for s in surveys:
            sid = int(s["id"])

            cur.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(is_missing) AS missing,
                       SUM(CASE WHEN confidence_level='LOW' THEN 1 ELSE 0 END) AS low_conf,
                       SUM(CASE WHEN answer_source IS NULL OR answer_source='' THEN 1 ELSE 0 END) AS no_source,
                       SUM(CASE WHEN confidence_level IS NULL OR confidence_level='' THEN 1 ELSE 0 END) AS no_conf
                FROM survey_answers
                WHERE survey_id = ?
                """,
                (sid,),
            )
            m = cur.fetchone()

            total = int(m["total"] or 0)
            if total == 0:
                continue

            missing = int(m["missing"] or 0)
            low_conf = int(m["low_conf"] or 0)
            no_source = int(m["no_source"] or 0)
            no_conf = int(m["no_conf"] or 0)

            missing_pct = (missing * 100.0) / total
            low_conf_pct = (low_conf * 100.0) / total
            no_source_pct = (no_source * 100.0) / total
            no_conf_pct = (no_conf * 100.0) / total

            flags = []
            if missing_pct >= float(missing_threshold_pct):
                flags.append("MISSING")
            if low_conf_pct >= float(low_conf_threshold_pct):
                flags.append("LOW_CONF")
            if no_source_pct >= float(no_source_threshold_pct):
                flags.append("NO_SOURCE")
            if no_conf_pct >= float(no_conf_threshold_pct):
                flags.append("NO_CONF")

            if not flags:
                continue

            cur.execute("SELECT name FROM facilities WHERE id=? LIMIT 1", (int(s["facility_id"]),))
            frow = cur.fetchone()
            facility_name = frow["name"] if frow else "-"

            severity = missing_pct + low_conf_pct + no_source_pct + no_conf_pct

            alerts.append({
                "survey_id": sid,
                "facility_id": int(s["facility_id"]),
                "facility_name": facility_name,
                "template_id": s["template_id"],
                "survey_type": s["survey_type"],
                "enumerator_name": s["enumerator_name"],
                "status": s["status"],
                "created_at": s["created_at"],
                "total_answers": total,
                "flags": flags,
                "severity": float(severity)
            })

        alerts.sort(key=lambda x: x["severity"], reverse=True)
        return alerts[:int(limit)]