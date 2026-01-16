import csv
import json
from typing import List, Dict, Any

from db import get_conn


def export_facilities_csv(filepath: str) -> str:
    """
    Export all facilities to CSV.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, facility_type, address, lga, state,
                   contact_name, contact_phone, created_at
            FROM facilities
            ORDER BY id ASC
        """)
        rows = cur.fetchall()

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "name", "facility_type", "address",
            "lga", "state", "contact_name", "contact_phone", "created_at"
        ])
        for r in rows:
            writer.writerow([
                r["id"], r["name"], r["facility_type"], r["address"],
                r["lga"], r["state"], r["contact_name"],
                r["contact_phone"], r["created_at"]
            ])

    return filepath


def export_surveys_answers_csv(filepath: str) -> str:
    """
    Export surveys and their answers to a flat CSV.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                s.id AS survey_id,
                f.name AS facility_name,
                s.template_id,
                s.survey_type,
                s.enumerator_name,
                s.status,
                s.created_at AS survey_created_at,
                a.id AS answer_id,
                a.template_question_id,
                a.question,
                a.answer,
                a.answer_source,
                a.confidence_level,
                a.is_missing,
                a.missing_reason,
                a.created_at AS answer_created_at
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            LEFT JOIN survey_answers a ON a.survey_id = s.id
            ORDER BY s.id ASC, a.id ASC
        """)
        rows = cur.fetchall()

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "survey_id", "facility_name", "template_id",
            "survey_type", "enumerator_name", "status",
            "survey_created_at", "answer_id",
            "template_question_id", "question", "answer",
            "answer_source", "confidence_level",
            "is_missing", "missing_reason",
            "answer_created_at"
        ])

        for r in rows:
            writer.writerow([
                r["survey_id"], r["facility_name"], r["template_id"],
                r["survey_type"], r["enumerator_name"], r["status"],
                r["survey_created_at"], r["answer_id"],
                r["template_question_id"], r["question"], r["answer"],
                r["answer_source"], r["confidence_level"],
                r["is_missing"], r["missing_reason"],
                r["answer_created_at"]
            ])

    return filepath


def export_surveys_answers_json(filepath: str, limit: int = 1000) -> str:
    """
    Export surveys and answers to structured JSON.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                s.id AS survey_id,
                s.facility_id,
                f.name AS facility_name,
                s.template_id,
                s.survey_type,
                s.enumerator_name,
                s.status,
                s.created_at
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            ORDER BY s.id DESC
            LIMIT ?
        """, (int(limit),))
        surveys = cur.fetchall()

        output: List[Dict[str, Any]] = []

        for s in surveys:
            sid = int(s["survey_id"])
            cur.execute("""
                SELECT id, template_question_id, question, answer,
                       answer_source, confidence_level,
                       is_missing, missing_reason, created_at
                FROM survey_answers
                WHERE survey_id = ?
                ORDER BY id ASC
            """, (sid,))
            answers = cur.fetchall()

            output.append({
                "survey": {
                    "id": sid,
                    "facility_id": int(s["facility_id"]),
                    "facility_name": s["facility_name"],
                    "template_id": s["template_id"],
                    "survey_type": s["survey_type"],
                    "enumerator_name": s["enumerator_name"],
                    "status": s["status"],
                    "created_at": s["created_at"]
                },
                "answers": [
                    {
                        "id": int(a["id"]),
                        "template_question_id": a["template_question_id"],
                        "question": a["question"],
                        "answer": a["answer"],
                        "answer_source": a["answer_source"],
                        "confidence_level": a["confidence_level"],
                        "is_missing": int(a["is_missing"] or 0),
                        "missing_reason": a["missing_reason"],
                        "created_at": a["created_at"]
                    }
                    for a in answers
                ]
            })

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return filepath


def export_one_survey_json(filepath: str, survey_id: int) -> str:
    """
    Export a single survey (header + answers) to JSON.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                s.id AS survey_id,
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
        """, (int(survey_id),))
        s = cur.fetchone()
        if not s:
            raise ValueError("Survey not found.")

        cur.execute("""
            SELECT id, template_question_id, question, answer,
                   answer_source, confidence_level,
                   is_missing, missing_reason, created_at
            FROM survey_answers
            WHERE survey_id = ?
            ORDER BY id ASC
        """, (int(survey_id),))
        answers = cur.fetchall()

    data = {
        "survey": {
            "id": int(s["survey_id"]),
            "facility_id": int(s["facility_id"]),
            "facility_name": s["facility_name"],
            "template_id": s["template_id"],
            "survey_type": s["survey_type"],
            "enumerator_name": s["enumerator_name"],
            "status": s["status"],
            "created_at": s["created_at"]
        },
        "answers": [
            {
                "id": int(a["id"]),
                "template_question_id": a["template_question_id"],
                "question": a["question"],
                "answer": a["answer"],
                "answer_source": a["answer_source"],
                "confidence_level": a["confidence_level"],
                "is_missing": int(a["is_missing"] or 0),
                "missing_reason": a["missing_reason"],
                "created_at": a["created_at"]
            }
            for a in answers
        ]
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return filepath