import os
import re
from datetime import datetime
from typing import List, Dict, Optional

from flask import Flask, request, jsonify, redirect, url_for, render_template_string, Response

from db import init_db, get_conn
from facilities import list_facilities, add_facility, get_facility_by_id
from templates import create_template, add_template_question, list_templates, get_template_questions
from surveys import create_survey, add_answer, complete_survey
from supervision import filter_surveys, get_survey_details, qa_alerts_dashboard

app = Flask(__name__)

# =========================
# Enumerator configuration
# =========================
ENUM_ALLOWED_SOURCES = ["OBSERVATION", "INTERVIEW", "RECORD", "ESTIMATE"]
ENUM_ALLOWED_CONF = ["HIGH", "MEDIUM", "LOW"]
ENUM_MISSING_REASONS = ["NOT_APPLICABLE", "REFUSED", "UNAVAILABLE", "UNSURE", "TIME_CONSTRAINT"]

# Professional, minimal "modes"
MODE_TEMPLATE = "TEMPLATE"
MODE_MANUAL = "MANUAL"


# =========================
# Utilities
# =========================
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _facility_name_suggestions(limit: int = 200) -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM facilities ORDER BY name ASC LIMIT ?", (int(limit),))
        return [r["name"] for r in cur.fetchall() if r and r["name"]]


def _get_or_create_facility_by_name(name: str) -> int:
    n = (name or "").strip()
    if not n:
        raise ValueError("Facility name is required.")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM facilities WHERE LOWER(name)=LOWER(?) LIMIT 1", (n,))
        row = cur.fetchone()
        if row:
            return int(row["id"])

    # Create if not existing
    return int(add_facility(name=n))


def _templates_quick_list(limit: int = 200):
    # templates.list_templates() returns (id, name, description, created_at)
    rows = list_templates(limit=limit)
    return [(tid, name) for (tid, name, desc, created) in rows]


def _seed_default_templates():
    """
    Creates a few built-in templates so enumerators can immediately select templates.
    If they already exist, they are reused.
    """
    defaults = [
        {
            "name": "Facility Assessment",
            "desc": "Standard facility assessment checklist for routine visits.",
            "questions": [
                ("Facility type (e.g., Hospital, Clinic, PHC)", "TEXT", 1, 1),
                ("Is the facility currently operational?", "YESNO", 2, 1),
                ("Number of staff currently on duty", "NUMBER", 3, 0),
                ("Top challenge observed today", "TEXT", 4, 0),
            ],
        },
        {
            "name": "Service Availability",
            "desc": "Quick service availability and basic capacity checks.",
            "questions": [
                ("Are essential medicines available today?", "YESNO", 1, 1),
                ("Is power supply available now?", "YESNO", 2, 1),
                ("Is clean water available today?", "YESNO", 3, 1),
                ("Average waiting time (minutes)", "NUMBER", 4, 0),
            ],
        },
        {
            "name": "Patient Experience Quick Check",
            "desc": "Short patient experience snapshot for service improvement.",
            "questions": [
                ("Were patients treated respectfully today?", "YESNO", 1, 1),
                ("Were fees clearly explained?", "YESNO", 2, 0),
                ("Main patient complaint (if any)", "TEXT", 3, 0),
            ],
        },
    ]

    with get_conn() as conn:
        cur = conn.cursor()

        # existing templates
        cur.execute("SELECT id, name FROM survey_templates")
        existing = {r["name"]: int(r["id"]) for r in cur.fetchall()}

        for t in defaults:
            if t["name"] in existing:
                tid = existing[t["name"]]
            else:
                tid = create_template(t["name"], t["desc"])

            # Only seed questions if template has none
            cur.execute("SELECT COUNT(*) AS c FROM template_questions WHERE template_id = ?", (int(tid),))
            count = int(cur.fetchone()["c"] or 0)
            if count > 0:
                continue

            for (qtext, qtype, order_no, required) in t["questions"]:
                add_template_question(int(tid), qtext, qtype, int(order_no), int(required))


def _validate_by_type(qtype: str, raw: str) -> str:
    qt = (qtype or "").strip().upper()
    v = (raw or "").strip()

    if qt == "YESNO":
        vlow = v.lower()
        if vlow in ("yes", "y", "true", "1"):
            return "YES"
        if vlow in ("no", "n", "false", "0"):
            return "NO"
        raise ValueError("Please choose YES or NO.")

    if qt == "NUMBER":
        # allow ints or floats
        float(v)
        return v

    if not v:
        raise ValueError("Response is required.")
    return v


def _template_answer_map(survey_id: int) -> Dict[int, Dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT template_question_id, id, answer, answer_source, confidence_level, is_missing, missing_reason
            FROM survey_answers
            WHERE survey_id = ? AND template_question_id IS NOT NULL
            """,
            (int(survey_id),),
        )
        m = {}
        for r in cur.fetchall():
            tqid = int(r["template_question_id"])
            m[tqid] = {
                "answer_id": int(r["id"]),
                "answer": r["answer"],
                "source": r["answer_source"],
                "confidence": r["confidence_level"],
                "is_missing": int(r["is_missing"] or 0),
                "missing_reason": r["missing_reason"],
            }
        return m


def _upsert_template_answer(
    survey_id: int,
    template_question_id: int,
    question_text: str,
    answer: str,
    source: Optional[str],
    confidence: Optional[str],
    is_missing: int,
    missing_reason: Optional[str],
):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM survey_answers
            WHERE survey_id = ? AND template_question_id = ?
            LIMIT 1
            """,
            (int(survey_id), int(template_question_id)),
        )
        row = cur.fetchone()

        if row:
            aid = int(row["id"])
            cur.execute(
                """
                UPDATE survey_answers
                SET question=?,
                    answer=?,
                    answer_source=?,
                    confidence_level=?,
                    is_missing=?,
                    missing_reason=?
                WHERE id=?
                """,
                (
                    question_text,
                    answer,
                    source,
                    confidence,
                    int(is_missing),
                    missing_reason,
                    aid,
                ),
            )
            conn.commit()
            return

    # Insert new
    add_answer(
        survey_id=survey_id,
        question=question_text,
        answer=answer,
        template_question_id=template_question_id,
        answer_source=source,
        confidence_level=confidence,
        is_missing=int(is_missing),
        missing_reason=missing_reason,
    )


def _find_next_unanswered_index(template_questions, answered_map: Dict[int, Dict]) -> int:
    # first: required unanswered
    for i, (qid, qtext, qtype, order_no, is_req) in enumerate(template_questions):
        if int(is_req) == 1 and int(qid) not in answered_map:
            return i
    # second: any unanswered
    for i, (qid, qtext, qtype, order_no, is_req) in enumerate(template_questions):
        if int(qid) not in answered_map:
            return i
    return 0


# =========================
# API root / health
# =========================
@app.get("/")
def home():
    return jsonify(
        {
            "name": "OpenField Collect API",
            "ui": "/ui",
            "enumerator_ui": "/enum",
            "endpoints": [
                "GET /facilities",
                "POST /facilities",
                "GET /facilities/<id>",
                "GET /surveys?status=&enumerator=&template_id=",
                "GET /surveys/<id>",
                "GET /qa/alerts",
            ],
        }
    )


@app.get("/favicon.ico")
def favicon():
    return Response(status=204)


# =========================
# API endpoints (minimal)
# =========================
@app.get("/facilities")
def api_facilities():
    limit = int(request.args.get("limit", 50))
    rows = list_facilities(limit=limit)
    data = []
    for fid, name, ftype, lga, state, created_at in rows:
        data.append(
            {
                "id": fid,
                "name": name,
                "facility_type": ftype,
                "lga": lga,
                "state": state,
                "created_at": created_at,
            }
        )
    return jsonify(data)


@app.post("/facilities")
def api_facilities_create():
    payload = request.get_json(force=True)
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    fid = add_facility(name=name)
    return jsonify({"id": fid}), 201


@app.get("/facilities/<int:fid>")
def api_facility_detail(fid):
    row = get_facility_by_id(fid)
    if not row:
        return jsonify({"error": "not found"}), 404
    (fid, name, ftype, address, lga, state, contact_name, contact_phone, created_at) = row
    return jsonify(
        {
            "id": fid,
            "name": name,
            "facility_type": ftype,
            "address": address,
            "lga": lga,
            "state": state,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "created_at": created_at,
        }
    )


@app.get("/surveys")
def api_surveys():
    status = request.args.get("status") or None
    enumerator = request.args.get("enumerator") or None
    template_id = request.args.get("template_id") or None
    limit = int(request.args.get("limit", 50))

    rows = filter_surveys(
        status=status,
        enumerator=enumerator,
        template_id=int(template_id) if (template_id and str(template_id).isdigit()) else None,
        limit=limit,
    )

    data = []
    for sid, facility_name, tid, survey_type, enum, st, created_at in rows:
        data.append(
            {
                "id": sid,
                "facility_name": facility_name,
                "template_id": tid,
                "survey_type": survey_type,
                "enumerator_name": enum,
                "status": st,
                "created_at": created_at,
            }
        )
    return jsonify(data)


@app.get("/surveys/<int:sid>")
def api_survey_detail(sid):
    header, answers, qa = get_survey_details(sid)
    (sid, fid, facility_name, template_id, survey_type, enumerator, status, created_at) = header
    return jsonify(
        {
            "survey": {
                "id": sid,
                "facility_id": fid,
                "facility_name": facility_name,
                "template_id": template_id,
                "survey_type": survey_type,
                "enumerator_name": enumerator,
                "status": status,
                "created_at": created_at,
            },
            "qa": qa,
            "answers": [
                {
                    "answer_id": a[0],
                    "template_question_id": a[1],
                    "question": a[2],
                    "answer": a[3],
                    "answer_source": a[4],
                    "confidence_level": a[5],
                    "is_missing": int(a[6] or 0),
                    "missing_reason": a[7],
                }
                for a in answers
            ],
        }
    )


@app.get("/qa/alerts")
def api_qa_alerts():
    alerts = qa_alerts_dashboard(limit=int(request.args.get("limit", 50)))
    return jsonify(alerts)


# =========================
# Supervisor UI (minimal)
# =========================
UI_BASE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .nav a { margin-right: 12px; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid #ddd; padding: 8px; font-size: 14px; }
    th { text-align: left; }
    .card { border: 1px solid #ddd; padding: 12px; border-radius: 10px; margin-top: 12px; }
    .muted { color: #666; }
    .ok { color: #0a7; font-weight: bold; }
  </style>
</head>
<body>
  <div class="nav">
    <a href="/ui">Dashboard</a>
    <a href="/ui/surveys">Surveys</a>
    <a href="/ui/qa">QA Alerts</a>
    <a href="/enum">Enumerator UI</a>
  </div>
  <hr>
  {{ body|safe }}
</body>
</html>
"""


def _ui(title: str, body: str, **ctx):
    return render_template_string(UI_BASE, title=title, body=render_template_string(body, **ctx))


@app.get("/ui")
def ui_dashboard():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM facilities")
        facilities_count = int(cur.fetchone()["c"] or 0)
        cur.execute("SELECT COUNT(*) AS c FROM surveys")
        surveys_count = int(cur.fetchone()["c"] or 0)
        cur.execute("SELECT COUNT(*) AS c FROM surveys WHERE status='COMPLETED'")
        completed_count = int(cur.fetchone()["c"] or 0)

    alerts = qa_alerts_dashboard(limit=10)

    body = """
    <h2>Supervisor Dashboard</h2>
    <div class="card">
      <p>Facilities: <b>{{ facilities_count }}</b></p>
      <p>Surveys: <b>{{ surveys_count }}</b></p>
      <p>Completed: <b>{{ completed_count }}</b></p>
      <p class="muted">Enumerator-first collection. Supervisor follows with QA and oversight.</p>
    </div>

    <div class="card">
      <h3>Top QA Alerts</h3>
      {% if alerts %}
        <table>
          <tr><th>Survey</th><th>Facility</th><th>Enumerator</th><th>Flags</th></tr>
          {% for a in alerts %}
            <tr>
              <td><a href="/ui/survey/{{ a.survey_id }}">#{{ a.survey_id }}</a></td>
              <td>{{ a.facility_name }}</td>
              <td>{{ a.enumerator_name }}</td>
              <td>{{ ", ".join(a.flags) }}</td>
            </tr>
          {% endfor %}
        </table>
      {% else %}
        <p class="muted">No alerts yet.</p>
      {% endif %}
    </div>
    """
    return _ui(
        "Supervisor Dashboard",
        body,
        facilities_count=facilities_count,
        surveys_count=surveys_count,
        completed_count=completed_count,
        alerts=alerts,
    )


@app.get("/ui/surveys")
def ui_surveys():
    rows = filter_surveys(limit=200)
    body = """
    <h2>Surveys</h2>
    <div class="card">
      <table>
        <tr><th>ID</th><th>Facility</th><th>Template</th><th>Type</th><th>Enumerator</th><th>Status</th><th>Date</th></tr>
        {% for sid, facility_name, tid, survey_type, enum, st, created_at in rows %}
          <tr>
            <td><a href="/ui/survey/{{ sid }}">#{{ sid }}</a></td>
            <td>{{ facility_name }}</td>
            <td>{{ tid or "-" }}</td>
            <td>{{ survey_type }}</td>
            <td>{{ enum }}</td>
            <td>{{ st }}</td>
            <td>{{ created_at }}</td>
          </tr>
        {% endfor %}
      </table>
    </div>
    """
    return _ui("Surveys", body, rows=rows)


@app.get("/ui/survey/<int:sid>")
def ui_one_survey(sid: int):
    header, answers, qa = get_survey_details(sid)
    (sid, fid, facility_name, template_id, survey_type, enumerator, status, created_at) = header

    body = """
    <h2>Survey #{{ sid }}</h2>
    <div class="card">
      <p><b>Facility:</b> {{ facility_name }}</p>
      <p><b>Enumerator:</b> {{ enumerator }}</p>
      <p><b>Status:</b> {{ status }}</p>
      <p><b>QA:</b> total={{ qa.total_answers }}, missing={{ qa.missing_count }},
         low_conf={{ qa.low_confidence_count }}, no_source={{ qa.no_source_count }},
         no_conf={{ qa.no_confidence_count }}</p>
    </div>

    <div class="card">
      <h3>Answers</h3>
      <table>
        <tr><th>#</th><th>Question</th><th>Answer</th><th>Source</th><th>Conf</th><th>Missing</th><th>Reason</th></tr>
        {% for (aid, tqid, q, a, src, conf, is_missing, reason) in answers %}
          <tr>
            <td>{{ aid }}</td>
            <td>{{ q }}</td>
            <td>{{ a }}</td>
            <td>{{ src or "-" }}</td>
            <td>{{ conf or "-" }}</td>
            <td>{{ 1 if is_missing else 0 }}</td>
            <td>{{ reason or "-" }}</td>
          </tr>
        {% endfor %}
      </table>
    </div>
    """
    return _ui("Survey Detail", body, sid=sid, facility_name=facility_name, enumerator=enumerator,
               status=status, qa=qa, answers=answers)


@app.get("/ui/qa")
def ui_qa():
    alerts = qa_alerts_dashboard(limit=200)
    body = """
    <h2>QA Alerts</h2>
    <div class="card">
      {% if alerts %}
        <table>
          <tr><th>Survey</th><th>Facility</th><th>Enumerator</th><th>Flags</th><th>Severity</th></tr>
          {% for a in alerts %}
            <tr>
              <td><a href="/ui/survey/{{ a.survey_id }}">#{{ a.survey_id }}</a></td>
              <td>{{ a.facility_name }}</td>
              <td>{{ a.enumerator_name }}</td>
              <td>{{ ", ".join(a.flags) }}</td>
              <td>{{ "%.1f"|format(a.severity) }}</td>
            </tr>
          {% endfor %}
        </table>
      {% else %}
        <p class="muted">No alerts.</p>
      {% endif %}
    </div>
    """
    return _ui("QA Alerts", body, alerts=alerts)


# =========================
# Enumerator UI (impress-first)
# =========================
ENUM_BASE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .nav a { margin-right: 12px; }
    .card { border: 1px solid #ddd; padding: 12px; border-radius: 12px; margin-top: 12px; }
    input, select, textarea { padding: 12px; width: 100%; box-sizing: border-box; margin-top: 6px; font-size: 16px; }
    button { padding: 12px; width: 100%; margin-top: 10px; font-size: 16px; }
    .muted { color: #666; }
    .pill { display:inline-block; padding: 4px 10px; border-radius: 999px; border: 1px solid #ddd; font-size: 12px; }
    .row { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    @media (max-width: 700px) { .row { grid-template-columns: 1fr; } }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid #ddd; padding: 8px; font-size: 14px; }
    th { text-align: left; }
    .btnrow { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  </style>
</head>
<body>
  <div class="nav">
    <a href="/enum">Enumerator Home</a>
    <a href="/ui">Supervisor</a>
  </div>
  <hr>
  {{ body|safe }}
</body>
</html>
"""


def _enum(title: str, body: str, **ctx):
    return render_template_string(ENUM_BASE, title=title, body=render_template_string(body, **ctx))


def _drafts_for_enum(enumerator_name: str, limit: int = 50):
    enum = (enumerator_name or "").strip()
    if not enum:
        return []
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.id, f.name AS facility_name, s.template_id, s.survey_type, s.created_at
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            WHERE s.status='DRAFT' AND s.enumerator_name=?
            ORDER BY s.id DESC
            LIMIT ?
            """,
            (enum, int(limit)),
        )
        return [(int(r["id"]), r["facility_name"], r["template_id"], r["survey_type"], r["created_at"]) for r in cur.fetchall()]


@app.get("/enum")
def enum_home():
    enum = (request.args.get("e") or "").strip()
    drafts = _drafts_for_enum(enum) if enum else []

    body = """
    <h2>Enumerator Home</h2>

    <div class="card">
      <h3>Your name</h3>
      <form method="get" action="/enum">
        <input name="e" value="{{ enum }}" placeholder="e.g., Sobowale" required>
        <button type="submit">Continue</button>
      </form>
      <p class="muted">We use your name to find your drafts and label your submissions.</p>
    </div>

    {% if enum %}
      <div class="card">
        <h3>Start New Survey</h3>
        <a href="/enum/start?e={{ enum }}"><button type="button">Start</button></a>
        <p class="muted">Default: one facility per day. You can still complete multiple facilities when needed.</p>
      </div>

      <div class="card">
        <h3>Continue Drafts</h3>
        {% if drafts %}
          <table>
            <tr><th>Survey</th><th>Facility</th><th>Template</th><th>Title</th><th>Created</th></tr>
            {% for sid, fname, tid, stype, created in drafts %}
              <tr>
                <td><a href="/enum/survey/{{ sid }}?e={{ enum }}">#{{ sid }}</a></td>
                <td>{{ fname }}</td>
                <td>{{ tid or "-" }}</td>
                <td>{{ stype }}</td>
                <td>{{ created }}</td>
              </tr>
            {% endfor %}
          </table>
        {% else %}
          <p class="muted">No drafts found.</p>
        {% endif %}
      </div>
    {% endif %}
    """
    return _enum("Enumerator Home", body, enum=enum, drafts=drafts)


@app.get("/enum/start")
def enum_start():
    enum = (request.args.get("e") or "").strip()
    if not enum:
        return redirect(url_for("enum_home"))

    templates = _templates_quick_list()
    facility_names = _facility_name_suggestions()

    body = """
    <h2>Start New Survey</h2>

    <div class="card">
      <h3>Setup</h3>
      <form method="post" action="/enum/start">
        <input type="hidden" name="e" value="{{ enum }}">

        <label>Select Template</label>
        <select name="template_id">
          <option value="">-- Select Template --</option>
          {% for tid, tname in templates %}
            <option value="{{ tid }}">{{ tname }}</option>
          {% endfor %}
        </select>
        <p class="muted" style="margin-top:6px;">
          Choose a template for the best experience. You can edit templates later.
        </p>

        <label style="margin-top:12px;">Facility Name</label>
        <input name="facility_name" list="facility_list" placeholder="Type facility name (enumerator enters it)" required>
        <datalist id="facility_list">
          {% for n in facility_names %}
            <option value="{{ n }}"></option>
          {% endfor %}
        </datalist>

        <label style="margin-top:12px;">Mode</label>
        <select name="mode">
          <option value="TEMPLATE" selected>Template Survey (Recommended)</option>
          <option value="MANUAL">Manual Survey (Fallback)</option>
        </select>

        <label style="margin-top:12px;">Survey Title (only for Manual mode)</label>
        <input name="survey_title" placeholder="e.g., Special Inspection - January 2026">

        <button type="submit">Create Survey</button>
      </form>
    </div>
    """
    return _enum("Start Survey", body, enum=enum, templates=templates, facility_names=facility_names)


@app.post("/enum/start")
def enum_start_post():
    enum = (request.form.get("e") or "").strip()
    if not enum:
        return redirect(url_for("enum_home"))

    facility_name = (request.form.get("facility_name") or "").strip()
    mode = (request.form.get("mode") or MODE_TEMPLATE).strip().upper()
    template_id_raw = (request.form.get("template_id") or "").strip()
    survey_title = (request.form.get("survey_title") or "").strip()

    # facility name is entered by enumerator
    facility_id = _get_or_create_facility_by_name(facility_name)

    if mode == MODE_MANUAL:
        if not survey_title:
            survey_title = "Manual Survey"
        sid = create_survey(
            facility_id=facility_id,
            survey_type=survey_title,
            enumerator_name=enum,
            template_id=None,
        )
        return redirect(url_for("enum_survey", sid=sid, e=enum))

    # TEMPLATE mode
    # If template not selected, default to Facility Assessment if available
    if not template_id_raw:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM survey_templates WHERE name=? LIMIT 1", ("Facility Assessment",))
            row = cur.fetchone()
            if row:
                template_id_raw = str(int(row["id"]))

    if not template_id_raw or not template_id_raw.isdigit():
        return redirect(url_for("enum_start", e=enum))

    template_id = int(template_id_raw)

    # Use template name as survey_type for clean reporting
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM survey_templates WHERE id=? LIMIT 1", (template_id,))
        row = cur.fetchone()
        tname = row["name"] if row else "Template Survey"

    sid = create_survey(
        facility_id=facility_id,
        survey_type=tname,
        enumerator_name=enum,
        template_id=template_id,
    )
    return redirect(url_for("enum_survey", sid=sid, e=enum))


@app.get("/enum/survey/<int:sid>")
def enum_survey(sid: int):
    enum = (request.args.get("e") or "").strip()

    header, answers, qa = get_survey_details(sid)
    (sid, fid, facility_name, template_id, survey_type, enumerator, status, created_at) = header

    # Manual mode
    if template_id is None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, question, answer
                FROM survey_answers
                WHERE survey_id=?
                ORDER BY id DESC
                LIMIT 20
                """,
                (int(sid),),
            )
            recent = [(int(r["id"]), r["question"], r["answer"]) for r in cur.fetchall()]

        body = """
        <h2>Manual Survey #{{ sid }}</h2>
        <div class="card">
          <p><b>Facility:</b> {{ facility_name }}</p>
          <p><b>Title:</b> {{ survey_type }}</p>
          <p><b>Status:</b> <span class="pill">{{ status }}</span></p>
        </div>

        <div class="card">
          <h3>Add Answer</h3>
          <form method="post" action="/enum/survey/{{ sid }}/manual_add">
            <input type="hidden" name="e" value="{{ enum }}">
            <label>Question</label>
            <input name="question" required>
            <label>Answer</label>
            <textarea name="answer" rows="4" required></textarea>
            <button type="submit">Save</button>
          </form>
        </div>

        <div class="card">
          <h3>Recent Answers</h3>
          {% if recent %}
            <table>
              <tr><th>ID</th><th>Question</th><th>Answer</th></tr>
              {% for aid, q, a in recent %}
                <tr><td>{{ aid }}</td><td>{{ q }}</td><td>{{ a }}</td></tr>
              {% endfor %}
            </table>
          {% else %}
            <p class="muted">No answers yet.</p>
          {% endif %}
        </div>

        <div class="card">
          <a href="/enum/submit/{{ sid }}?e={{ enum }}"><button type="button">Submit Survey</button></a>
        </div>
        """
        return _enum("Manual Survey", body, sid=sid, enum=enum, facility_name=facility_name,
                     survey_type=survey_type, status=status, recent=recent)

    # Template mode
    tq = get_template_questions(int(template_id))
    if not tq:
        body = """
        <h2>Template Survey #{{ sid }}</h2>
        <div class="card">
          <p>This template has no questions yet.</p>
        </div>
        """
        return _enum("Template Survey", body, sid=sid)

    answered_map = _template_answer_map(sid)
    total_q = len(tq)
    answered_count = len(answered_map)
    progress_pct = int((answered_count * 100) / total_q) if total_q else 0

    idx_raw = (request.args.get("q") or "").strip()
    if idx_raw.isdigit():
        idx = max(0, min(int(idx_raw), total_q - 1))
    else:
        idx = _find_next_unanswered_index(tq, answered_map)

    qid, qtext, qtype, order_no, is_required = tq[idx]
    qid = int(qid)
    is_required = int(is_required)

    existing = answered_map.get(qid)
    has_existing = existing is not None

    prev_idx = idx - 1 if idx > 0 else 0
    next_idx = idx + 1 if idx < total_q - 1 else total_q - 1

    body = """
    <h2>Template Survey #{{ sid }}</h2>

    <div class="card">
      <p><b>Facility:</b> {{ facility_name }}</p>
      <p><b>Title:</b> {{ survey_type }}</p>
      <p><b>Status:</b> <span class="pill">{{ status }}</span></p>
      <p><b>Progress:</b> {{ answered_count }}/{{ total_q }} ({{ progress_pct }}%)</p>
    </div>

    <div class="card">
      <h3>Question {{ idx+1 }} of {{ total_q }}</h3>
      <p><b>{{ qtext }}</b></p>
      <p class="muted">Type: {{ qtype }} | {% if is_required==1 %}Required{% else %}Optional{% endif %}</p>

      {% if has_existing %}
        <p class="muted">Already answered. You can update it.</p>
      {% endif %}

      <form method="post" action="/enum/survey/{{ sid }}/template_answer">
        <input type="hidden" name="e" value="{{ enum }}">
        <input type="hidden" name="q_index" value="{{ idx }}">
        <input type="hidden" name="template_question_id" value="{{ qid }}">
        <input type="hidden" name="question_text" value="{{ qtext }}">
        <input type="hidden" name="question_type" value="{{ qtype }}">
        <input type="hidden" name="is_required" value="{{ is_required }}">

        {% if qtype == "YESNO" %}
          <div class="btnrow">
            <button name="answer" value="YES" type="submit">YES</button>
            <button name="answer" value="NO" type="submit">NO</button>
          </div>

          <label style="margin-top:12px;">Source</label>
          <select name="answer_source">
            {% for s in sources %}
              <option value="{{ s }}" {% if has_existing and existing.source==s %}selected{% endif %}>{{ s }}</option>
            {% endfor %}
          </select>

          <label style="margin-top:10px;">Confidence</label>
          <select name="confidence_level">
            {% for c in confs %}
              <option value="{{ c }}" {% if has_existing and existing.confidence==c %}selected{% endif %}>{{ c }}</option>
            {% endfor %}
          </select>

        {% elif qtype == "NUMBER" %}
          <label>Answer (number)</label>
          <input name="answer" value="{{ existing.answer if has_existing else '' }}" inputmode="numeric" placeholder="e.g., 12">

          <label style="margin-top:10px;">Source</label>
          <select name="answer_source">
            {% for s in sources %}
              <option value="{{ s }}" {% if has_existing and existing.source==s %}selected{% endif %}>{{ s }}</option>
            {% endfor %}
          </select>

          <label style="margin-top:10px;">Confidence</label>
          <select name="confidence_level">
            {% for c in confs %}
              <option value="{{ c }}" {% if has_existing and existing.confidence==c %}selected{% endif %}>{{ c }}</option>
            {% endfor %}
          </select>

          <button type="submit" style="margin-top:12px;">Save</button>

        {% else %}
          <label>Answer</label>
          <textarea name="answer" rows="4">{{ existing.answer if has_existing else '' }}</textarea>

          <label style="margin-top:10px;">Source</label>
          <select name="answer_source">
            {% for s in sources %}
              <option value="{{ s }}" {% if has_existing and existing.source==s %}selected{% endif %}>{{ s }}</option>
            {% endfor %}
          </select>

          <label style="margin-top:10px;">Confidence</label>
          <select name="confidence_level">
            {% for c in confs %}
              <option value="{{ c }}" {% if has_existing and existing.confidence==c %}selected{% endif %}>{{ c }}</option>
            {% endfor %}
          </select>

          <button type="submit" style="margin-top:12px;">Save</button>
        {% endif %}
      </form>

      {% if is_required == 0 %}
        <hr>
        <h4>Skip (optional)</h4>
        <form method="post" action="/enum/survey/{{ sid }}/template_skip">
          <input type="hidden" name="e" value="{{ enum }}">
          <input type="hidden" name="q_index" value="{{ idx }}">
          <input type="hidden" name="template_question_id" value="{{ qid }}">
          <input type="hidden" name="question_text" value="{{ qtext }}">
          <label>Missing reason</label>
          <select name="missing_reason" required>
            {% for r in reasons %}
              <option value="{{ r }}" {% if has_existing and existing.missing_reason==r %}selected{% endif %}>{{ r }}</option>
            {% endfor %}
          </select>
          <button type="submit">Skip & Save</button>
        </form>
      {% endif %}
    </div>

    <div class="card">
      <div class="btnrow">
        <a href="/enum/survey/{{ sid }}?e={{ enum }}&q={{ prev_idx }}"><button type="button">Back</button></a>
        <a href="/enum/survey/{{ sid }}?e={{ enum }}&q={{ next_idx }}"><button type="button">Next</button></a>
      </div>
    </div>

    <div class="card">
      <a href="/enum/submit/{{ sid }}?e={{ enum }}"><button type="button">Submit Survey</button></a>
    </div>
    """
    return _enum(
        "Template Survey",
        body,
        sid=sid,
        enum=enum,
        facility_name=facility_name,
        survey_type=survey_type,
        status=status,
        idx=idx,
        total_q=total_q,
        answered_count=answered_count,
        progress_pct=progress_pct,
        qid=qid,
        qtext=qtext,
        qtype=qtype,
        is_required=is_required,
        has_existing=has_existing,
        existing=existing or {},
        prev_idx=prev_idx,
        next_idx=next_idx,
        sources=ENUM_ALLOWED_SOURCES,
        confs=ENUM_ALLOWED_CONF,
        reasons=ENUM_MISSING_REASONS,
    )


@app.post("/enum/survey/<int:sid>/manual_add")
def enum_manual_add(sid: int):
    enum = (request.form.get("e") or "").strip()
    q = (request.form.get("question") or "").strip()
    a = (request.form.get("answer") or "").strip()
    if q and a:
        add_answer(survey_id=sid, question=q, answer=a)
    return redirect(url_for("enum_survey", sid=sid, e=enum))


@app.post("/enum/survey/<int:sid>/template_answer")
def enum_template_answer(sid: int):
    enum = (request.form.get("e") or "").strip()
    q_index = int(request.form.get("q_index") or 0)

    tqid = int(request.form.get("template_question_id"))
    qtext = (request.form.get("question_text") or "").strip()
    qtype = (request.form.get("question_type") or "TEXT").strip().upper()

    raw_answer = (request.form.get("answer") or "").strip()
    source = (request.form.get("answer_source") or "INTERVIEW").strip().upper()
    conf = (request.form.get("confidence_level") or "MEDIUM").strip().upper()

    try:
        cleaned = _validate_by_type(qtype, raw_answer)
        if source not in ENUM_ALLOWED_SOURCES:
            source = "INTERVIEW"
        if conf not in ENUM_ALLOWED_CONF:
            conf = "MEDIUM"

        _upsert_template_answer(
            survey_id=sid,
            template_question_id=tqid,
            question_text=qtext,
            answer=cleaned,
            source=source,
            confidence=conf,
            is_missing=0,
            missing_reason=None,
        )
    except Exception:
        return redirect(url_for("enum_survey", sid=sid, e=enum, q=q_index))

    return redirect(url_for("enum_survey", sid=sid, e=enum, q=q_index + 1))


@app.post("/enum/survey/<int:sid>/template_skip")
def enum_template_skip(sid: int):
    enum = (request.form.get("e") or "").strip()
    q_index = int(request.form.get("q_index") or 0)

    tqid = int(request.form.get("template_question_id"))
    qtext = (request.form.get("question_text") or "").strip()
    missing_reason = (request.form.get("missing_reason") or "UNAVAILABLE").strip().upper()
    if missing_reason not in ENUM_MISSING_REASONS:
        missing_reason = "UNAVAILABLE"

    _upsert_template_answer(
        survey_id=sid,
        template_question_id=tqid,
        question_text=qtext,
        answer="__MISSING__",
        source=None,
        confidence=None,
        is_missing=1,
        missing_reason=missing_reason,
    )
    return redirect(url_for("enum_survey", sid=sid, e=enum, q=q_index + 1))


@app.get("/enum/submit/<int:sid>")
def enum_submit(sid: int):
    enum = (request.args.get("e") or "").strip()

    header, answers, qa = get_survey_details(sid)
    (sid, fid, facility_name, template_id, survey_type, enumerator, status, created_at) = header

    body = """
    <h2>Submit Survey #{{ sid }}</h2>
    <div class="card">
      <p><b>Facility:</b> {{ facility_name }}</p>
      <p><b>Total answers:</b> {{ qa.total_answers }}</p>
      <p class="muted">Submitting will mark this survey as COMPLETED.</p>

      <form method="post" action="/enum/submit/{{ sid }}">
        <input type="hidden" name="e" value="{{ enum }}">
        <button type="submit">Confirm Submit</button>
      </form>

      <a href="/enum/survey/{{ sid }}?e={{ enum }}"><button type="button">Go back</button></a>
    </div>
    """
    return _enum("Submit Survey", body, sid=sid, facility_name=facility_name, qa=qa, enum=enum)


@app.post("/enum/submit/<int:sid>")
def enum_submit_post(sid: int):
    enum = (request.form.get("e") or "").strip()
    complete_survey(sid)
    body = """
    <h2>Submitted</h2>
    <div class="card">
      <p><b>Survey #{{ sid }}</b> submitted successfully.</p>
      <a href="/enum?e={{ enum }}"><button type="button">Back to Home</button></a>
      <a href="/enum/start?e={{ enum }}"><button type="button">Start Another Survey</button></a>
    </div>
    """
    return _enum("Submitted", body, sid=sid, enum=enum)


# =========================
# Run
# =========================
if __name__ == "__main__":
    init_db()
    _seed_default_templates()
    app.run(host="0.0.0.0", port=5000, debug=True)