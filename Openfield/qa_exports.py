import csv
from pathlib import Path
from supervision import qa_alerts_dashboard


def _exports_dir():
    d = Path(__file__).with_name("exports")
    d.mkdir(exist_ok=True)
    return d


def export_qa_alerts_csv(
    filename="qa_alerts.csv",
    status_filter="COMPLETED",
    missing_threshold_pct=20.0,
    low_conf_threshold_pct=20.0,
    no_source_threshold_pct=10.0,
    no_conf_threshold_pct=10.0,
    limit=200,
):
    out_path = _exports_dir() / filename

    alerts = qa_alerts_dashboard(
        status_filter=status_filter,
        missing_threshold_pct=float(missing_threshold_pct),
        low_conf_threshold_pct=float(low_conf_threshold_pct),
        no_source_threshold_pct=float(no_source_threshold_pct),
        no_conf_threshold_pct=float(no_conf_threshold_pct),
        limit=int(limit),
    )

    headers = [
        "survey_id", "facility_id", "facility_name",
        "template_id", "survey_type", "enumerator_name", "status", "created_at",
        "total_answers",
        "missing", "missing_pct",
        "low_confidence", "low_conf_pct",
        "no_source", "no_source_pct",
        "no_confidence", "no_conf_pct",
        "flags", "severity",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for a in alerts:
            w.writerow([
                a["survey_id"], a["facility_id"], a["facility_name"],
                a["template_id"], a["survey_type"], a["enumerator_name"], a["status"], a["created_at"],
                a["total_answers"],
                a["missing"], f"{a['missing_pct']:.1f}",
                a["low_confidence"], f"{a['low_conf_pct']:.1f}",
                a["no_source"], f"{a['no_source_pct']:.1f}",
                a["no_confidence"], f"{a['no_conf_pct']:.1f}",
                "; ".join(a["flags"]),
                f"{a['severity']:.1f}",
            ])

    return str(out_path)