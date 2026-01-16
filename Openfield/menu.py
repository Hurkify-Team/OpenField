from db import init_db
from facilities import add_facility, list_facilities, get_facility_by_id
from surveys import (
    create_survey,
    add_answer,
    complete_survey,
    list_surveys,
    start_survey_from_template,
)
from templates import (
    create_template,
    list_templates,
    add_template_question,
    get_template_questions,
)
from exports import (
    export_facilities_csv,
    export_surveys_flat_csv,
    export_surveys_json,
    export_one_survey_json,
)
from supervision import (
    search_facilities_by_name,
    list_surveys_by_facility,
    filter_surveys,
    get_survey_details,
    qa_alerts_dashboard,
    _format_alert_row,
    enumerator_performance_dashboard,
    _format_enumerator_row,
)
from qa_exports import export_qa_alerts_csv


# ---------- PRINT HELPERS ----------

def _print_facilities(rows):
    if not rows:
        print("\nNo facilities found.")
        return
    print("\nFacilities")
    print("-" * 80)
    for fid, name, ftype, lga, state, created_at in rows:
        print(f"#{fid} | {name} | {ftype or '-'} | {lga or '-'}, {state or '-'} | {created_at}")
    print("-" * 80)


def _print_surveys(rows):
    if not rows:
        print("\nNo surveys found.")
        return
    print("\nSurveys")
    print("-" * 120)
    for sid, facility, template_id, survey_type, enum, status, created_at in rows:
        print(
            f"#{sid} | {facility} | TID:{template_id or '-'} | "
            f"{survey_type} | {enum} | {status} | {created_at}"
        )
    print("-" * 120)


def _print_templates(rows):
    if not rows:
        print("\nNo templates found.")
        return
    print("\nSurvey Templates")
    print("-" * 90)
    for tid, name, desc, created_at in rows:
        print(f"#{tid} | {name} | {desc or '-'} | {created_at}")
    print("-" * 90)


def _print_template_questions(rows):
    if not rows:
        print("\nNo questions for this template.")
        return
    print("\nTemplate Questions")
    print("-" * 100)
    for qid, qtext, qtype, order, req in rows:
        print(
            f"QID:{qid} | Order:{order} | {qtype} | "
            f"{'REQUIRED' if req else 'OPTIONAL'} | {qtext}"
        )
    print("-" * 100)


def _print_survey_detail(header, answers, qa):
    sid, fid, facility, template_id, survey_type, enum, status, created_at = header

    print("\nSurvey Detail")
    print("-" * 80)
    print(f"Survey ID: {sid}")
    print(f"Facility: {facility} (ID {fid})")
    print(f"Template ID: {template_id or '-'}")
    print(f"Survey Type: {survey_type}")
    print(f"Enumerator: {enum}")
    print(f"Status: {status}")
    print(f"Created At: {created_at}")

    print("\nQA Summary")
    print("-" * 80)
    for k, v in qa.items():
        print(f"{k.replace('_',' ').title()}: {v}")

    print("\nAnswers")
    print("-" * 120)
    for aid, tqid, q, a, src, conf, is_missing, reason in answers:
        print(
            f"AID:{aid} | TQID:{tqid or '-'} | Missing:{is_missing} | "
            f"Source:{src or '-'} | Confidence:{conf or '-'} | Reason:{reason or '-'}"
        )
        print(f"Q: {q}")
        print(f"A: {a}")
        print("-" * 120)


# ---------- MAIN MENU ----------

def run_menu():
    init_db()

    while True:
        print("\n=== OpenField Collect (CLI) ===")
        print("1) Register Facility")
        print("2) List Facilities")

        print("3) Start Survey (Manual)")
        print("4) Add Manual Survey Answers")
        print("5) Complete Survey")
        print("6) List Surveys")

        print("7) Create Survey Template")
        print("8) List Templates")
        print("9) Add Template Question")
        print("10) View Template Questions")
        print("11) Start Survey From Template")

        print("12) Export Facilities (CSV)")
        print("13) Export Surveys + Answers (CSV)")
        print("14) Export Surveys + Answers (JSON)")
        print("15) Export One Survey (JSON)")

        print("16) Search Facility by Name")
        print("17) List Surveys by Facility")
        print("18) Filter Surveys")
        print("19) View Survey Detail + QA")

        print("20) QA Alerts Dashboard")
        print("21) Export QA Alerts (CSV)")
        print("22) Enumerator Performance Dashboard")

        print("0) Exit")

        choice = input("Select: ").strip()

        try:
            if choice == "1":
                name = input("Facility name: ").strip()
                fid = add_facility(name=name)
                print(f"Facility saved. ID: {fid}")

            elif choice == "2":
                _print_facilities(list_facilities())

            elif choice == "3":
                fid = int(input("Facility ID: "))
                stype = input("Survey type: ").strip()
                enum = input("Enumerator name: ").strip()
                sid = create_survey(fid, stype, enum)
                print(f"Survey started. ID: {sid}")

            elif choice == "4":
                sid = int(input("Survey ID: "))
                while True:
                    q = input("Question (blank to stop): ").strip()
                    if not q:
                        break
                    a = input("Answer: ").strip()
                    add_answer(sid, q, a)
                print("Answers saved.")

            elif choice == "5":
                sid = int(input("Survey ID: "))
                complete_survey(sid)
                print("Survey marked as COMPLETED.")

            elif choice == "6":
                _print_surveys(list_surveys())

            elif choice == "7":
                name = input("Template name: ").strip()
                desc = input("Description [optional]: ").strip()
                tid = create_template(name, desc)
                print(f"Template created. ID: {tid}")

            elif choice == "8":
                _print_templates(list_templates())

            elif choice == "9":
                tid = int(input("Template ID: "))
                q = input("Question text: ").strip()
                qt = input("Type (TEXT/YESNO/NUMBER): ").strip().upper()
                order = int(input("Display order: "))
                req = input("Required? (Y/N): ").strip().lower() != "n"
                add_template_question(tid, q, qt, order, int(req))
                print("Question added.")

            elif choice == "10":
                tid = int(input("Template ID: "))
                _print_template_questions(get_template_questions(tid))

            elif choice == "11":
                fid = int(input("Facility ID: "))
                tid = int(input("Template ID: "))
                enum = input("Enumerator name: ").strip()
                sid = start_survey_from_template(fid, tid, enum)
                print(f"Template survey saved. ID: {sid}")

            elif choice == "12":
                print("Saved:", export_facilities_csv())

            elif choice == "13":
                print("Saved:", export_surveys_flat_csv())

            elif choice == "14":
                print("Saved:", export_surveys_json())

            elif choice == "15":
                sid = int(input("Survey ID: "))
                print("Saved:", export_one_survey_json(sid))

            elif choice == "16":
                kw = input("Facility name contains: ").strip()
                _print_facilities(search_facilities_by_name(kw))

            elif choice == "17":
                fid = int(input("Facility ID: "))
                _print_surveys(list_surveys_by_facility(fid))

            elif choice == "18":
                status = input("Status (COMPLETED/DRAFT) [optional]: ").strip() or None
                enum = input("Enumerator contains [optional]: ").strip() or None
                _print_surveys(filter_surveys(status=status, enumerator=enum))

            elif choice == "19":
                sid = int(input("Survey ID: "))
                h, a, qa = get_survey_details(sid)
                _print_survey_detail(h, a, qa)

            elif choice == "20":
                alerts = qa_alerts_dashboard()
                if not alerts:
                    print("No QA alerts.")
                else:
                    print("\nQA Alerts (ranked by severity)")
                    print("-" * 140)
                    for a in alerts:
                        print(_format_alert_row(a))
                    print("-" * 140)

            elif choice == "21":
                path = export_qa_alerts_csv()
                print(f"QA Alerts exported to: {path}")

            elif choice == "22":
                rows = enumerator_performance_dashboard()
                if not rows:
                    print("No enumerator data.")
                else:
                    print("\nEnumerator Performance (ranked by severity)")
                    print("-" * 140)
                    for e in rows:
                        print(_format_enumerator_row(e))
                    print("-" * 140)

            elif choice == "0":
                print("Goodbye.")
                break

            else:
                print("Invalid option.")

        except Exception as e:
            print("Error:", e)