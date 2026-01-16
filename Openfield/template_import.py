import re
from typing import List, Dict, Tuple


def extract_text_from_docx(path: str) -> str:
    # Free library: python-docx
    from docx import Document
    doc = Document(path)
    parts = []
    for p in doc.paragraphs:
        t = (p.text or "").strip()
        if t:
            parts.append(t)
    return "\n".join(parts)


def extract_text_from_pdf(path: str) -> str:
    """
    Extracts text from a text-based PDF (selectable text).
    For scanned PDFs (images), this returns little/no text.
    Free library: PyMuPDF (fitz).
    """
    try:
        import fitz  # PyMuPDF
    except Exception as e:
        raise RuntimeError(
            "Missing dependency: PyMuPDF. Install it with: pip install pymupdf"
        ) from e

    doc = fitz.open(path)
    parts = []
    for page in doc:
        t = (page.get_text("text") or "").strip()
        if t:
            parts.append(t)
    doc.close()
    return "\n".join(parts)


def _looks_like_question(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return False

    # Common patterns:
    # 1) "1. Question", "1) Question", "1 - Question"
    # 2) "A) Question"
    # 3) "- Question", "• Question"
    numbered = re.match(r"^\s*(\d+[\.\)\:\-]|\(?\d+\)?[\.\)\:\-])\s+.+", s)
    lettered = re.match(r"^\s*([A-Za-z][\)\.\:])\s+.+", s)
    bulleted = re.match(r"^\s*([-\u2022•])\s+.+", s)

    # Heuristic: ends with '?' strongly indicates a question
    ends_q = s.endswith("?")

    return bool(numbered or lettered or bulleted or ends_q)


def _clean_leading_marker(line: str) -> str:
    s = (line or "").strip()
    s = re.sub(r"^\s*(\(?\d+\)?[\.\)\:\-])\s+", "", s)          # 1.  1)  (1)
    s = re.sub(r"^\s*([A-Za-z][\)\.\:])\s+", "", s)            # A)  a.
    s = re.sub(r"^\s*([-\u2022•])\s+", "", s)                  # -  •
    return s.strip()


def infer_question_type(question_text: str) -> str:
    """
    Returns one of: YESNO, NUMBER, TEXT
    """
    q = (question_text or "").lower()

    # YES/NO detection
    if any(x in q for x in ["yes/no", "yes or no", "is the", "are you", "do you", "does the", "did you", "was the", "were the"]):
        # Not perfect, but useful
        return "YESNO"

    # NUMBER detection
    if any(x in q for x in ["how many", "number of", "minutes", "hours", "age", "count", "quantity", "rate", "percentage", "%"]):
        return "NUMBER"

    return "TEXT"


def parse_questions_from_text(raw_text: str, max_questions: int = 200) -> List[Dict]:
    """
    Produces a list of question dicts:
      { "question_text": str, "question_type": "TEXT|YESNO|NUMBER", "order_no": int, "is_required": int }
    """
    text = raw_text or ""
    lines = [ln.strip() for ln in text.splitlines()]

    questions: List[str] = []
    buffer: List[str] = []

    def flush():
        nonlocal buffer
        if buffer:
            q = " ".join([b.strip() for b in buffer if b.strip()]).strip()
            if q:
                questions.append(q)
        buffer = []

    for ln in lines:
        if not ln:
            # Blank line ends current buffer
            flush()
            continue

        if _looks_like_question(ln):
            # New question starts; flush old first
            flush()
            buffer.append(_clean_leading_marker(ln))
        else:
            # Continuation line for current question (often in Word/PDF layouts)
            if buffer:
                buffer.append(ln)
            else:
                # Ignore header text until first question appears
                continue

    flush()

    # Post-process, cap, and infer types
    out: List[Dict] = []
    order_no = 1
    for q in questions[:max_questions]:
        # Required heuristic: if contains "*" at end or "[Required]"
        is_required = 1 if ("*" in q or "required" in q.lower()) else 0
        q_clean = q.replace("[Required]", "").replace("(Required)", "").strip()
        out.append({
            "question_text": q_clean,
            "question_type": infer_question_type(q_clean),
            "order_no": order_no,
            "is_required": is_required
        })
        order_no += 1

    return out


def parse_questions_from_file(path: str) -> Tuple[str, List[Dict]]:
    """
    Returns (raw_text, parsed_questions)
    """
    lower = path.lower()
    if lower.endswith(".docx"):
        raw = extract_text_from_docx(path)
    elif lower.endswith(".pdf"):
        raw = extract_text_from_pdf(path)
    else:
        raise ValueError("Unsupported file type. Upload a .docx or .pdf")

    qs = parse_questions_from_text(raw)
    return raw, qs