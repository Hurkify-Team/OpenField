from datetime import datetime
from db import get_conn

def add_facility(
    name: str,
    facility_type: str = "",
    address: str = "",
    lga: str = "",
    state: str = "",
    contact_name: str = "",
    contact_phone: str = "",
) -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Facility name cannot be empty.")

    facility_type = (facility_type or "").strip()
    address = (address or "").strip()
    lga = (lga or "").strip()
    state = (state or "").strip()
    contact_name = (contact_name or "").strip()
    contact_phone = (contact_phone or "").strip()
    created_at = datetime.now().isoformat(timespec="seconds")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO facilities
            (name, facility_type, address, lga, state, contact_name, contact_phone, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, facility_type, address, lga, state, contact_name, contact_phone, created_at),
        )
        conn.commit()
        return int(cur.lastrowid)

def list_facilities(limit: int = 50):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, facility_type, lga, state, created_at
            FROM facilities
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return cur.fetchall()

def get_facility_by_id(facility_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, facility_type, address, lga, state, contact_name, contact_phone, created_at
            FROM facilities
            WHERE id = ?
            """,
            (int(facility_id),),
        )
        return cur.fetchone()