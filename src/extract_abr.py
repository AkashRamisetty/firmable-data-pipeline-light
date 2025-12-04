import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict

import psycopg2
from psycopg2.extras import execute_values


DB_CONFIG = {
    "dbname": "firmable_companies",
    "user": "firmable",
    "password": "firmable_password",
    "host": "localhost",
    "port": 5432,
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def parse_abr_xml(xml_path: Path, load_batch_id: str) -> List[Dict]:
    """
    Parse an ABR-like XML file and return a list of row dicts
    matching raw_abr columns.
    """
    print(f"📂 Parsing ABR XML: {xml_path}")
    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows = []

    # Adjust tag names/XPaths later for real ABR structure.
    for rec in root.findall(".//ABRRecord"):
        abn = (rec.findtext("ABN") or "").strip()
        entity_name = (rec.findtext("EntityName") or "").strip()
        entity_type = (rec.findtext("EntityType") or "").strip()
        entity_status = (rec.findtext("EntityStatus") or "").strip()

        addr = rec.find("MainBusinessPhysicalAddress")
        address_line_1 = (addr.findtext("AddressLine1") or "").strip() if addr is not None else None
        address_line_2 = (addr.findtext("AddressLine2") or "").strip() if addr is not None else None
        suburb = (addr.findtext("Suburb") or "").strip() if addr is not None else None
        postcode = (addr.findtext("Postcode") or "").strip() if addr is not None else None
        state = (addr.findtext("State") or "").strip() if addr is not None else None
        country = (addr.findtext("Country") or "").strip() if addr is not None else None

        start_date_raw = (rec.findtext("StartDate") or "").strip()

        if not abn or not entity_name:
            # Skip incomplete records; you can also log them.
            continue

        rows.append(
            {
                "abn": abn,
                "entity_name": entity_name,
                "entity_type": entity_type,
                "entity_status": entity_status,
                "address_line_1": address_line_1,
                "address_line_2": address_line_2,
                "suburb": suburb,
                "postcode": postcode,
                "state": state,
                "country": country,
                "start_date_raw": start_date_raw,
                "load_batch_id": load_batch_id,
            }
        )

    print(f"✅ Parsed {len(rows)} ABR records from XML")
    return rows


def insert_rows(rows: List[Dict]):
    if not rows:
        print("⚠️ No rows to insert.")
        return

    print(f"💾 Inserting {len(rows)} rows into raw_abr...")
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO raw_abr (
            abn,
            entity_name,
            entity_type,
            entity_status,
            address_line_1,
            address_line_2,
            suburb,
            postcode,
            state,
            country,
            start_date_raw,
            load_batch_id
        )
        VALUES %s
        ON CONFLICT (abn) DO UPDATE
        SET
            entity_name = EXCLUDED.entity_name,
            entity_type = EXCLUDED.entity_type,
            entity_status = EXCLUDED.entity_status,
            address_line_1 = EXCLUDED.address_line_1,
            address_line_2 = EXCLUDED.address_line_2,
            suburb = EXCLUDED.suburb,
            postcode = EXCLUDED.postcode,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            start_date_raw = EXCLUDED.start_date_raw,
            load_batch_id = EXCLUDED.load_batch_id
    """

    values = [
        (
            r["abn"],
            r["entity_name"],
            r["entity_type"],
            r["entity_status"],
            r["address_line_1"],
            r["address_line_2"],
            r["suburb"],
            r["postcode"],
            r["state"],
            r["country"],
            r["start_date_raw"],
            r["load_batch_id"],
        )
        for r in rows
    ]

    execute_values(cur, insert_sql, values)
    cur.close()
    conn.close()
    print("✅ Insert completed.")


def main():
    # For now, we hardcode the sample file & batch id.
    xml_path = Path(__file__).resolve().parent.parent / "data" / "sample_abr.xml"
    load_batch_id = "sample_abr_batch_1"

    rows = parse_abr_xml(xml_path, load_batch_id)
    insert_rows(rows)


if __name__ == "__main__":
    main()
