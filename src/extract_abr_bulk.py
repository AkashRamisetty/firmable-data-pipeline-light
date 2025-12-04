from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Dict, List

import psycopg2
from psycopg2.extras import execute_values
from xml.etree.ElementTree import iterparse

DB_CONFIG = {
    "dbname": "firmable_companies",
    "user": "firmable",
    "password": "firmable_password",
    "host": "localhost",
    "port": 5432,
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "abr"
BATCH_SIZE = 5000


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def strip_ns(tag: str) -> str:
    """Strip XML namespace: {ns}Tag -> Tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def parse_abr_entity(abr_elem) -> Dict:
    """
    Convert one <ABR> element into a flat dict matching raw_abr schema.

    Structure from your sample:

    <ABR recordLastUpdatedDate="20220512" ...>
      <ABN status="CAN" ABNStatusFromDate="20220511">55447533728</ABN>
      <EntityType>
        <EntityTypeInd>IND</EntityTypeInd>
        <EntityTypeText>Individual/Sole Trader</EntityTypeText>
      </EntityType>
      <LegalEntity>
        <IndividualName type="LGL">
          <GivenName>...</GivenName>
          <FamilyName>...</FamilyName>
        </IndividualName>
        <BusinessAddress>... <State>NSW</State><Postcode>2170</Postcode> ...
      </LegalEntity>
      OR
      <MainEntity>
        <NonIndividualNameText>Some Name</NonIndividualNameText>
        <BusinessAddress>... <State>...</State><Postcode>...</Postcode> ...
      </MainEntity>
      <GST status="CAN" GSTStatusFromDate="20120101" />
      <OtherEntity>... (extra trading names) ...</OtherEntity>
    </ABR>
    """
    rec: Dict[str, str] = {
        "abn": None,
        "entity_name": None,
        "entity_type": None,
        "entity_status": None,
        "address_line_1": None,
        "address_line_2": None,
        "suburb": None,   # not present in sample, but keep for schema
        "postcode": None,
        "state": None,
        "country": "AU",
        "start_date_raw": None,
    }

    # Prefer main "organisation" name if available,
    # otherwise fall back to constructed person name.
    main_name_candidate: str | None = None
    person_given_names: List[str] = []
    person_family_name: str | None = None

    # ABR element-level attributes
    record_updated = abr_elem.attrib.get("recordLastUpdatedDate")
    if record_updated and not rec["start_date_raw"]:
        rec["start_date_raw"] = record_updated

    for child in abr_elem.iter():
        tag = strip_ns(child.tag)
        text = (child.text or "").strip()

        # ---------- ABN ----------
        if tag == "ABN":
            if text and not rec["abn"]:
                rec["abn"] = text
            status = child.attrib.get("status")
            if status and not rec["entity_status"]:
                rec["entity_status"] = status
            # ABNStatusFromDate is a good "start date" candidate
            abn_from = child.attrib.get("ABNStatusFromDate")
            if abn_from and not rec["start_date_raw"]:
                rec["start_date_raw"] = abn_from

        # ---------- Entity type ----------
        elif tag == "EntityTypeText":
            if text and not rec["entity_type"]:
                rec["entity_type"] = text

        # ---------- Organisation names ----------
        elif tag == "NonIndividualNameText":
            # e.g. FRESHWATER BAY PRIMARY SCHOOL, Gates Superannuation Fund,
            # The Trustee for J A Sill Super Fund, etc.
            if text and not main_name_candidate:
                main_name_candidate = text

        # ---------- Person names ----------
        elif tag == "GivenName":
            if text:
                person_given_names.append(text)
        elif tag == "FamilyName":
            if text and not person_family_name:
                person_family_name = text

        # ---------- Address ----------
        elif tag == "State":
            if text and not rec["state"]:
                rec["state"] = text
        elif tag == "Postcode":
            if text and not rec["postcode"]:
                rec["postcode"] = text
        # No explicit suburb in sample, so we leave rec["suburb"] as None

        # ---------- GST-based dates (fallback) ----------
        elif tag == "GST":
            gst_from = child.attrib.get("GSTStatusFromDate")
            if gst_from and not rec["start_date_raw"]:
                rec["start_date_raw"] = gst_from

    # Decide on entity_name:
    if main_name_candidate:
        rec["entity_name"] = main_name_candidate
    else:
        # Construct "PERSON" style name: "GIVEN GIVEN FAMILY"
        if person_given_names or person_family_name:
            parts = []
            parts.extend(person_given_names)
            if person_family_name:
                parts.append(person_family_name)
            rec["entity_name"] = " ".join(parts) if parts else None

    # address_line_1 / address_line_2:
    # ABR sample doesn't show address lines, only State/Postcode,
    # so we'll keep these as None for now (columns are nullable).
    return rec


def iter_abr_records_from_xml(xml_file):
    """
    Stream over an ABR XML file and yield one dict per <ABR> record.
    """
    print("🔍 Streaming XML from zip member...")
    context = iterparse(xml_file, events=("end",))
    for event, elem in context:
        tag = strip_ns(elem.tag)
        if tag == "ABR":
            rec = parse_abr_entity(elem)
            # Enforce NOT NULL constraint on entity_name
            if rec["abn"] and rec["entity_name"]:
                yield rec
            elem.clear()  # free memory


def load_abr_bulk_into_db():
    zip_files = sorted(DATA_DIR.glob("*.zip"))
    if not zip_files:
        raise FileNotFoundError(f"No .zip files found in {DATA_DIR}. Put public_split_*.zip there.")

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("🧹 Truncating raw_abr before bulk load (idempotent demo)...")
        cur.execute("TRUNCATE TABLE raw_abr;")

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
                start_date_raw
            )
            VALUES %s
        """

        batch: List[tuple] = []
        total = 0

        for zip_path in zip_files:
            print(f"📦 Processing ZIP: {zip_path.name}")
            with zipfile.ZipFile(zip_path, "r") as zf:
                for member in zf.namelist():
                    if not member.lower().endswith(".xml"):
                        continue
                    print(f"  📄 XML file: {member}")
                    with zf.open(member) as xml_file:
                        for rec in iter_abr_records_from_xml(xml_file):
                            batch.append(
                                (
                                    rec["abn"],
                                    rec["entity_name"],
                                    rec["entity_type"],
                                    rec["entity_status"],
                                    rec["address_line_1"],
                                    rec["address_line_2"],
                                    rec["suburb"],
                                    rec["postcode"],
                                    rec["state"],
                                    rec["country"],
                                    rec["start_date_raw"],
                                )
                            )

                            if len(batch) >= BATCH_SIZE:
                                execute_values(cur, insert_sql, batch)
                                total += len(batch)
                                print(f"    ✅ Inserted {total} rows so far...")
                                batch.clear()

        if batch:
            execute_values(cur, insert_sql, batch)
            total += len(batch)
            batch.clear()

        conn.commit()
        print(f"🎉 ABR bulk load complete. Total rows inserted: {total}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during ABR bulk load: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    print(f"Using ABR data dir: {DATA_DIR}")
    load_abr_bulk_into_db()
