import requests
import sqlite3
from contextlib import closing
from prefect import task, flow


# --- Extract
@task(retries=3)
def get_complaint_data():
    r = requests.get(
        "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/",
        params={"size": 10},
        timeout=10
    )
    r.raise_for_status()
    response_json = r.json()
    return response_json["hits"]["hits"]


# --- Transform
@task
def parse_complaint_data(raw):
    complaints = []
    for row in raw:
        source = row.get("_source", {})
        complaints.append((
            source.get("date_received"),     # ⚠ corregido el typo
            source.get("state"),
            source.get("product"),
            source.get("company"),
            source.get("complaint_what_happened")
        ))
    return complaints


# --- Load
@task
def store_complaints(parsed):
    create_script = """
    CREATE TABLE IF NOT EXISTS complaint (
        timestamp TEXT,
        state TEXT,
        product TEXT,
        company TEXT,
        complaint_what_happened TEXT
    )
    """
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed)
            conn.commit()


# --- Flow
@flow
def cfpb_etl_flow():
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    store_complaints(parsed)


if __name__ == "__main__":
    cfpb_etl_flow()