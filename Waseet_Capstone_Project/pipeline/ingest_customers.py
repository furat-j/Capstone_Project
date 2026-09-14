"""Pull the customer dimension from the Waseet API into the warehouse.

    python ingest_customers.py

Run this once before the first scan load, because `pipeline.py` checks
customer_id against this table.

The API is at ../api/waseet_api.py. Start it in-process:

    import sys
    sys.path.insert(0, "../api")
    from waseet_api import start_api
    BASE_URL = start_api()

Three things about it will bite a client that assumes the easy case, and all
three were in Lecture 7:

  * the key goes in the X-API-Key header, not the query string
  * the answer is an envelope; the records are under "customers"
  * the first attempt at every third page comes back 429

Two hundred customers arrive over eight pages. If your pull finishes with fewer
than two hundred and does not complain, that is the bug this API exists to find.
"""

import os

import pandas as pd
import requests
from sqlalchemy import create_engine

API_KEY = "waseet-demo-key"
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")

engine = create_engine(DB_URL)


import time
from sqlalchemy import text


def get_with_retry(url, headers, params, attempts=4):
    """One GET, with a timeout, and a doubling wait on 429 and 5xx.

    Retry the transient and give up on the permanent. A 401 will not become a
    200 no matter how many times you ask, and retrying it just delays the moment
    somebody notices the key is wrong.
    """
    wait = 1
    for attempt in range(attempts):
        reply = requests.get(url, headers=headers, params=params, timeout=5)
        # Permanent errors: fail immediately
        if reply.status_code in (401, 403, 404):
            return reply

        # Rate limit or server error: wait with exponential backoff and retry
        if reply.status_code == 429 or 500 <= reply.status_code < 600:
            retry_after = reply.headers.get("Retry-After")
            sleep_time = int(retry_after) if retry_after and retry_after.isdigit() else wait
            time.sleep(sleep_time)
            wait *= 2
            continue

        return reply
    return reply


def fetch_all_customers(base_url):
    """Page until has_more is False. Return a list of dicts.

    Drive the loop on what the API tells you, not on a page count you worked out
    yourself - the second one is right until the day the data grows.
    """
    page = 1
    all_customers = []
    headers = {"X-API-Key": API_KEY}

    while True:
        reply = get_with_retry(f"{base_url}/customers", headers=headers, params={"page": page})
        reply.raise_for_status()
        payload = reply.json()
        customers = payload.get("customers", [])
        all_customers.extend(customers)

        if not payload.get("has_more", False):
            break
        page += 1

    return all_customers


def load_customers(records):
    """Land them in the customers table.

    Idempotent: running this twice must not double the dimension, and must not
    fail either. The table has a primary key, which rules out a plain append.
    """
    upsert_stmt = text("""
        INSERT INTO customers (customer_id, customer_name, segment, city, signup_date, credit_limit)
        VALUES (:customer_id, :customer_name, :segment, :city, :signup_date, :credit_limit)
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            segment = EXCLUDED.segment,
            city = EXCLUDED.city,
            signup_date = EXCLUDED.signup_date,
            credit_limit = EXCLUDED.credit_limit
    """)

    with engine.begin() as conn:
        conn.execute(upsert_stmt, records)



if __name__ == "__main__":
    import sys
    sys.path.insert(0, "../api")
    from waseet_api import start_api

    base_url = start_api()
    print("API on", base_url)

    records = fetch_all_customers(base_url)
    print("fetched", len(records), "customers")

    load_customers(records)
    print("loaded")
