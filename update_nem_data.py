# ============================================
# NEM DATABASE UPDATE SCRIPT (GitHub Actions)
# Downloads nem_data.db from Google Drive,
# updates it with AEMO monthly price/demand data,
# then uploads the updated DB back to Google Drive.
# ============================================

import io
import os
import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


# --------------------------------------------
# 1. Settings
# --------------------------------------------

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

BASE_URL = (
    "https://www.aemo.com.au/-/media/files/electricity/nem/priceanddemand/price_and_demand"
)

# Local working copy on the GitHub Actions runner
DB_LOCAL_PATH = "nem_data.db"

# Prefer DRIVE_FILE_ID if you know it. Much more reliable.
DRIVE_FILE_ID = os.getenv("DRIVE_FILE_ID", "").strip()

# Optional fallback if you want to search by filename instead of file ID
DRIVE_FILE_NAME = os.getenv("DRIVE_FILE_NAME", "nem_data.db").strip()

# Google Drive scope
DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive"]

# Request timeout
REQUEST_TIMEOUT_SECONDS = 60


# --------------------------------------------
# 2. Google Drive helpers
# --------------------------------------------

def get_drive_service():
    """
    Uses Application Default Credentials provided by:
    google-github-actions/auth@v2 in your workflow.
    """
    credentials, _ = google.auth.default(scopes=DRIVE_SCOPE)
    return build("drive", "v3", credentials=credentials)


def find_drive_file_id(service, filename: str) -> Optional[str]:
    """
    Finds a file by name in the service account's visible Drive scope.
    This only works if the file has been shared with the service account.
    """
    query = f"name = '{filename}' and trashed = false"
    results = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)", pageSize=10)
        .execute()
    )
    files = results.get("files", [])
    if not files:
        return None
    if len(files) > 1:
        print(f"Warning: multiple files named {filename!r} found. Using the first one.")
    return files[0]["id"]


def resolve_drive_file_id(service) -> str:
    if DRIVE_FILE_ID:
        return DRIVE_FILE_ID

    file_id = find_drive_file_id(service, DRIVE_FILE_NAME)
    if file_id:
        print(f"Resolved Drive file ID from name {DRIVE_FILE_NAME!r}: {file_id}")
        return file_id

    raise RuntimeError(
        "Could not determine Google Drive file ID. "
        "Set DRIVE_FILE_ID in your GitHub Actions environment, "
        "or share the file with the service account and set DRIVE_FILE_NAME."
    )


def download_db_from_drive(service, file_id: str, local_path: str) -> None:
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(local_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"Download progress: {int(status.progress() * 100)}%")

    fh.close()
    print(f"Downloaded database to {local_path}")


def upload_db_to_drive(service, file_id: str, local_path: str) -> None:
    media = MediaFileUpload(local_path, mimetype="application/x-sqlite3", resumable=True)
    updated = (
        service.files()
        .update(fileId=file_id, media_body=media, fields="id, name")
        .execute()
    )
    print(f"Uploaded updated database back to Google Drive: {updated.get('name')} ({updated.get('id')})")


# --------------------------------------------
# 3. SQLite helpers
# --------------------------------------------

def ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Creates the table if it does not exist.
    Assumes one row per (SETTLEMENTDATE, REGIONID).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS regional_market_data (
            SETTLEMENTDATE TEXT NOT NULL,
            REGIONID TEXT NOT NULL,
            OPERATIONAL_DEMAND REAL,
            RRP REAL,
            PRIMARY KEY (SETTLEMENTDATE, REGIONID)
        )
    """)
    conn.commit()


def get_latest_timestamp(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(SETTLEMENTDATE) FROM regional_market_data").fetchone()
    return row[0] if row else None


def parse_latest_timestamp(latest: Optional[str]) -> tuple[int, int]:
    if not latest:
        return 1999, 1

    dt = pd.to_datetime(latest, errors="coerce")
    if pd.isna(dt):
        raise RuntimeError(f"Could not parse latest timestamp in DB: {latest!r}")

    return int(dt.year), int(dt.month)


# --------------------------------------------
# 4. AEMO download helpers
# --------------------------------------------

def month_list(start_year: int, start_month: int) -> list[tuple[int, int]]:
    """
    Builds a list of months to check from the last-loaded month to the current month.
    INSERT OR IGNORE prevents duplicates if the first month is already partially loaded.
    """
    now = datetime.utcnow()
    months = []

    y = start_year
    m = start_month

    while (y < now.year) or (y == now.year and m <= now.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    return months


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans column names, parses timestamps, and standardizes the columns we need.
    """
    df.columns = [str(c).strip().upper() for c in df.columns]

    required = {"SETTLEMENTDATE", "REGIONID", "TOTALDEMAND", "RRP"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in CSV: {sorted(missing)}")

    out = df[["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND", "RRP"]].copy()

    # AEMO dates can be parsed more safely with dayfirst=True
    out["SETTLEMENTDATE"] = pd.to_datetime(
        out["SETTLEMENTDATE"], errors="coerce", dayfirst=True
    )

    # Drop rows where timestamp failed
    out = out.dropna(subset=["SETTLEMENTDATE"])

    # Standardize datetime format for SQLite text storage
    out["SETTLEMENTDATE"] = out["SETTLEMENTDATE"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Numeric cleanup
    out["TOTALDEMAND"] = pd.to_numeric(out["TOTALDEMAND"], errors="coerce")
    out["RRP"] = pd.to_numeric(out["RRP"], errors="coerce")

    # Rename to match DB column name
    out = out.rename(columns={"TOTALDEMAND": "OPERATIONAL_DEMAND"})

    return out


def download_month_region(year: int, month: int, region: str) -> Optional[pd.DataFrame]:
    ym = f"{year}{month:02d}"
    filename = f"PRICE_AND_DEMAND_{ym}_{region}.csv"
    url = f"{BASE_URL}/{filename}"

    print(f"Downloading: {filename}")

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        print(f"Request failed for {filename}: {exc}")
        return None

    if response.status_code != 200:
        print(f"File not available: {filename} (status {response.status_code})")
        return None

    try:
        df = pd.read_csv(io.StringIO(response.text))
        return normalize_dataframe(df)
    except Exception as exc:
        print(f"CSV parse failed for {filename}: {exc}")
        return None


def insert_dataframe(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows_before = conn.total_changes

    conn.executemany(
        """
        INSERT OR IGNORE INTO regional_market_data
        (SETTLEMENTDATE, REGIONID, OPERATIONAL_DEMAND, RRP)
        VALUES (?, ?, ?, ?)
        """,
        df[["SETTLEMENTDATE", "REGIONID", "OPERATIONAL_DEMAND", "RRP"]]
        .itertuples(index=False, name=None),
    )
    conn.commit()

    return conn.total_changes - rows_before


# --------------------------------------------
# 5. Main
# --------------------------------------------

def main():
    print("Starting NEM database update")

    # Authenticate to Google Drive using OIDC/ADC
    drive_service = get_drive_service()

    # Resolve and download database
    drive_file_id = resolve_drive_file_id(drive_service)
    print(f"Using Drive file ID: {drive_file_id}")

    download_db_from_drive(drive_service, drive_file_id, DB_LOCAL_PATH)

    # Update SQLite locally
    conn = sqlite3.connect(DB_LOCAL_PATH)
    try:
        ensure_schema(conn)

        latest = get_latest_timestamp(conn)
        print(f"Latest timestamp in DB: {latest}")

        latest_year, latest_month = parse_latest_timestamp(latest)
        months = month_list(latest_year, latest_month)
        print(f"Months to check: {months}")

        total_rows_inserted = 0

        for year, month in months:
            for region in REGIONS:
                df = download_month_region(year, month, region)
                if df is None or df.empty:
                    continue

                inserted = insert_dataframe(conn, df)
                total_rows_inserted += inserted
                print(f"Inserted {inserted} rows for {year}-{month:02d} {region}")

        # Final DB summary
        total_rows = conn.execute("SELECT COUNT(*) FROM regional_market_data").fetchone()[0]
        min_date, max_date = conn.execute(
            "SELECT MIN(SETTLEMENTDATE), MAX(SETTLEMENTDATE) FROM regional_market_data"
        ).fetchone()

        print("\nDatabase summary")
        print(f"Rows inserted this run: {total_rows_inserted}")
        print(f"Total rows: {total_rows}")
        print(f"Date range: {min_date} to {max_date}")

        print("\nRows by region:")
        for row in conn.execute("""
            SELECT REGIONID, COUNT(*)
            FROM regional_market_data
            GROUP BY REGIONID
            ORDER BY REGIONID
        """):
            print(row)

    finally:
        conn.close()

    # Upload updated DB back to Google Drive
    upload_db_to_drive(drive_service, drive_file_id, DB_LOCAL_PATH)

    print("\nDatabase update complete.")


if __name__ == "__main__":
    main()
