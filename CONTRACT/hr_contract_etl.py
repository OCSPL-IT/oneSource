# CONTRACT/hr_contract_etl.py

import logging, urllib
from datetime import datetime, timedelta, time
from typing import List, Dict
import pandas as pd
import pytz
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Date, Float
from zk import ZK

IN_PUNCH_IPS  = ["192.168.0.28", "192.168.0.25"]
OUT_PUNCH_IPS = ["192.168.0.29", "192.168.0.27"]
DEVICE_PORT  = 4370
IST          = pytz.timezone("Asia/Kolkata")

GRACE_MIN     = 60
OVERNIGHT_MAX = time(7, 15)
SHIFT_MAP = [
    ("1st Shift (07:00AM-15:00PM)", time(7),  time(15), False),
    ("General shift (09:00AM-18:00PM)",   time(9),  time(18), False),
    ("2nd Shift (15:00PM-23:00PM)", time(15), time(23), False),
    ("4th Shift (19:00PM-07:00AM)", time(19), time(7),  True),
    ("Night Shift (23:00PM-07:00AM)",     time(23), time(7),  True),
    ("3rd Shift (07:00AM-19:00PM)", time(7),  time(19), False),
]

params = urllib.parse.quote_plus(
    "DRIVER=ODBC Driver 17 for SQL Server;"
    "SERVER=TEST-SERVER;"
    "DATABASE=oneSource_Live;"
    "Trusted_Connection=yes;"
)
ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={params}",
                       fast_executemany=True)

def fetch_raw() -> pd.DataFrame:
    rows: List[Dict] = []

    devices = [(ip, "IN") for ip in IN_PUNCH_IPS] + [(ip, "OUT") for ip in OUT_PUNCH_IPS]

    for ip, flag in devices:
        zk = ZK(ip=ip, port=DEVICE_PORT, timeout=10, verbose=False)
        conn = None
        try:
            conn = zk.connect()
            conn.disable_device()

            for rec in conn.get_attendance() or []:
                if rec and rec.user_id and rec.timestamp:
                    rows.append({
                        "employee_id": str(rec.user_id),
                        "punch_time": IST.localize(rec.timestamp),
                        "flag": flag,
                    })
        except Exception:
            logging.exception("Device %s (%s) error", flag, ip)

        finally:
            try:
                if conn:
                    conn.enable_device()
                    conn.disconnect()
            except Exception:
                pass
    df = pd.DataFrame(rows)
    # Optional but recommended (prevents duplicates if same log appears from multiple devices)
    if not df.empty:
        df = df.drop_duplicates(subset=["employee_id", "punch_time", "flag"])
    return df


def daily_pairs(df: pd.DataFrame) -> pd.DataFrame:
    df["work_date"] = df.apply(
        lambda r: r.punch_time.date() - timedelta(days=1)
        if r.flag=="OUT" and r.punch_time.time() <= OVERNIGHT_MAX
        else r.punch_time.date(), axis=1)
    first_in = (df[df.flag=="IN"]
                .groupby(["employee_id","work_date"])["punch_time"]
                .min().rename("in_time").reset_index())
    last_out = (df[df.flag=="OUT"]
                .groupby(["employee_id","work_date"])["punch_time"]
                .max().rename("out_time").reset_index())
    return pd.merge(first_in, last_out,
                    on=["employee_id","work_date"], how="left")

def _match(in_ts, out_ts, s, e, overnight):
    start = datetime.combine(in_ts.date(), s, tzinfo=IST)
    end   = (datetime.combine(in_ts.date()+timedelta(days=1), e, tzinfo=IST)
             if overnight and e <= s else datetime.combine(in_ts.date(), e, tzinfo=IST))
    if not (start - timedelta(minutes=GRACE_MIN) <= in_ts <= start + timedelta(minutes=GRACE_MIN)):
        return False
    if pd.isna(out_ts):
        return True
    return end - timedelta(minutes=GRACE_MIN) <= out_ts <= end + timedelta(minutes=GRACE_MIN)

def choose_shift(in_ts, out_ts):
    wins = [(abs((in_ts-datetime.combine(in_ts.date(), s, tzinfo=IST)).total_seconds()), lbl)
            for lbl,s,e,o in SHIFT_MAP if _match(in_ts,out_ts,s,e,o)]
    if wins:
        return min(wins,key=lambda x:x[0])[1]
    if time(9) <= in_ts.time() < time(12):
        return "General (09:00AM-18:00PM)"
    return "Unknown"

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    shift, dur, ot, dot = [], [], [], []
    for r in df.itertuples(index=False):
        s = choose_shift(r.in_time, r.out_time); shift.append(s)
        if pd.notna(r.out_time):
            mins = int((r.out_time - r.in_time).total_seconds()//60)
            h,m = divmod(mins,60); dur.append(f"{h:02d}:{m:02d}")
            ot.append(round(max(0,mins-480)/60,2))
            dot.append(round(max(0,mins-720)/60,2))
        else:
            dur.append(None); ot.append(None); dot.append(None)
    df["shift"] = shift; df["work_hhmm"] = dur
    df["ot_hours"] = ot; df["double_ot_hours"] = dot
    df["in_date"] = df["in_time"].dt.date
    df["in_time"] = df["in_time"].dt.strftime("%H:%M:%S")
    df["out_date"] = df["out_time"].dt.date.where(df.out_time.notna(), None)
    df["out_time"] = df["out_time"].dt.strftime("%H:%M:%S").where(df.out_time.notna(), None)
    return df

def run_hr_contract_etl():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)8s | %(message)s")

    today = datetime.now(IST).date()
    yesterday = today - timedelta(days=1)
    dateset = {today, yesterday}

    # 1. delete existing two-day slice
    delete_sql = text("""
        DELETE FROM dbo.hr_contract
        WHERE work_date IN (:d1, :d2)
    """)
    with ENGINE.begin() as conn:
        rows_del = conn.execute(delete_sql, {"d1": str(yesterday), "d2": str(today)}).rowcount
    logging.info("Deleted %d existing rows for %s and %s", rows_del, yesterday, today)

    # 2. fetch -> pair -> enrich
    raw_df   = fetch_raw()
    daily_df = daily_pairs(raw_df)
    final_df = enrich(daily_df)
    final_df = final_df[final_df.work_date.isin(dateset)]

    # 3. append back in
    dtype_map = {
        "employee_id":       String(50),
        "shift":             String(100),
        "in_time":           String(12),
        "out_time":          String(12),
        "work_hhmm":         String(8),
        "work_date":         Date(),
        "in_date":           Date(),
        "out_date":          Date(),
        "ot_hours":          Float(precision=2),
        "double_ot_hours":   Float(precision=2),
    }
    final_df.to_sql("hr_contract", schema="dbo", con=ENGINE,
                    if_exists="append", index=False,
                    dtype=dtype_map, chunksize=1000)

    print(f"✅  inserted {len(final_df)} rows for {yesterday} & {today}")

