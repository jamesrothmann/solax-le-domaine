# streamlit_app.py
# Complete Streamlit app that:
# - Fetches Daily yield kWh via Jina every hour 04:00–20:00 SAST
# - Logs to CSV with interval_kwh computed from deltas
# - At 21:00 SAST builds XML with hourly datapoints (interval=3600) using E_INT = interval_kwh
# - Pushes data via FTP into the root directory (no subdirectories)
# - Provides a black + light-green "hacker" UI
# - Includes buttons: fetch data manually, manually generate xml and upload via FTP
# - Shows FTP upload results in both UI and console
# - All secrets are read from .streamlit/secrets.toml
#
# Example .streamlit/secrets.toml
# [general]
# PLANT_NAME = "Solax Le Domaine Plant"
# viewer_passwords = ["optional-password"]
#
# [jina]
# JINA_URL   = "https://r.jina.ai/"
# JINA_TOKEN = "YOUR_JINA_TOKEN"
# SHARE_URL  = "https://your_public_page"
#
# [ftp]
# FTP_HOST     = "ftp.meteocontrol.de"    # from portal
# FTP_PORT     = 21                       # optional
# FTP_USERNAME = "provided_user"
# FTP_PASSWORD = "provided_pass"
# FTP_TLS      = true                     # true for FTPS, false for plain FTP
# FTP_COMPRESS = "none"                   # one of: none, gz, bz2, zlib
#
# Environment:
#   Python 3.10+
#   pip install streamlit requests pandas

import os
import re
import io
import csv
import bz2
import zlib
import gzip
import json
import time
import queue
import ftplib
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Iterable

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

import requests
import pandas as pd
import streamlit as st
import xml.etree.ElementTree as ET

# ------------------------------------------------------------
# Config and paths
# ------------------------------------------------------------
DATA_DIR = Path(os.getenv("SOLAX_APP_DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_FILE = DATA_DIR / "solax_daily_kwh_log.csv"
STATE_FILE = DATA_DIR / "state.json"
SNAPSHOT_MD = DATA_DIR / "solax_share_snapshot.md"
XML_OUT = DATA_DIR / "solax_le_domaine_hourly.xml"
LAST_PUSH_JSON = DATA_DIR / "last_push_result.json"

TZ_NAME = "Africa/Johannesburg"
TZ = ZoneInfo(TZ_NAME) if ZoneInfo else None

PLANT_NAME = st.secrets.get("PLANT_NAME", st.secrets.get("general", {}).get("PLANT_NAME", "Solax Le Domaine Plant"))

# Jina
JINA_URL   = st.secrets.get("JINA_URL",   st.secrets.get("jina", {}).get("JINA_URL", "https://r.jina.ai/"))
JINA_TOKEN = st.secrets.get("JINA_TOKEN", st.secrets.get("jina", {}).get("JINA_TOKEN"))
SHARE_URL  = st.secrets.get("SHARE_URL",  st.secrets.get("jina", {}).get("SHARE_URL"))

# FTP
FTP_HOST     = st.secrets.get("FTP_HOST",     st.secrets.get("ftp", {}).get("FTP_HOST"))
FTP_PORT     = int(st.secrets.get("FTP_PORT", st.secrets.get("ftp", {}).get("FTP_PORT", 21)))
FTP_USERNAME = st.secrets.get("FTP_USERNAME", st.secrets.get("ftp", {}).get("FTP_USERNAME"))
FTP_PASSWORD = st.secrets.get("FTP_PASSWORD", st.secrets.get("ftp", {}).get("FTP_PASSWORD"))
FTP_TLS      = bool(st.secrets.get("FTP_TLS", st.secrets.get("ftp", {}).get("FTP_TLS", True)))
FTP_COMPRESS = (st.secrets.get("FTP_COMPRESS", st.secrets.get("ftp", {}).get("FTP_COMPRESS", "none")) or "none").lower()

# Viewer auth
VIEWER_PASSWORDS = set(st.secrets.get("viewer_passwords", st.secrets.get("general", {}).get("viewer_passwords", [])))

# Approximation constants (optional in secrets; fall back to sane defaults)
INV_EFF = float(st.secrets.get("INV_EFF",  st.secrets.get("approx", {}).get("INV_EFF", 0.982)))
VAC     = float(st.secrets.get("VAC",      st.secrets.get("approx", {}).get("VAC",     245.0)))
PF      = float(st.secrets.get("PF",       st.secrets.get("approx", {}).get("PF",      1.0)))
VMPPT   = float(st.secrets.get("VMPPT",    st.secrets.get("approx", {}).get("VMPPT",   221.0)))

# Schedule
HOUR_WINDOW_START = 4
HOUR_WINDOW_END   = 20
XML_POST_HOUR     = 21

# Datalogger identity used in XML and filename
DL_VENDOR = "SolaX"
DL_SERIAL = "DL-PUBLIC-SHARE"
DEVICE_ID = "inverter-1"
DEVICE_UID = "INV-1"

# ------------------------------------------------------------
# Time helpers
# ------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_sast() -> datetime:
    if TZ is None:
        return datetime.fromtimestamp(time.time())
    return now_utc().astimezone(TZ)

# ------------------------------------------------------------
# Fetch and parse Daily yield via Jina
# ------------------------------------------------------------
def fetch_markdown_via_jina(page_url: str) -> str:
    if not JINA_TOKEN:
        raise RuntimeError("JINA_TOKEN is missing in secrets.")
    headers = {
        "Authorization": f"Bearer {JINA_TOKEN}",
        "Content-Type": "application/json",
        "DNT": "1",
        "X-Base": "final",
        "X-Engine": "browser",
        "X-No-Cache": "true",
        "X-Retain-Images": "none",
    }
    payload = {"url": page_url}
    r = requests.post(JINA_URL, headers=headers, data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    md = r.text
    SNAPSHOT_MD.write_text(md, encoding="utf-8")
    return md

def extract_daily_kwh_from_markdown(md: str) -> float:
    pat = re.compile(
        r"Daily\s*yield[^0-9\-]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*kWh",
        re.IGNORECASE
    )
    m = pat.search(md)
    if not m:
        number_str = None
        for line in md.splitlines():
            if "daily" in line.lower() and "yield" in line.lower():
                nums = re.findall(r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)", line)
                if nums:
                    number_str = nums[0]
                    break
        if not number_str:
            raise ValueError("Daily yield value not found in markdown")
    else:
        number_str = m.group(1)

    number_str = number_str.replace(",", "")
    return float(number_str)

# ------------------------------------------------------------
# CSV logging and interval computation
# ------------------------------------------------------------
def read_last_row(csv_path: Path):
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return None
        return df.tail(1).iloc[0].to_dict()
    except Exception:
        return None

def append_csv(timestamp_utc: datetime, kwh_today: float, interval_kwh: float):
    is_new = not CSV_FILE.exists()
    with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow(["timestamp_utc", "timestamp_sast", "daily_kwh", "interval_kwh"])
        ts_utc = timestamp_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        ts_sast = now_sast().replace(microsecond=0).isoformat()
        w.writerow([ts_utc, ts_sast, f"{kwh_today:.3f}", f"{interval_kwh:.3f}"])

def compute_interval(prev_daily_kwh: Optional[float], current_daily_kwh: float) -> float:
    if prev_daily_kwh is None:
        return current_daily_kwh
    diff = current_daily_kwh - prev_daily_kwh
    if diff < 0:
        return current_daily_kwh
    return diff

# ------------------------------------------------------------
# Pandas helpers for hourly XML aggregation
# ------------------------------------------------------------
def parse_csv() -> pd.DataFrame:
    if not CSV_FILE.exists():
        return pd.DataFrame(columns=["timestamp_utc", "timestamp_sast", "daily_kwh", "interval_kwh"])
    df = pd.read_csv(CSV_FILE)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    if TZ is not None:
        df["timestamp_sast_dt"] = df["timestamp_utc"].dt.tz_convert(TZ)
    else:
        df["timestamp_sast_dt"] = df["timestamp_utc"]
    return df

def rows_for_sast_day(target_day) -> pd.DataFrame:
    df = parse_csv()
    if df.empty:
        return df
    mask = (df["timestamp_sast_dt"].dt.date == target_day)
    day_df = df.loc[mask].copy()
    day_df = day_df.sort_values("timestamp_utc").reset_index(drop=True)
    return day_df

# ------------------------------------------------------------
# XML build with correct namespaces
# ------------------------------------------------------------
def build_mii_xml_hourly(day_df: pd.DataFrame) -> bytes:
    """
    Build an MII XML document with one datapoint per row in day_df.
    interval="3600".
    Emits:
      - E_INT  : interval_kwh (kWh)
      - P_AC   : avg AC power (W) from interval_kwh
      - P_DC   : est. DC power (W)
      - I_AC   : est. AC current (A)
      - I_DC   : est. DC current (A)
      - COS_PHI: assumed PF
      - U_AC1  : assumed AC volts
      - E_DAY  : cumulative Wh for the day
    """
    NS_MAIN   = "http://api.sspcdn.com/mii"
    NS_CONFIG = "http://api.sspcdn.com/mii/datalogger/configuration"
    NS_DATA   = "http://api.sspcdn.com/mii/datalogger/datapoints"

    ET.register_namespace("", NS_MAIN)
    ET.register_namespace("cfg", NS_CONFIG)
    ET.register_namespace("dp", NS_DATA)

    mii = ET.Element(ET.QName(NS_MAIN, "mii"),
                     attrib={"version": "2.0", "targetNamespace": NS_MAIN})
    datalogger = ET.SubElement(mii, ET.QName(NS_MAIN, "datalogger"))

    # --- configuration
    cfg = ET.SubElement(datalogger, ET.QName(NS_CONFIG, "configuration"))
    uuid_el = ET.SubElement(cfg, ET.QName(NS_CONFIG, "uuid"))
    ET.SubElement(uuid_el, ET.QName(NS_CONFIG, "vendor")).text = DL_VENDOR
    ET.SubElement(uuid_el, ET.QName(NS_CONFIG, "serial")).text = DL_SERIAL
    ET.SubElement(cfg, ET.QName(NS_CONFIG, "name")).text = PLANT_NAME
    ET.SubElement(cfg, ET.QName(NS_CONFIG, "firmware")).text = "n/a"
    ET.SubElement(cfg, ET.QName(NS_CONFIG, "next-scheduled-transfer")).text = \
        now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")

    devices = ET.SubElement(cfg, ET.QName(NS_CONFIG, "devices"))
    dev = ET.SubElement(devices, ET.QName(NS_CONFIG, "device"),
                        attrib={"type": "inverter", "id": DEVICE_ID})
    ET.SubElement(dev, ET.QName(NS_CONFIG, "uid")).text = DEVICE_UID

    # --- datapoints
    dps = ET.SubElement(datalogger, ET.QName(NS_DATA, "datapoints"))

    # cumulative day Wh for E_DAY
    df = day_df.copy()
    df["interval_kwh"] = pd.to_numeric(df["interval_kwh"], errors="coerce").fillna(0.0)
    df["cum_kwh"] = df["interval_kwh"].cumsum()
    df = df.sort_values("timestamp_utc")

    for _, row in df.iterrows():
        ts_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
        interval_kwh = float(row["interval_kwh"])
        cum_kwh = float(row["cum_kwh"])

        # Core transforms
        pac_w = interval_kwh * 1000.0                 # average W over the hour
        pdc_w = pac_w / INV_EFF if INV_EFF > 0 else pac_w
        iac_a = pac_w / (VAC * PF) if (VAC > 0 and PF > 0) else 0.0
        idc_a = pdc_w / VMPPT if VMPPT > 0 else 0.0
        e_day_wh = cum_kwh * 1000.0

        dp = ET.SubElement(dps, ET.QName(NS_DATA, "datapoint"), attrib={
            "interval": "3600",
            "timestamp": ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        dv = ET.SubElement(dp, ET.QName(NS_DATA, "device"), attrib={"id": DEVICE_ID})

        # Required original metric
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "E_INT", "v": f"{interval_kwh:.3f}"})

        # Additional approximations
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "P_AC", "v": f"{pac_w:.2f}"})
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "P_DC", "v": f"{pdc_w:.2f}"})
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "I_AC", "v": f"{iac_a:.3f}"})
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "I_DC", "v": f"{idc_a:.3f}"})
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "COS_PHI", "v": f"{PF:.3f}"})
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "U_AC1", "v": f"{VAC:.1f}"})
        ET.SubElement(dv, ET.QName(NS_DATA, "mv"),
                      attrib={"t": "E_DAY", "v": f"{e_day_wh:.0f}"})

    xml_bytes = ET.tostring(mii, encoding="utf-8", xml_declaration=True)
    XML_OUT.write_bytes(xml_bytes)
    return xml_bytes

# ------------------------------------------------------------
# FTP helpers
# ------------------------------------------------------------
def _compress_bytes(xml_bytes: bytes, mode: str) -> Tuple[bytes, str]:
    mode = (mode or "none").lower()
    if mode == "gz":
        return gzip.compress(xml_bytes), "xml.gz"
    if mode == "bz2":
        return bz2.compress(xml_bytes), "xml.bz2"
    if mode == "zlib":
        return zlib.compress(xml_bytes), "xml.zlib"
    return xml_bytes, "xml"

def _build_unique_filename(serial: str, ext: str) -> str:
    # Use UTC timestamp to guarantee uniqueness
    ts = now_utc().strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{serial}.{ext}"

def _ftp_connect() -> ftplib.FTP:
    host, port = FTP_HOST, FTP_PORT
    if not host or not FTP_USERNAME or not FTP_PASSWORD:
        raise RuntimeError("FTP credentials are missing in secrets.")
    if FTP_TLS:
        ftp = ftplib.FTP_TLS()
        ftp.connect(host=host, port=port, timeout=30)
        welcome = ftp.getwelcome()
        print(f"[FTP] Connected (FTPS). Welcome: {welcome}")
        ftp.auth()
        ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
        ftp.prot_p()  # secure data connection
    else:
        ftp = ftplib.FTP()
        ftp.connect(host=host, port=port, timeout=30)
        welcome = ftp.getwelcome()
        print(f"[FTP] Connected (FTP). Welcome: {welcome}")
        ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
    ftp.set_pasv(True)
    return ftp

def push_via_ftp(file_bytes: bytes, remote_filename: str) -> Tuple[bool, str]:
    # Spec: root directory only, no subdirectories
    if len(file_bytes) > 5 * 1024 * 1024:
        return False, "File size exceeds 5 MB after decompression limit"
    try:
        bio = io.BytesIO(file_bytes)
        ftp = _ftp_connect()
        # ensure we are at root
        try:
            ftp.cwd("/")
        except Exception:
            pass
        resp = ftp.storbinary(f"STOR {remote_filename}", bio)
        # ftplib.storbinary returns None on success; we can issue a NOOP to get a response string
        noop_resp = ""
        try:
            noop_resp = ftp.voidcmd("NOOP")
        except Exception:
            noop_resp = "226 Transfer complete (assumed)"
        try:
            ftp.quit()
        except Exception:
            ftp.close()
        msg = f"Upload ok: {remote_filename} | server: {noop_resp}"
        print(f"[FTP] {msg}")
        return True, msg
    except Exception as e:
        err = f"FTP upload failed: {e}"
        print(f"[FTP] {err}")
        return False, err

# ------------------------------------------------------------
# Jobs
# ------------------------------------------------------------
def run_fetch_job() -> dict:
    md = fetch_markdown_via_jina(SHARE_URL)
    daily_kwh = extract_daily_kwh_from_markdown(md)
    last_row = read_last_row(CSV_FILE)
    prev_kwh = float(last_row["daily_kwh"]) if last_row and "daily_kwh" in last_row else None
    interval = compute_interval(prev_kwh, daily_kwh)
    append_csv(now_utc(), daily_kwh, interval)
    return {"daily_kwh": daily_kwh, "interval_kwh": interval}

def manual_xml_and_push() -> Tuple[str, str]:
    day_df = rows_for_sast_day(now_sast().date())
    if day_df.empty:
        raise RuntimeError("No rows for today yet. Fetch at least once before generating XML.")
    xml_bytes = build_mii_xml_hourly(day_df)
    payload, ext = _compress_bytes(xml_bytes, FTP_COMPRESS)
    remote_name = _build_unique_filename(DL_SERIAL, ext)
    ok, msg = push_via_ftp(payload, remote_name)
    LAST_PUSH_JSON.write_text(json.dumps({
        "time": now_sast().isoformat(),
        "filename": remote_name,
        "ok": ok,
        "message": msg
    }, indent=2), encoding="utf-8")
    return remote_name, msg

# ------------------------------------------------------------
# State management and scheduler
# ------------------------------------------------------------
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"last_fetch_iso": None, "last_xml_sent_date": None}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")

def scheduler_loop(stop_event: threading.Event, status_queue: queue.Queue):
    state = load_state()

    while not stop_event.is_set():
        try:
            local_now = now_sast()
            hour = local_now.hour
            minute = local_now.minute
            today_str = local_now.date().isoformat()

            # hourly fetch at minute 0
            should_run_hourly = (HOUR_WINDOW_START <= hour <= HOUR_WINDOW_END) and (minute == 0)

            last_fetch_iso = state.get("last_fetch_iso")
            already_fetched_this_hour = False
            if last_fetch_iso:
                try:
                    last_fetch = datetime.fromisoformat(last_fetch_iso.replace("Z", "+00:00"))
                    last_fetch_local = last_fetch.astimezone(TZ) if TZ else last_fetch
                    already_fetched_this_hour = (
                        last_fetch_local.year == local_now.year and
                        last_fetch_local.month == local_now.month and
                        last_fetch_local.day == local_now.day and
                        last_fetch_local.hour == local_now.hour
                    )
                except Exception:
                    already_fetched_this_hour = False

            if should_run_hourly and not already_fetched_this_hour:
                result = run_fetch_job()
                state["last_fetch_iso"] = now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")
                save_state(state)
                status_queue.put({"type": "fetch", "time": local_now.isoformat(), "result": result})

            # nightly XML build and FTP push
            last_sent_date = state.get("last_xml_sent_date")
            if hour == XML_POST_HOUR and minute == 0 and last_sent_date != today_str:
                day_df = rows_for_sast_day(local_now.date())
                if not day_df.empty:
                    try:
                        xml_bytes = build_mii_xml_hourly(day_df)
                        payload, ext = _compress_bytes(xml_bytes, FTP_COMPRESS)
                        remote_name = _build_unique_filename(DL_SERIAL, ext)
                        ok, msg = push_via_ftp(payload, remote_name)
                        LAST_PUSH_JSON.write_text(json.dumps({
                            "time": now_sast().isoformat(),
                            "filename": remote_name,
                            "ok": ok,
                            "message": msg
                        }, indent=2), encoding="utf-8")
                        if ok:
                            state["last_xml_sent_date"] = today_str
                            save_state(state)
                        status_queue.put({"type": "ftp", "time": local_now.isoformat(), "ok": ok, "filename": remote_name, "msg": msg[:300]})
                    except Exception as e:
                        status_queue.put({"type": "ftp_error", "time": local_now.isoformat(), "error": str(e)})

            stop_event.wait(20)
        except Exception as e:
            status_queue.put({"type": "loop_error", "time": now_sast().isoformat(), "error": str(e)})
            stop_event.wait(20)

# ------------------------------------------------------------
# Cache the background thread so it persists across reruns
# ------------------------------------------------------------
@st.cache_resource
def start_scheduler():
    stop_event = threading.Event()
    status_q = queue.Queue()
    t = threading.Thread(target=scheduler_loop, args=(stop_event, status_q), daemon=True)
    t.start()
    return stop_event, status_q

# ------------------------------------------------------------
# UI helpers
# ------------------------------------------------------------
def apply_hacker_theme():
    st.markdown("""
    <style>
      :root {
        --bg: #0b0f0c;
        --card: #0f1511;
        --fg: #b7ffbf;
        --muted: #57d364;
        --accent: #23c552;
      }
      .stApp { background: var(--bg) !important; color: var(--fg) !important; }
      .stMarkdown, .stText, .stDataFrame, .stTable, .stTextInput, .stButton { color: var(--fg) !important; }
      .block-container { padding-top: 1.5rem; }
      div[data-baseweb="input"] input { color: var(--fg) !important; background: #0d140f !important; }
      .stButton>button {
        background: #0d140f; border: 1px solid var(--muted); color: var(--fg);
        border-radius: 10px; padding: 0.5rem 1rem;
      }
      .stButton>button:hover { border-color: var(--accent); }
      .css-1v0mbdj, .ef3psqc12 { background: var(--card) !important; }
      h1,h2,h3 {
        color: var(--accent) !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      }
      .metric { background: var(--card); padding: 10px 14px; border: 1px solid var(--muted); border-radius: 12px; }
      .stDataFrame { border: 1px solid var(--muted); border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

def check_password() -> bool:
    if not VIEWER_PASSWORDS:
        return True
    pw = st.sidebar.text_input("Viewer password", type="password")
    ok = pw in VIEWER_PASSWORDS
    if not ok and pw:
        st.sidebar.error("Invalid password")
    return ok

def render_dashboard(status_q: queue.Queue):
    st.title("⚡ Solax Hourly Intervals — Hacker Panel")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"<div class='metric'><b>Plant</b><br>{PLANT_NAME}</div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div class='metric'><b>Local time</b><br>{now_sast().strftime('%Y-%m-%d %H:%M:%S')}</div>", unsafe_allow_html=True)
    with col3:
        st.markdown(f"<div class='metric'><b>Run window</b><br>{HOUR_WINDOW_START:02d}:00 to {HOUR_WINDOW_END:02d}:00 SAST</div>", unsafe_allow_html=True)

    st.divider()

    # Controls
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("fetch data manually"):
            try:
                result = run_fetch_job()
                st.success(f"Fetched. Daily={result['daily_kwh']:.3f} kWh  Interval={result['interval_kwh']:.3f} kWh")
            except Exception as e:
                st.error(f"Manual fetch failed: {e}")
    with c2:
        if st.button("generate xml and upload via ftp"):
            try:
                filename, msg = manual_xml_and_push()
                st.success(f"Uploaded: {filename}. {msg}")
            except Exception as e:
                st.error(f"Manual FTP push failed: {e}")
    with c3:
        if st.button("test ftp login"):
            try:
                ftp = _ftp_connect()
                try:
                    pwd = ftp.pwd()
                except Exception:
                    pwd = "/"
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
                st.success(f"FTP login ok. Working dir: {pwd}")
            except Exception as e:
                st.error(f"FTP login failed: {e}")

    # Data table
    if CSV_FILE.exists():
        df = pd.read_csv(CSV_FILE)
        if "timestamp_utc" in df.columns:
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
            df = df.sort_values("timestamp_utc", ascending=False).reset_index(drop=True)
        st.subheader("Latest logs (newest first)")
        st.dataframe(df, use_container_width=True, height=440)
        if not df.empty:
            st.caption(
                f"Rows: {len(df)} • Last log at {df.iloc[0]['timestamp_sast'] if 'timestamp_sast' in df.columns else df.iloc[0]['timestamp_utc']}"
            )
    else:
        st.info("No CSV yet. Scheduler will log at the next scheduled hour.")

    st.divider()

    st.subheader("Scheduler events")
    events = []
    while not status_q.empty():
        events.append(status_q.get())
    if events:
        for ev in reversed(events[-100:]):
            if ev["type"] == "fetch":
                st.write(f"Fetched at {ev['time']} — Daily {ev['result']['daily_kwh']:.3f} kWh — Interval {ev['result']['interval_kwh']:.3f} kWh")
            elif ev["type"] == "ftp":
                st.write(f"FTP push at {ev['time']} — ok={ev['ok']} — {ev['filename']} — {ev['msg']}")
            else:
                st.write(f"{ev['type']} at {ev['time']} — {ev.get('error','')[:200]}")
    else:
        st.caption("No scheduler messages yet.")

    st.divider()
    st.subheader("Transfer info")
    st.markdown(f"- FTP host: `{FTP_HOST}:{FTP_PORT}` • TLS: `{FTP_TLS}` • Compression: `{FTP_COMPRESS}`")
    if XML_OUT.exists():
        st.markdown(f"- Last XML file: `{XML_OUT}` • Size: {XML_OUT.stat().st_size} bytes")
        with XML_OUT.open("rb") as f:
            preview = f.read(800).decode("utf-8", errors="ignore")
        st.code(preview + ("\n..." if XML_OUT.stat().st_size > 800 else ""), language="xml")
        st.download_button("Download last XML", XML_OUT.read_bytes(), file_name=XML_OUT.name, mime="application/xml")
    if LAST_PUSH_JSON.exists():
        st.caption("Last push result:")
        st.code(LAST_PUSH_JSON.read_text(encoding="utf-8")[:2000], language="json")

# ------------------------------------------------------------
# App entry
# ------------------------------------------------------------
def apply_hacker_theme():
    st.markdown("""
    <style>
      :root {
        --bg: #0b0f0c;
        --card: #0f1511;
        --fg: #b7ffbf;
        --muted: #57d364;
        --accent: #23c552;
      }
      .stApp { background: var(--bg) !important; color: var(--fg) !important; }
      .stMarkdown, .stText, .stDataFrame, .stTable, .stTextInput, .stButton { color: var(--fg) !important; }
      .block-container { padding-top: 1.5rem; }
      div[data-baseweb="input"] input { color: var(--fg) !important; background: #0d140f !important; }
      .stButton>button {
        background: #0d140f; border: 1px solid var(--muted); color: var(--fg);
        border-radius: 10px; padding: 0.5rem 1rem;
      }
      .stButton>button:hover { border-color: var(--accent); }
      .css-1v0mbdj, .ef3psqc12 { background: var(--card) !important; }
      h1,h2,h3 {
        color: var(--accent) !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      }
      .metric { background: var(--card); padding: 10px 14px; border: 1px solid var(--muted); border-radius: 12px; }
      .stDataFrame { border: 1px solid var(--muted); border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

apply_hacker_theme()
stop_event, status_q = start_scheduler()

if not check_password():
    st.stop()

render_dashboard(status_q)
