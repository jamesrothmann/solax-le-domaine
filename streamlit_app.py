# streamlit_app.py
# Complete Streamlit app that:
# - Fetches Daily yield kWh via Jina every hour 04:00–20:00 SAST
# - Logs to CSV with interval_kwh computed from deltas
# - At 21:00 SAST builds XML with hourly datapoints (interval=3600) using E_INT = interval_kwh
# - Posts XML to meteocontrol HTTPS endpoint
# - Provides a black + light-green “hacker” UI
# - Includes buttons: fetch data manually, manually generate xml and transfer data
# - All secrets are read from .streamlit/secrets.toml

import os
import re
import csv
import json
import time
import queue
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

import requests
import pandas as pd
import streamlit as st
import xml.etree.ElementTree as ET

# ------------------------------------------------------------
# Config and paths (secure values via st.secrets)
# ------------------------------------------------------------
DATA_DIR = Path(os.getenv("SOLAX_APP_DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_FILE = DATA_DIR / "solax_daily_kwh_log.csv"
STATE_FILE = DATA_DIR / "state.json"
SNAPSHOT_MD = DATA_DIR / "solax_share_snapshot.md"
XML_OUT = DATA_DIR / "solax_le_domaine_hourly.xml"

TZ_NAME = "Africa/Johannesburg"

PLANT_NAME = st.secrets.get("PLANT_NAME", "Solax Le Domaine Plant")

# Jina browser API
JINA_URL = st.secrets.get("JINA_URL", "https://r.jina.ai/")
JINA_TOKEN = st.secrets["JINA_TOKEN"]           # required
SHARE_URL = st.secrets["SHARE_URL"]             # required

# Meteocontrol MII
MII_MODE = st.secrets.get("MII_MODE", "import").lower()   # "import" or "validation"
MII_API_KEY = st.secrets["MII_API_KEY"]                   # required
MII_IMPORT_ENDPOINT = "https://mii.meteocontrol.de/v2/"
MII_VALIDATION_ENDPOINT = "https://mii.meteocontrol.de/v2-validation/"
MII_ENDPOINT = MII_IMPORT_ENDPOINT if MII_MODE == "import" else MII_VALIDATION_ENDPOINT

# Viewer auth (viewer-only; scheduler runs regardless)
VIEWER_PASSWORDS = set(st.secrets.get("viewer_passwords", []))

# Scheduled hours in SAST
HOUR_WINDOW_START = 4    # inclusive
HOUR_WINDOW_END = 20     # inclusive
XML_POST_HOUR = 21       # 21:00 SAST

# ------------------------------------------------------------
# Time helpers
# ------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_sast() -> datetime:
    if ZoneInfo is None:
        # best effort fallback
        return datetime.fromtimestamp(time.time())
    return now_utc().astimezone(ZoneInfo(TZ_NAME))

# ------------------------------------------------------------
# Fetch and parse Daily yield via Jina (markdown snapshot)
# ------------------------------------------------------------
def fetch_markdown_via_jina(page_url: str) -> str:
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
    # Primary: "Daily yield ... <number> kWh"
    pat = re.compile(
        r"Daily\s*yield[^0-9\-]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*kWh",
        re.IGNORECASE
    )
    m = pat.search(md)
    if not m:
        # Fallback: first number on the same line as "Daily yield"
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
        # new day rollover
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
    if ZoneInfo is not None:
        df["timestamp_sast_dt"] = df["timestamp_utc"].dt.tz_convert(ZoneInfo(TZ_NAME))
    else:
        df["timestamp_sast_dt"] = df["timestamp_utc"]
    return df

def rows_for_sast_day(target_day) -> pd.DataFrame:
    """
    Return rows whose local SAST calendar date equals target_day (date object).
    Sorted ascending for chronological datapoints.
    """
    df = parse_csv()
    if df.empty:
        return df
    mask = (df["timestamp_sast_dt"].dt.date == target_day)
    day_df = df.loc[mask].copy()
    day_df = day_df.sort_values("timestamp_utc").reset_index(drop=True)
    return day_df

# ------------------------------------------------------------
# XML build and HTTPS post (hourly intervals)
# ------------------------------------------------------------
def build_mii_xml_hourly(day_df: pd.DataFrame) -> bytes:
    """
    Build an MII XML document with one datapoint per hour in day_df.
    interval="3600" and mv t="E_INT" with value in kWh for that hour.
    """
    NS_MAIN   = "http://api.sspcdn.com/mii"
    NS_CONFIG = "http://api.sspcdn.com/mii/datalogger/configuration"
    NS_DATA   = "http://api.sspcdn.com/mii/datalogger/datapoints"

    ET.register_namespace("", NS_MAIN)
    mii = ET.Element(ET.QName(NS_MAIN, "mii"), attrib={"version": "2.0", "targetNamespace": NS_MAIN})
    datalogger = ET.SubElement(mii, "datalogger")

    ET.register_namespace("", NS_CONFIG)
    cfg = ET.SubElement(datalogger, ET.QName(NS_CONFIG, "configuration"))
    uuid_el = ET.SubElement(cfg, "uuid")
    ET.SubElement(uuid_el, "vendor").text = "SolaX"
    ET.SubElement(uuid_el, "serial").text = "DL-PUBLIC-SHARE"
    ET.SubElement(cfg, "name").text = PLANT_NAME
    ET.SubElement(cfg, "firmware").text = "n/a"
    ET.SubElement(cfg, "next-scheduled-transfer").text = now_utc().replace(microsecond=0).isoformat().replace("+00:00","Z")

    devices = ET.SubElement(cfg, "devices")
    dev = ET.SubElement(devices, "device", attrib={"type": "inverter", "id": "inverter-1"})
    ET.SubElement(dev, "uid").text = "INV-1"

    ET.register_namespace("", NS_DATA)
    dps = ET.SubElement(datalogger, ET.QName(NS_DATA, "datapoints"))

    for _, row in day_df.iterrows():
        ts_utc: pd.Timestamp = row["timestamp_utc"]
        interval_kwh = float(row["interval_kwh"])
        dp = ET.SubElement(dps, "datapoint", attrib={
            "interval": "3600",
            "timestamp": ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        dv = ET.SubElement(dp, "device", attrib={"id": "inverter-1"})
        # Interval energy for that hour in kWh
        ET.SubElement(dv, "mv", attrib={"t": "E_INT", "v": f"{interval_kwh:.3f}"})

    xml_bytes = ET.tostring(mii, encoding="utf-8", xml_declaration=True)
    XML_OUT.write_bytes(xml_bytes)
    return xml_bytes

def post_xml_to_mii(xml_payload: bytes) -> tuple[int, str]:
    headers = {
        "Content-Type": "application/xml; charset=utf-8",
        "api-key": MII_API_KEY,
    }
    try:
        r = requests.post(MII_ENDPOINT, headers=headers, data=xml_payload, timeout=60)
        print(f"[MII] POST {MII_ENDPOINT} -> {r.status_code} {r.text[:200]}")
        return r.status_code, r.text[:1000]
    except Exception as e:
        print(f"[MII] POST {MII_ENDPOINT} failed: {e}")
        return 0, str(e)

# ------------------------------------------------------------
# State management for scheduler
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

def manual_xml_and_transfer() -> tuple[int, str]:
    # Build for today's SAST day
    day_df = rows_for_sast_day(now_sast().date())
    if day_df.empty:
        raise RuntimeError("No rows for today yet. Fetch at least once before generating XML.")
    xml_bytes = build_mii_xml_hourly(day_df)
    code, body = post_xml_to_mii(xml_bytes)
    print(f"[MII] Manual transfer -> {code} {body[:200]}")
    return code, body

# ------------------------------------------------------------
# Scheduler loop
# ------------------------------------------------------------
def scheduler_loop(stop_event: threading.Event, status_queue: queue.Queue):
    state = load_state()

    while not stop_event.is_set():
        try:
            local_now = now_sast()
            hour = local_now.hour
            minute = local_now.minute
            today_str = local_now.date().isoformat()

            # Hourly run at minute 0 within the window
            should_run_hourly = (HOUR_WINDOW_START <= hour <= HOUR_WINDOW_END) and (minute == 0)

            last_fetch_iso = state.get("last_fetch_iso")
            already_fetched_this_hour = False
            if last_fetch_iso:
                try:
                    last_fetch = datetime.fromisoformat(last_fetch_iso.replace("Z", "+00:00"))
                    last_fetch_local = last_fetch.astimezone(ZoneInfo(TZ_NAME)) if ZoneInfo else last_fetch
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

            # At 21:00 SAST, build hourly XML for today and post once
            last_sent_date = state.get("last_xml_sent_date")
            if hour == XML_POST_HOUR and minute == 0 and last_sent_date != today_str:
                day_df = rows_for_sast_day(local_now.date())
                if not day_df.empty:
                    try:
                        xml_bytes = build_mii_xml_hourly(day_df)
                        code, body = post_xml_to_mii(xml_bytes)
                        if code == 202:
                            state["last_xml_sent_date"] = today_str
                            save_state(state)
                        status_queue.put({"type": "xml", "time": local_now.isoformat(), "status": code, "body": body[:300]})
                    except Exception as e:
                        status_queue.put({"type": "xml_error", "time": local_now.isoformat(), "error": str(e)})

            # Sleep in small steps so stop_event is responsive
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

    c1, c2 = st.columns(2)
    with c1:
        if st.button("fetch data manually"):
            try:
                result = run_fetch_job()
                st.success(f"Fetched. Daily={result['daily_kwh']:.3f} kWh  Interval={result['interval_kwh']:.3f} kWh")
            except Exception as e:
                st.error(f"Manual fetch failed: {e}")
    with c2:
        if st.button("manually generate xml and transfer data"):
            try:
                code, body = manual_xml_and_transfer()
                if code == 202:
                    st.success(f"XML posted. HTTP {code}")
                else:
                    st.warning(f"Posted XML. HTTP {code}. Response preview: {body[:200]}")
            except Exception as e:
                st.error(f"Manual XML transfer failed: {e}")

    # Data table (newest first)
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
        for ev in reversed(events[-80:]):
            if ev["type"] == "fetch":
                st.write(
                    f"Fetched at {ev['time']} — Daily {ev['result']['daily_kwh']:.3f} kWh — Interval {ev['result']['interval_kwh']:.3f} kWh"
                )
            elif ev["type"] == "xml":
                st.write(f"XML posted at {ev['time']} — HTTP {ev['status']}")
            else:
                st.write(f"{ev['type']} at {ev['time']} — {ev.get('error','')[:200]}")
    else:
        st.caption("No scheduler messages yet.")

    st.divider()
    st.subheader("XML transfer")
    st.markdown(f"- Mode: **{MII_MODE}**  • Endpoint: `{MII_ENDPOINT}`")
    if XML_OUT.exists():
        st.markdown(f"- Last XML file: `{XML_OUT}`  • Size: {XML_OUT.stat().st_size} bytes")
        st.download_button("Download last XML", XML_OUT.read_bytes(), file_name=XML_OUT.name, mime="application/xml")

# ------------------------------------------------------------
# App entry
# ------------------------------------------------------------
apply_hacker_theme()
stop_event, status_q = start_scheduler()

# Gate only the viewer. Scheduler is always running.
if not check_password():
    st.stop()

render_dashboard(status_q)
