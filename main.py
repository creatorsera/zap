# Zap Scraper — 2025 Premium Edition
import streamlit as st
import pandas as pd
import re
import os
import uuid
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Optional Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except:
    SELENIUM_AVAILABLE = False

import requests
from bs4 import BeautifulSoup

# ========================= PAGE CONFIG & MODERN THEME =========================
st.set_page_config(page_title="Zap Scraper", layout="centered", page_icon="zap")

st.markdown("""
<style>
    .main {background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 0;}
    .block-container {background: rgba(255,255,255,0.95); border-radius: 20px; padding: 3rem; margin: 2rem auto; max-width: 1100px; box-shadow: 0 20px 40px rgba(0,0,0,0.1);}
    .big-title {
        font-size: 4.5rem !important; font-weight: 900; text-align: center;
        background: linear-gradient(90deg, #ff006e, #ffbe0b, #8338ec, #3a86ff);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin: 0 0 1rem 0; letter-spacing: -2px;
    }
    .subtitle {font-size: 1.4rem; text-align: center; color: #555; margin-bottom: 3rem;}
    .stButton>button {
        background: linear-gradient(90deg, #e63946, #ff6b6b) !important;
        color: white !important; height: 3.5rem; border-radius: 16px !important;
        font-size: 1.2rem !important; font-weight: 600; box-shadow: 0 8px 20px rgba(230,57,70,0.3);
        transition: all 0.3s !important;
    }
    .stButton>button:hover {transform: translateY(-4px); box-shadow: 0 12px 30px rgba(230,57,70,0.4) !important;}
    .metric-card {
        background: white; padding: 1.5rem; border-radius: 16px; text-align: center;
        box-shadow: 0 8px 25px rgba(0,0,0,0.08); border: 1px solid #f0f0f0;
    }
    .metric-value {font-size: 2.2rem; font-weight: 800; color: #e63946;}
    .metric-label {color: #666; font-size: 0.9rem; margin-top: 0.5rem;}
    hr {border: 0; height: 1px; background: linear-gradient(90deg, transparent, #ddd, transparent); margin: 3rem 0;}
</style>
""", unsafe_allow_html=True)

# ========================= HERO =========================
st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Lightning-fast email extractor • Blog & niche detection • Resume any time</div>', unsafe_allow_html=True)

# ========================= FILE UPLOAD =========================
uploaded = st.file_uploader("Drop your CSV or Excel file here", type=["csv","xlsx","xls"], label_visibility="collapsed")

if not uploaded:
    st.stop()

# Load & preview
if uploaded.name.endswith(".csv"):
    df = pd.read_csv(uploaded)
else:
    df = pd.read_excel(uploaded)

st.success(f"Loaded {len(df):,} rows • {len(df.columns)} columns")
st.dataframe(df.head(10), use_container_width=True)

url_col = st.selectbox("Select the column with URLs", options=df.columns, index=0)

# ========================= PROGRESS & SESSION =========================
session_id = st.session_state.get("sid", str(uuid.uuid4()))
st.session_state.sid = session_id
progress_file = f"zap_progress/zap_{session_id}_{uploaded.name[:50]}.csv"
os.makedirs("zap_progress", exist_ok=True)

progress = pd.DataFrame()
if os.path.exists(progress_file):
    try: progress = pd.read_csv(progress_file)
    except: pass

if progress.empty or len(progress) != len(df):
    progress = pd.DataFrame({
        "URL": df[url_col].astype(str).tolist(),
        "Emails": [json.dumps([]) for _ in range(len(df))],
        "Is_Blog": False,
        "Niche": "",
        "Status": "pending"
    })
    progress.to_csv(progress_file, index=False)

# ========================= METRICS =========================
col1, col2, col3, col4 = st.columns(4)
done = len(progress[progress["Status"] == "done"])
total = len(progress)
col1.markdown(f"<div class='metric-card'><div class='metric-value'>{done}/{total}</div><div class='metric-label'>Processed</div></div>", unsafe_allow_html=True)
col2.markdown(f"<div class='metric-card'><div class='metric-value'>{len(progress[progress['Emails'].apply(lambda x: json.loads(x)) != []])}</div><div class='metric-label'>Emails Found</div></div>", unsafe_allow_html=True)
col3.markdown(f"<div class='metric-card'><div class='metric-value'>{len(progress[progress['Is_Blog']])}</div><div class='metric-label'>Blogs Detected</div></div>", unsafe_allow_html=True)
col4.markdown(f"<div class='metric-card'><div class='metric-value'>{progress['Niche'].nunique()}</div><div class='metric-label'>Niches Found</div></div>", unsafe_allow_html=True)

st.markdown("---")

# ========================= SETTINGS =========================
s1, s2, s3 = st.columns(3)
threads = s1.slider("Thread power", 1, 10, 6, help="More = faster (but heavier)")
selenium = s2.checkbox("Enable Selenium fallback", help="For JavaScript-heavy sites")
skip_done = s3.checkbox("Skip already done", True)

# ========================= SCRAPING ENGINE (same as before, but cleaner) =========================
def scrape_url(url):
    try:
        r = requests.get(url, headers={"User-Agent": "ZapScraper/2.0"}, timeout=15)
        r.raise_for_status()
        html = r.text
    except:
        if not selenium or not SELENIUM_AVAILABLE: return {"emails": [], "blog": False, "niche": "Other", "status": "error"}
        try:
            opts = Options()
            for arg in ["--headless","--no-sandbox","--disable-dev-shm-usage"]:
                opts.add_argument(arg)
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(20)
            driver.get(url)
            time.sleep(2)
            html = driver.page_source
            driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    emails = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html + text)))

    blog = bool(soup.find("article") or "blog" in url.lower() or soup.find("meta", property="article:published_time"))
    niche = max(["Health","Finance","Tech","Ecommerce","Travel","Food","Education","Other"],
                key=lambda k: sum(w in text.lower() for w in {
                    "Health": ["health","doctor","diet"], "Finance": ["bank","money","stock"],
                    "Tech": ["tech","ai","software"], "Ecommerce": ["shop","buy","cart"],
                    "Travel": ["hotel","flight","trip"], "Food": ["recipe","chef","food"],
                    "Education": ["course","learn","school"]
                }.get(k, [])) or -1)

    return {"emails": emails, "blog": blog, "niche": niche, "status": "done"}

# ========================= START BUTTON =========================
if st.button("Start Scraping", type="primary", use_container_width=True):
    pending = [(i, row["URL"]) for i, row in progress.iterrows() if not (skip_done and row["Status"] == "done")]
    if not pending:
        st.balloons()
        st.success("Everything is already processed!")
    else:
        bar = st.progress(0)
        status = st.empty()
        with ThreadPoolExecutor(max_workers=threads) as exe:
            futures = {exe.submit(scrape_url, url): i for i, url in pending}
            for completed, future in enumerate(as_completed(futures), 1):
                i = futures[future]
                res = future.result()
                progress.at[i, "Emails"] = json.dumps(res["emails"])
                progress.at[i, "Is_Blog"] = res["blog"]
                progress.at[i, "Niche"] = res["niche"]
                progress.at[i, "Status"] = res["status"]
                progress.to_csv(progress_file, index=False)

                bar.progress(completed / len(pending))
                status.markdown(f"**{completed}/{len(pending)}** — Found **{len(res['emails'])}** emails on this page")

        st.balloons()
        st.success("Scraping complete!")

# ========================= RESULTS & DOWNLOAD =========================
st.markdown("---")
st.markdown("### Results")

result = df.copy()
result["Zap_Emails"] = progress["Emails"].apply(lambda x: "; ".join(json.loads(x)) if json.loads(x) else "")
result["Zap_Is_Blog"] = progress["Is_Blog"]
result["Zap_Niche"] = progress["Niche"]
result["Zap_Status"] = progress["Status"]

st.dataframe(result, use_container_width=True)

st.markdown("### Download")
csv = result.to_csv(index=False).encode()
st.download_button(
    "Download Zap_Results.csv",
    data=csv,
    file_name="Zap_Results.csv",
    mime="text/csv",
    use_container_width=True
)

st.markdown("<br><br><center>Made with ❤️ by creatorsera</center>", unsafe_allow_html=True)
