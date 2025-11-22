# Zap Scraper — FINAL WORKING VERSION (Nov 2025)
import streamlit as st
import pandas as pd
import re
import os
import uuid
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium (will work after packages.txt fix)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_OK = True
except:
    SELENIUM_OK = False

import requests
from bs4 import BeautifulSoup

# ========================= CONFIG & THEME =========================
st.set_page_config(page_title="Zap Scraper", layout="centered", page_icon="zap")

st.markdown("""
<style>
    .big-title {font-size:4.5rem !important; font-weight:900; text-align:center;
                background:linear-gradient(90deg,#ff006e,#ffbe0b,#8338ec,#3a86ff);
                -webkit-background-clip:text; -webkit-text-fill-color:transparent;}
    .stButton>button {background:#e63946 !important; color:white; height:3.5rem; border-radius:16px;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown("### Fast Email + Blog + Niche Extractor • Resume Anytime

# ========================= FILE UPLOAD =========================
uploaded = st.file_uploader("Upload CSV/Excel with URLs", type=["csv","xlsx","xls"])
if not uploaded:
    st.stop()

df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
st.success(f"Loaded {len(df)} rows")
st.dataframe(df.head(10), use_container_width=True)

url_col = st.selectbox("Select column containing URLs", options=df.columns)

# ========================= PROGRESS =========================
if "sid" not in st.session_state:
    st.session_state.sid = str(uuid.uuid4())
os.makedirs("zap_progress", exist_ok=True)
progress_file = f"zap_progress/progress_{st.session_state.sid}_{uploaded.name[:50]}.csv"

if os.path.exists(progress_file):
    progress = pd.read_csv(progress_file)
else:
    progress = pd.DataFrame({
        "URL": df[url_col].astype(str).tolist(),
        "Emails": [json.dumps([]) for _ in range(len(df))],
        "Is_Blog": [False] * len(df),
        "Niche": [""] * len(df),
        "Status": ["pending"] * len(df)
    })
    progress.to_csv(progress_file, index=False)

# ========================= SETTINGS =========================
c1, c2, c3 = st.columns(3)
threads = c1.slider("Threads", 1, 10, 6)
use_selenium = c2.checkbox("Enable Selenium (for JavaScript sites)", False)
skip_done = c3.checkbox("Skip already done", True)

# ========================= SCRAPER =========================
def scrape_one(url):
    # Fast requests first
    try:
        r = requests.get(url, headers={"User-Agent": "ZapScraper/2025"}, timeout=12)
        r.raise_for_status()
        html = r.text
    except:
        html = None

    # Selenium fallback
    if (html is None or use_selenium) and SELENIUM_OK:
        try:
            opts = Options()
            for arg in ["--headless", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"]:
                opts.add_argument(arg)
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(20)
            driver.get(url)
            html = driver.page_source
            driver.quit()
        except:
            pass

    if not html:
        return {"emails": [], "blog": False, "niche": "Other", "status": "error"}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()

    emails = list(set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html + text)))
    blog = bool(soup.find("article") or "blog" in url.lower() or soup.find("meta", property="article:published_time"))
    niche_keywords = {"Health":["health","doctor"],"Finance":["bank","money"],"Tech":["tech","ai","software"],"Ecommerce":["shop","buy"],"Travel":["hotel","flight"],"Food":["recipe","food"],"Education":["course","learn"]}
    niche = max(niche_keywords, key=lambda k: sum(w in text.lower() for w in niche_keywords[k]), default="Other")

    return {"emails": emails, "blog": blog, "niche": niche, "status": "done"}

# ========================= START BUTTON =========================
if st.button("START SCRAPING", type="primary", use_container_width=True):
    to_do = [(i, r["URL"]) for i, r in progress.iterrows() if not (skip_done and r["Status"] == "done")]
    if not to_do:
        st.success("All done!")
        st.balloons()
    else:
        bar = st.progress(0)
        stat = st.empty()
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(scrape_one, url): i for i, url in to_do}
            for n, f in enumerate(as_completed(futures), 1):
                i = futures[f]
                res = f.result()
                progress.at[i, "Emails"] = json.dumps(res["emails"])
                progress.at[i, "Is_Blog"] = res["blog"]
                progress.at[i, "Niche"] = res["niche"]
                progress.at[i, "Status"] = res["status"]
                progress.to_csv(progress_file, index=False)
                bar.progress(n / len(to_do))
                stat.write(f"**{n}/{len(to_do)}** → {len(res['emails'])} emails")
        st.balloons()
        st.success("Finished!")

# ========================= RESULTS =========================
result = df.copy()
result["Zap_Emails"] = ["; ".join(json.loads(e)) if json.loads(e) else "" for e in progress["Emails"]]
result["Zap_Is_Blog"] = progress["Is_Blog"]
result["Zap_Niche"] = progress["Niche"]
result["Zap_Status"] = progress["Status"]

st.dataframe(result, use_container_width=True)
st.download_button("Download Zap_Results.csv", result.to_csv(index=False).encode(), "Zap_Results.csv", use_container_width=True)
