import streamlit as st
import pandas as pd
import re
import os
import uuid
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium support
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except:
    SELENIUM_AVAILABLE = False

import requests
from bs4 import BeautifulSoup

# ======================== PAGE & STYLE ========================
st.set_page_config(page_title="Zap Scraper", layout="centered", page_icon="zap")

st.markdown("""
<style>
    .big-title {font-size:4.8rem !important; font-weight:900; text-align:center;
                background:linear-gradient(90deg,#e63946,#ff6b6b);
                -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:1rem;}
    .stButton>button {background:#e63946 !important; color:white; height:3.5rem; border-radius:16px; font-size:1.2rem;}
    .block-container {max-width:1100px; padding:2rem;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown("**Instant email extractor • Blog & niche detection Resume anytime**")

# ======================== UPLOAD ========================
uploaded = st.file_uploader("Upload CSV or Excel with URLs", type=["csv","xlsx","xls"])
if not uploaded:
    st.stop()

df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
st.success(f"Loaded {len(df)} URLs")
st.dataframe(df.head(10), use_container_width=True)

url_col = st.selectbox("Select the column that contains URLs", options=df.columns)

# ======================== PROGRESS SYSTEM ========================
if "sid" not in st.session_state:
    st.session_state.sid = str(uuid.uuid4())

os.makedirs("zap_progress", exist_ok=True)
progress_file = f"zap_progress/{st.session_state.sid}_{uploaded.name[:60]}.csv"

if os.path.exists(progress_file):
    progress = pd.read_csv(progress_file)
else:
    progress = pd.DataFrame({
        "URL": df[url_col].astype(str).tolist(),
        "Emails": [json.dumps([]) for _ in df.index],
        "Is_Blog": [False] * len(df),
        "Niche": [""] * len(df),
        "Status": ["pending"] * len(df)
    })
    progress.to_csv(progress_file, index=False)

# ======================== SETTINGS ========================
c1, c2, c3 = st.columns(3)
threads = c1.slider("Threads", 1, 10, 6)
use_selenium = c2.checkbox("Enable Selenium (for JavaScript-heavy sites)", False)
skip_done = c3.checkbox("Skip already processed", True)

# ======================== SCRAPER ========================
def scrape(url):
    html = None
    # Fast requests first
    try:
        r = requests.get(url, headers={"User-Agent": "ZapScraper/2025"}, timeout=12)
        r.raise_for_status()
        html = r.text
    except:
        pass

    # Selenium fallback
    if (html is None or use_selenium) and SELENIUM_AVAILABLE:
        try:
            opts = Options()
            opts.add_argument("--headless")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
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
    blog = bool(soup.find("article") or "blog" in url.lower())
    niche = "Other"
    if any(w in text.lower() for w in ["health","doctor","diet"]): niche = "Health"
    elif any(w in text.lower() for w in ["finance","bank","money"]): niche = "Finance"
    elif any(w in text.lower() for w in ["tech","software","ai"]): niche = "Tech"
    elif any(w in text.lower() for w in ["shop","buy","cart"]): niche = "Ecommerce"

    return {"emails": emails, "blog": blog, "niche": niche, "status": "done"}

# ======================== START ========================
if st.button("START SCRAPING", type="primary", use_container_width=True):
    to_do = [(i, r["URL"]) for i, r in progress.iterrows() if not (skip_done and r["Status"] == "done")]
    if not to_do:
        st.balloons()
        st.success("All URLs already processed!")
    else:
        bar = st.progress(0)
        info = st.empty()
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(scrape, url): i for i, url in to_do}
            for n, f in enumerate(as_completed(futures), 1):
                i = futures[f]
                res = f.result()
                progress.at[i, "Emails"] = json.dumps(res["emails"])
                progress.at[i, "Is_Blog"] = res["blog"]
                progress.at[i, "Niche"] = res["niche"]
                progress.at[i, "Status"] = res["status"]
                progress.to_csv(progress_file, index=False)
                bar.progress(n / len(to_do))
                info.markdown(f"**{n}/{len(to_do)}** — {len(res['emails'])} emails found")
        st.balloons()
        st.success("Scraping finished!")

# ======================== RESULTS ========================
final = df.copy()
final["Zap_Emails"] = ["; ".join(json.loads(e)) if json.loads(e) else "" for e in progress["Emails"]]
final["Zap_Is_Blog"] = progress["Is_Blog"]
final["Zap_Niche"] = progress["Niche"]
final["Zap_Status"] = progress["Status"]

st.dataframe(final, use_container_width=True)
st.download_button(
    "DOWNLOAD ZAP_RESULTS.CSV",
    final.to_csv(index=False).encode(),
    "Zap_Results.csv",
    use_container_width=True
)
