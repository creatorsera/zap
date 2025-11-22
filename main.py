# Zap Scraper — FINAL PREMIUM EDITION (Working 100%)
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

# ========================= PAGE & THEME =========================
st.set_page_config(page_title="Zap Scraper", layout="centered", page_icon="zap")

st.markdown("""
<style>
    .main {background {background: linear-gradient(135deg, #1e3c72, #2a5298); min-height: 100vh;}
    .block-container {background: rgba(255,255,255,0.97); border-radius: 24px; padding: 3rem; margin: 2rem auto; max-width: 1100px; box-shadow: 0 25px 50px rgba(0,0,0,0.15);}
    .big-title {font-size: 4.8rem !important; font-weight: 900; text-align: center;
                background: linear-gradient(90deg, #ff006e, #ffbe0b, #8338ec, #3a86ff);
                -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0 0 1rem;}
    .subtitle {font-size: 1.5rem; text-align: center; color: #444; margin-bottom: 3rem;}
    .stButton>button {background: linear-gradient(90deg, #e63946, #f77f7f) !important; color: white !important;
                      height: 3.8rem; border-radius: 18px !important; font-size: 1.3rem !important; font-weight: 700;
                      box-shadow: 0 10px 25px rgba(230,57,70,0.4); transition: all 0.3s;}
    .stButton>button:hover {transform: translateY(-5px); box-shadow: 0 15px 35px rgba(230,57,70,0.5) !important;}
    .metric-card {background: white; padding: 1.8rem; border-radius: 18px; text-align: center;
                  box-shadow: 0 10px 30px rgba(0,0,0,0.1); border: 1px solid #eee;}
    .metric-value {font-size: 2.5rem; font-weight: 900; color: #e63946;}
    .metric-label {color: #777; font-size: 1rem; margin-top: 0.5rem;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Instant email extraction • Blog & niche detection • Resume anytime</div>', unsafe_allow_html=True)

# ========================= UPLOAD =========================
uploaded = st.file_uploader("Drop your CSV/Excel file here", type=["csv","xlsx","xls"], label_visibility="collapsed")
if not uploaded:
    st.stop()

# Load data
df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
st.success(f"Loaded {len(df):,} rows • {len(df.columns)} columns")
st.dataframe(df.head(10), use_container_width=True)

url_col = st.selectbox("Select URL column", options=df.columns.tolist())

# ========================= PROGRESS SYSTEM =========================
if "sid" not in st.session_state:
    st.session_state.sid = str(uuid.uuid4())
os.makedirs("zap_progress", exist_ok=True)
progress_file = f"zap_progress/progress_{st.session_state.sid}_{uploaded.name[:50]}.csv"

progress = pd.DataFrame()
if os.path.exists(progress_file):
    try:
        progress = pd.read_csv(progress_file)
    except:
        pass

if progress.empty or len(progress) != len(df):
    progress = pd.DataFrame({
        "URL": df[url_col].astype(str).tolist(),
        "Emails": [json.dumps([]) for _ in range(len(df))],
        "Is_Blog": [False] * len(df),
        "Niche": [""] * len(df),
        "Status": ["pending"] * len(df)
    })
    progress.to_csv(progress_file, index=False)

# ========================= METRICS =========================
c1, c2, c3, c4 = st.columns(4)
done = (progress["Status"] == "done").sum()
emails_found = sum(1 for x in progress["Emails"] if json.loads(x))
blogs = progress["Is_Blog"].sum()
c1.markdown(f"<div class='metric-card'><div class='metric-value'>{done}/{len(df)}</div><div class='metric-label'>Processed</div></div>", unsafe_allow_html=True)
c2.markdown(f"<div class='metric-card'><div class='metric-value'>{emails_found}</div><div class='metric-label'>Emails Found</div></div>", unsafe_allow_html=True)
c3.markdown(f"<div class='metric-card'><div class='metric-value'>{blogs}</div><div class='metric-label'>Blogs</div></div>", unsafe_allow_html=True)
c4.markdown(f"<div class='metric-card'><div class='metric-value'>{progress['Niche'].nunique()}</div><div class='metric-label'>Niches</div></div>", unsafe_allow_html=True)

st.markdown("---")

# ========================= SETTINGS =========================
s1, s2, s3 = st.columns(3)
threads = s1.slider("Threads (speed)", 1, 10, 6)
use_selenium = s2.checkbox("Selenium fallback (for JS sites)", False)
skip_done = s3.checkbox("Skip completed", True)

# ========================= SCRAPING FUNCTION (NOW FIXED!) =========================
def scrape_url(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; ZapScraper/3.0)"}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        if not use_selenium or not SELENIUM_AVAILABLE:
            return {"emails": [], "blog": False, "niche": "Other", "status": "error"}
        try:
            opts = Options()
            for a in ["--headless","--no-sandbox","--disable-dev-shm-usage","--disable-gpu"]:
                opts.add_argument(a)
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(20)
            driver.get(url)
            time.sleep(2)
            html = driver.page_source
            driver.quit()
        except:
            return {"emails": [], "blog": False, "niche": "Other", "status": "error"}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    emails = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html + " " + text)))

    blog = bool(soup.find("article") or "blog" in url.lower() or soup.find("meta", property="article:published_time"))
    niche_keywords = {
        "Health": ["health","doctor","diet","wellness"],
        "Finance": ["finance","bank","investment","money"],
        "Tech": ["tech","ai","software","app"],
        "Ecommerce": ["shop","buy","cart","store"],
        "Travel": ["hotel","flight","trip"],
        "Food": ["food","recipe","restaurant"],
        "Education": ["learn","course","school"]
    }
    niche = max(niche_keywords, key=lambda k: sum(w in text.lower() for w in niche_keywords[k]), default="Other")

    return {"emails": emails, "blog": blog, "niche": niche, "status": "done"}

# ========================= START BUTTON =========================
if st.button("START SCRAPING", type="primary", use_container_width=True):
    pending = [(i, row["URL"]) for i, row in progress.iterrows() if not (skip_done and row["Status"] == "done")]
    if not pending:
        st.balloons()
        st.success("All URLs already processed!")
    else:
        bar = st.progress(0)
        status = st.empty()
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(scrape_url, url): idx for idx, url in pending}
            for count, future in enumerate(as_completed(futures), 1):
                idx = futures[future]
                result = future.result()
                progress.at[idx, "Emails"] = json.dumps(result["emails"])
                progress.at[idx, "Is_Blog"] = result["blog"]
                progress.at[idx, "Niche"] = result["niche"]
                progress.at[idx, "Status"] = result["status"]
                progress.to_csv(progress_file, index=False)
                bar.progress(count / len(pending))
                status.markdown(f"**{count}/{len(pending)}** — Found **{len(result['emails'])}** emails")
        st.balloons()
        st.success("Scraping completed — you're a legend!")

# ========================= RESULTS =========================
st.markdown("---")
st.subheader("Results")

final = df.copy()
final["Zap_Emails"] = [("; ".join(json.loads(e)) if json.loads(e) else "") for e in progress["Emails"]]
final["Zap_Is_Blog"] = progress["Is_Blog"]
final["Zap_Niche"] = progress["Niche"]
final["Zap_Status"] = progress["Status"]

st.dataframe(final, use_container_width=True)

st.download_button(
    "DOWNLOAD ZAP_RESULTS.CSV",
    data=final.to_csv(index=False).encode(),
    file_name="Zap_Results.csv",
    mime="text/csv",
    use_container_width=True
)

st.markdown("<center>Made with love by creatorsera</center>", unsafe_allow_html=True)
