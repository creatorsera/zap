# Zap Scraper — upgraded Streamlit app
# Features added:
# - Resumable checkpoints per user/session
# - Faster scraping: requests + BeautifulSoup first, Selenium fallback
# - Blog detection + niche detection
# - Auto-save after each URL and on every change
# - Modern reddish UI and Zap branding
# - Download filename: Zap_Results.csv

import streamlit as st
import pandas as pd
import re
import time
import os
import uuid
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse

# Optional Selenium fallback
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

import requests
from bs4 import BeautifulSoup

# --------------------------- Configuration ---------------------------
MAX_THREADS = 6
REQUEST_TIMEOUT = 15
PROGRESS_DIR = "zap_progress"
os.makedirs(PROGRESS_DIR, exist_ok=True)

NICHE_KEYWORDS = {
    "Health": ["health","fitness","doctor","symptom","recipe","diet","workout","wellness","medical","clinic"],
    "Finance": ["finance","bank","loan","investment","trading","stock","crypto","insurance","money","tax"],
    "Travel": ["travel","hotel","flight","itinerary","destination","tour","booking","trip"],
    "Food": ["recipe","cooking","restaurant","dish","ingredients","cuisine","chef"],
    "Tech": ["tech","software","app","developer","coding","AI","machine learning","gadget","hardware"],
    "Ecommerce": ["shop","cart","buy","product","checkout","store","ecommerce","sale"],
    "Education": ["school","college","course","lesson","tutorial","study","learn"]
}

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# --------------------------- Helpers ---------------------------

def make_session_id():
    if "zap_session_id" not in st.session_state:
        st.session_state["zap_session_id"] = str(uuid.uuid4())
    return st.session_state["zap_session_id"]

def progress_filename(session_id, source_name):
    safe_name = re.sub(r"[^0-9a-zA-Z-_\.]+", "_", source_name)[:60]
    return os.path.join(PROGRESS_DIR, f"progress_{session_id}_{safe_name}.csv")

def load_progress(path):
    if path and os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_progress(df, path):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def extract_emails_from_text(text):
    return list(set(EMAIL_REGEX.findall(text or "")))

def is_likely_blog(soup, text, url):
    blog_signs = 0
    if any(p in url.lower() for p in ["/blog", "/post", "/article", "/tag", "/category", "/202", "/posts/"]):
        blog_signs += 1
    if soup.find_all("article"):
        blog_signs += 1
    if soup.find_all(attrs={"class": re.compile(r"post|article|entry|blog", re.I)}):
        blog_signs += 1
    if soup.find("meta", {"property": "article:published_time"}) or soup.find("time"):
        blog_signs += 1
    return blog_signs >= 2

def detect_niche(text):
    scores = {}
    lower = (text or "").lower()
    for niche, keywords in NICHE_KEYWORDS.items():
        cnt = sum(lower.count(k) for k in keywords)
        if cnt > 0:
            scores[niche] = cnt
    if not scores:
        return "Other"
    return max(scores.items(), key=lambda x: x[1])[0]

def fetch_with_requests(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ZapScraper/1.0)"}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text

def fetch_with_selenium(url):
    chrome_options = Options()
    chrome_options.headless = True
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.set_page_load_timeout(REQUEST_TIMEOUT)
        driver.get(url)
        time.sleep(1)
        return driver.page_source
    finally:
        driver.quit()

def scrape_single(url, use_selenium_fallback=True):
    result = {"url": url, "emails": [], "is_blog": False, "niche": "", "status": "error", "error": ""}
    try:
        html = None
        try:
            html = fetch_with_requests(url)
        except Exception as e:
            result["error"] = f"requests failed: {str(e)}"
            if not use_selenium_fallback or not SELENIUM_AVAILABLE:
                return result
            try:
                html = fetch_with_selenium(url)
            except Exception as e2:
                result["error"] = f"selenium failed: {str(e2)}"
                return result

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ")

        emails = extract_emails_from_text(html + " " + text)
        result["emails"] = emails
        result["is_blog"] = is_likely_blog(soup, text, url)
        result["niche"] = detect_niche(text)
        result["status"] = "done"

    except Exception as e:
        result["error"] = str(e)

    return result

# --------------------------- UI ---------------------------
st.set_page_config(page_title="Zap Scraper", layout="wide")

st.markdown("""
<style>
    .big-font {font-size:3.5rem !important; font-weight:800; text-align:center;
               background: linear-gradient(90deg,#e63946,#ff6b6b);
               -webkit-background-clip: text; -webkit-text-fill-color: transparent;}
    .block-container {padding: 2rem;}
    .stButton>button {background:#e63946 !important; color:white;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-font">Zap Scraper</h1>', unsafe_allow_html=True)

uploaded_file = st.file_uploader("Upload CSV/Excel with website URLs", type=["csv","xlsx","xls"])

if not uploaded_file:
    st.stop()

if uploaded_file.name.endswith(".csv"):
    input_df = pd.read_csv(uploaded_file)
else:
    input_df = pd.read_excel(uploaded_file)

url_column = "URL" if "URL" in input_df.columns else "url"
if url_column not in input_df.columns:
    st.error("Column 'URL' or 'url' not found")
    st.stop()

st.success(f"Loaded {len(input_df)} URLs")

session_id = make_session_id()
progress_path = progress_filename(session_id, uploaded_file.name)

progress_df = load_progress(progress_path)
if progress_df.empty:
    progress_df = pd.DataFrame({
        'URL': input_df[url_column].astype(str).tolist(),
        'emails': ["[]"] * len(input_df),
        'is_blog': [False] * len(input_df),
        'niche': [""] * len(input_df),
        'status': ["pending"] * len(input_df),
        'error': [""] * len(input_df),
        'last_updated': [""] * len(input_df)
    })
    save_progress(progress_df, progress_path)

c1, c2, c3, c4 = st.columns(4)
concurrency = c1.slider("Concurrency", 1, 12, 4)
skip_done = c2.checkbox("Skip done", True)
use_selenium = c3.checkbox("Selenium fallback", False)
auto_save_every = c4.number_input("Auto-save every", 1, 50, 1)

st.markdown("### Progress")
st.write(progress_df['status'].value_counts().to_dict())

if st.button("Start / Resume", type="primary"):
    urls_to_process = [(i, r['URL']) for i, r in progress_df.iterrows() if not (skip_done and r['status'] == 'done')]

    if not urls_to_process:
        st.success("All done!")
    else:
        ph = st.empty()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(scrape_single, url, use_selenium): idx for idx, url in urls_to_process}
            done = 0
            for future in as_completed(futures):
                idx = futures[future]
                res = future.result()
                progress_df.at[idx, 'emails'] = json.dumps(res["emails"])
                progress_df.at[idx, 'is_blog'] = res["is_blog"]
                progress_df.at[idx, 'niche'] = res["niche"]
                progress_df.at[idx, 'status'] = res["status"]
                progress_df.at[idx, 'error'] = res["error"]
                progress_df.at[idx, 'last_updated'] = datetime.utcnow().isoformat()
                done += 1
                if done % auto_save_every == 0:
                    save_progress(progress_df, progress_path)
                ph.markdown(f"Processed {done}/{len(urls_to_process)}")
        save_progress(progress_df, progress_path)
        st.success("Finished!")

# Download
output_df = progress_df.copy()
output_df['emails'] = output_df['emails'].apply(lambda x: ';'.join(json.loads(x)) if x != "[]" else '')
final_df = input_df.merge(output_df[['URL','emails','is_blog','niche','status']], on='URL', how='left')
final = final.rename(columns={'emails': 'Zap_emails', 'is_blog': 'Zap_is_blog', 'niche': 'Zap_niche', 'status': 'Zap_status'})

st.download_button("Download Zap_Results.csv", final.to_csv(index=False).encode(), "Zap_Results.csv", "text/csv")

st.dataframe(progress_df.head(20))
