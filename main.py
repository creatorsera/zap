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

# Developer-provided uploaded file path (local). The deployment environment can transform this path to a URL if needed.
DEFAULT_UPLOADED_PATH = "/mnt/data/sdcjkdncksdncwnt245090fkerfjfacebookkrenfjkrenemailfere-ejewjroejrowe-main (1).zip"

# Niche keywords (extendable)
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
    # Each user session gets a stable id stored in session_state
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
    # atomic save
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def extract_emails_from_text(text):
    return list(set(EMAIL_REGEX.findall(text or "")))


def is_likely_blog(soup, text, url):
    # Heuristics for blog detection
    lower_text = (text or "").lower()
    html = str(soup)

    blog_signs = 0
    # url patterns
    if any(p in url.lower() for p in ["/blog", "/post", "/article", "/tag", "/category", "/202", "/posts/"]):
        blog_signs += 1
    # HTML tags
    if soup.find_all("article"):
        blog_signs += 1
    if soup.find_all(attrs={"class": re.compile(r"post|article|entry|blog", re.I)}):
        blog_signs += 1
    # metadata
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
    # return top niche
    return max(scores.items(), key=lambda x: x[1])[0]


def fetch_with_requests(url):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ZapScraper/1.0)"}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def fetch_with_selenium(url):
    # Minimal headless Chrome with images/css disabled for speed
    chrome_options = Options()
    chrome_options.headless = True
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--window-size=1200,800")

    # try to create driver (note: on some deployments this will fail if chromedriver not installed)
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.set_page_load_timeout(REQUEST_TIMEOUT)
        driver.get(url)
        time.sleep(1)  # give a small time for JS to render
        return driver.page_source
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def scrape_single(url, use_selenium_fallback=True):
    result = {
        "url": url,
        "emails": [],
        "is_blog": False,
        "niche": "",
        "status": "error",
        "error": ""
    }
    try:
        html = None
        # 1. Try fast requests first
        try:
            html = fetch_with_requests(url)
            result["status"] = "done"
        except Exception as e:
            result["error"] = f"requests failed: {e}"
            if not use_selenium_fallback or not SELENIUM_AVAILABLE:
                return result

            # 2. Selenium fallback
            try:
                html = fetch_with_selenium(url)
                result["status"] = "done (selenium)"
            except Exception as e2:
                result["error"] = f"selenium failed: {e2}"
                return result

        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ")

        # Extract emails from raw HTML + visible text
        emails = extract_emails_from_text(html + " " + text)
        result["emails"] = emails

        # Detect if it's a blog and which niche
        result["is_blog"] = is_likely_blog(soup, text, url)
        result["niche"] = detect_niche(text)
        result["status"] = "done"

    except Exception as unexpected:
        result["error"] = str(unexpected)

    return result
    
" + text)
        result["emails"] = emails
        result["is_blog"] = is_likely_blog(soup, text, url)
        result["niche"] = detect_niche(text)
        result["status"] = "done"
        return result
    except Exception as e:
        result["error"] = str(e)
        return result


# --------------------------- Streamlit UI ---------------------------

st.set_page_config(page_title="Zap — Website Scraper", layout="wide")

# Simple red-ish theme via CSS
st.markdown("""
<style>
:root{--zap-red:#b71c1c;--zap-dark:#7f0000;--card:#fff5f5}
body{background:linear-gradient(180deg, #fff, #fff5f5)}
.stApp {font-family: Inter, system-ui, sans-serif}
.header{display:flex;align-items:center;gap:16px}
.logo{width:56px;height:56px;border-radius:12px;background:var(--zap-red);display:flex;align-items:center;justify-content:center;color:white;font-weight:700;font-size:22px}
.title{font-size:22px;font-weight:700}
.card{background:var(--card);padding:14px;border-radius:12px;box-shadow:0 6px 20px rgba(0,0,0,0.06)}
.small{font-size:13px;color:#333}
</style>
""", unsafe_allow_html=True)

col1, col2 = st.columns([3,1])
with col1:
    st.markdown('<div class="header"> <div class="logo">Z</div> <div><div class="title">Zap — Website Scraper</div><div class="small">Fast, resumable scraping with blog & niche detection</div></div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f"**Session:** `{make_session_id()[:8]}`")

st.markdown("
")

with st.expander("Upload input CSV or use sample file", expanded=True):
    uploaded = st.file_uploader("Upload a CSV with a column of URLs", type=["csv"], key="file_uploader")
    st.write("Or use the provided sample upload path (useful for local testing):")
    st.code(DEFAULT_UPLOADED_PATH, language="bash")

    st.markdown("---")
    st.info("CSV must have a column containing URLs (we'll ask you to choose which column).")

# Load initial dataframe
input_df = None
source_name = "user_upload"
if uploaded is not None:
    try:
        input_df = pd.read_csv(uploaded)
        source_name = getattr(uploaded, 'name', 'user_upload')
    except Exception as e:
        st.error(f"Failed to read uploaded CSV: {e}")

# allow the user to load from default local path for convenience (developer file)
use_default = st.checkbox("Use the example uploaded file path (local dev path)", value=False)
if use_default and os.path.exists(DEFAULT_UPLOADED_PATH):
    # try to find a CSV inside the ZIP or a default CSV; here we just demonstrate using the path as a source name
    st.success("Default uploaded file path will be used as source name (you still must upload a CSV in the UI when deploying remotely).")
    source_name = os.path.basename(DEFAULT_UPLOADED_PATH)

if input_df is None:
    st.warning("Please upload a CSV to proceed.")
    st.stop()

# choose URL column
cols = list(input_df.columns)
url_column = st.selectbox("Which column contains the website URLs?", cols)

# session progress file
session_id = make_session_id()
progress_path = progress_filename(session_id, source_name)

# load or initialize progress
progress_df = load_progress(progress_path)
if progress_df.empty:
    # initialize
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

st.markdown("---")

cols = st.columns([3,1,1,1])
with cols[0]:
    concurrency = st.slider("Concurrency (threads)", 1, min(12, MAX_THREADS), value=4)
with cols[1]:
    skip_done = st.checkbox("Skip already done URLs", value=True)
with cols[2]:
    use_selenium = st.checkbox("Allow Selenium fallback (slower)", value=False)
with cols[3]:
    auto_save_every = st.number_input("Auto-save every X URLs", min_value=1, max_value=50, value=1)

st.markdown("---")

# Show a summary table (small)
st.markdown("### Progress Overview")
counts = progress_df['status'].value_counts().to_dict()
st.write(counts)

# Buttons: Resume / Start new
start_col, resume_col = st.columns(2)
if resume_col.button("Resume previous run"):
    st.success("Resuming from saved progress file.")
if start_col.button("Start fresh (reset progress)"):
    if os.path.exists(progress_path):
        os.remove(progress_path)
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
    st.experimental_rerun()

st.markdown("---")

# Core scraping loop using ThreadPoolExecutor
urls_to_process = []
for idx, row in progress_df.iterrows():
    if skip_done and row['status'] == 'done':
        continue
    urls_to_process.append((idx, row['URL']))

if not urls_to_process:
    st.info("No URLs to process — all done or no pending URLs.")
else:
    placeholder = st.empty()

    results = []
    total = len(urls_to_process)
    st.write(f"Processing {total} URLs with {concurrency} threads. Selenium allowed: {use_selenium}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(scrape_single, url, use_selenium): (idx, url) for idx, url in urls_to_process}
        completed = 0
        for fut in as_completed(futures):
            idx, url = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"url": url, "emails": [], "is_blog": False, "niche": "", "status": "error", "error": str(e)}

            # write back to progress_df
            try:
                emails_json = json.dumps(res.get('emails', []))
                progress_df.at[idx, 'emails'] = emails_json
                progress_df.at[idx, 'is_blog'] = res.get('is_blog', False)
                progress_df.at[idx, 'niche'] = res.get('niche', '')
                progress_df.at[idx, 'status'] = res.get('status', 'error')
                progress_df.at[idx, 'error'] = res.get('error', '')
                progress_df.at[idx, 'last_updated'] = datetime.utcnow().isoformat()
            except Exception as e:
                st.error(f"Failed to write progress for {url}: {e}")

            completed += 1
            # auto-save periodically
            if completed % auto_save_every == 0 or completed == total:
                save_progress(progress_df, progress_path)

            # update UI
            placeholder.markdown(f"Processed **{completed}/{total}** — last: {url}")

    # final save
    save_progress(progress_df, progress_path)

    st.success("Scraping run completed or paused. You can download results below.")

# Prepare download
# Expand emails JSON and merge with original input
output_df = progress_df.copy()
# turn emails JSON into semicolon-separated string
output_df['emails'] = output_df['emails'].apply(lambda x: ';'.join(json.loads(x)) if x and x.startswith('[') else (x if x else ''))

# Merge with original input to preserve other columns
final_df = input_df.copy()
final_df = final_df.reset_index(drop=True)
# ensure same length
if len(final_df) == len(output_df):
    final_df['Zap_emails'] = output_df['emails']
    final_df['Zap_is_blog'] = output_df['is_blog']
    final_df['Zap_niche'] = output_df['niche']
    final_df['Zap_status'] = output_df['status']
else:
    # fallback
    temp = output_df[['URL','emails','is_blog','niche','status']]
    final_df = final_df.merge(temp, left_on=url_column, right_on='URL', how='left')

csv_bytes = final_df.to_csv(index=False).encode('utf-8')

st.download_button("Download Zap_Results.csv", data=csv_bytes, file_name="Zap_Results.csv", mime='text/csv')

st.markdown("---")

st.markdown("### Quick tips")
st.markdown("- Use a modest concurrency on cheap VMs (2-6).")
st.markdown("- If Selenium is not available on your host, keep fallback disabled.")
st.markdown("- Progress files are stored in `zap_progress/` and are named per session + source to avoid collision.")

# show snippet of progress
st.markdown("### Sample progress (first 10 rows)")
st.dataframe(progress_df.head(10))

