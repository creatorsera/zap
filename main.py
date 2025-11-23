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

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", re.IGNORECASE)

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
        time.sleep(1)  # give a small time for JS to render
        return driver.page_source
    finally:
        driver.quit()


def scrape_single(url, use_selenium_fallback=True):
    result = {"url": url, "emails": [], "is_blog": False, "niche": "", "status": "error", "error": ""}
    try:
        html = None
        # try requests first
        try:
            html = fetch_with_requests(url)
            result["status"] = "done"
        except Exception as e:
            result["error"] = f"requests failed: {str(e)}"
            if not use_selenium_fallback or not SELENIUM_AVAILABLE:
                return result
            # Selenium fallback
            try:
                html = fetch_with_selenium(url)
                result["status"] = "done (selenium)"
            except Exception as e2:
                result["error"] = f"selenium failed: {str(e2)}"
                return result

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ")

        emails = extract_emails_from_text(html + " " + text)
        result["emails"] = emails

        result["is_blog"] = is_likely_blog(soup, text, url)
        result["niche"] = detect_niche(text)

    except Exception as e:
        result["error"] = str(e)

    return result


# --------------------------- UI ---------------------------

st.set_page_config(page_title="Zap Scraper", layout="wide")

st.markdown("""
<style>
    .big-font {
        font-size: 3.5rem !important;
        font-weight: 800;
        text-align: center;
        background: linear-gradient(90deg, #e63946, #f77f7f);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 2rem;
    }
    .block-container {padding-top: 2rem;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-font">Zap Scraper</h1>', unsafe_allow_html=True)

uploaded_file = st.file_uploader("Upload CSV/Excel with website URLs", type=["csv", "xlsx", "xls"])

if not uploaded_file:
    st.info("Upload a file containing URLs to begin")
    st.stop()

# Load input file
if uploaded_file.name.endswith(".csv"):
    input_df = pd.read_csv(uploaded_file)
else:
    input_df = pd.read_excel(uploaded_file)

# Find URL column
if "URL" in input_df.columns:
    url_column = "URL"
elif "url" in input_df.columns:
    url_column = "url"
else:
    st.error("No column named 'URL' or 'url' found")
    st.stop()

st.success(f"Loaded {len(input_df)} URLs from column `{url_column}`")

# Session handling
session_id = make_session_id()
source_name = uploaded_file.name
progress_path = progress_filename(session_id, source_name)

# Load or initialize progress
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

# Progress summary
st.markdown("### Progress Overview")
status_counts = progress_df['status'].value_counts()
st.write(status_counts.to_dict())

# Buttons
col1, col2 = st.columns(2)
if col2.button("Resume previous run"):
    st.success("Resuming from saved progress file.")
if col1.button("Start fresh (reset progress)"):
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
    st.rerun()

st.markdown("---")

# Collect URLs to process
urls_to_process = []
for idx, row in progress_df.iterrows():
    if skip_done and row['status'] == 'done':
        continue
    urls_to_process.append((idx, row['URL']))

if not urls_to_process:
    st.success("No URLs to process — all done or no pending URLs.")
else:
    placeholder = st.empty()
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
st.markdown("### Sample progress (first 10 rows")
st.dataframe(progress_df.head(10))
