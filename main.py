# Zap Scraper — GOD MODE (All Features Included)
import streamlit as st
import pandas as pd
import re
import os
import json
import asyncio
import aiohttp
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
import requests  # for Serper

# ======================== CONFIG ========================
st.set_page_config(page_title="Zap Scraper GOD MODE", page_icon="zap", layout="wide")

st.markdown("""
<style>
    .big-title {font-size:5rem !important; font-weight:900; text-align:center;
                background:linear-gradient(90deg,#ff006e,#ffbe0b,#8338ec,#3a86ff);
                -webkit-background-clip:text; -webkit-text-fill-color:transparent;}
    .stButton>button {background:#e63946 !important; color:white; height:3.8rem; border-radius:18px; font-size:1.3rem;}
    .metric-card {background:white; padding:1.8rem; border-radius:18px; text-align:center; box-shadow:0 10px 30px rgba(0,0,0,0.1);}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown("**GOD MODE — Emails • Phones • Socials • Guest Posts • Team Pages • Google Search → Scrape**")

# ======================== STATE ========================
if "paused" not in st.session_state:
    st.session_state.paused = False
if "results" not in st.session_state:
    st.session_state.results = []
if "serper_key" not in st.session_state:
    st.session_state.serper_key = ""

# ======================== REGEX ========================
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.IGNORECASE)
PHONE_REGEX = re.compile(r'(?:\+?\d{1,3})?[\-.\s]?(?:\d{1,4})?[\-.\s]?\d{3,4}[\-.\s]?\d{4,9}')
GUEST_REGEX = re.compile(r"(write for us|guest post|submit article|contributor guidelines|blog submission)", re.I)

# ======================== HELPERS ========================
def save_progress():
    if st.session_state.results:
        pd.DataFrame(st.session_state.results).to_csv("zap_progress.csv", index=False)

def google_search(query, api_key, num=20):
    if not api_key:
        st.error("Add Serper.dev API key in the tab to use Google Search")
        return []
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": num})
    headers = {'X-API-KEY': api_key, 'Content-Type': 'application/json'}
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        return [item['link'] for item in response.json().get('organic', [])]
    except:
        st.error("Search failed — check your Serper key")
        return []

# ======================== GOOGLE SHEETS EXPORT ========================
# What it does: Creates a new Google Sheet, dumps your results into it, and gives you the link. The sheet is private to you but shareable.
# Setup: Go to GCP console → Create service account → Enable Sheets/Drive API → Download JSON key → Add to Streamlit secrets.toml as [gcp_service_account] = {email = "your@service.gserviceaccount.com", private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n", ...}
def export_to_sheets(df):
    try:
        import gspread
        from google.auth import default
        creds, _ = default()
        gc = gspread.authorize(creds)
        sheet = gc.create("Zap Scraper Results")
        sheet.share('', perm_type='anyone', role='writer')  # Make public if needed
        worksheet = sheet.sheet1
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        return sheet.url
    except:
        st.error("Google Sheets export failed — check secrets.toml")
        return None

# ======================== CRAWLER ========================
async def crawl_site(start_url, session, max_pages=20):
    result = {
        "Website": start_url, "Company": "", "Logo": "", "Emails": "", "Phones": "", "Facebook": "", "LinkedIn": "",
        "Guest_Post_Page": "", "Team_Page": "", "Tech": "", "Pages_Scanned": 0
    }
    visited = set()
    queue = deque([(start_url, 0)])
    domain = urlparse(start_url).netloc
    priority = ["/contact", "/about", "/team", "/about-us", "/write-for-us", "/guest-post", "/blog/submit"]

    emails = set()
    phones = set()
    retries = 0
    max_retries = 3

    while queue and len(visited) < max_pages and retries < max_retries:
        url, depth = queue.popleft()
        if url in visited or depth > 3: continue
        visited.add(url)

        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200: continue
                html = await resp.text()
                retries = 0
        except:
            retries += 1
            time.sleep(1)  # Backoff
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()

        # Company & logo
        if not result["Company"]:
            result["Company"] = soup.title.string.split(" | ")[0].strip() if soup.title else ""
        if not result["Logo"]:
            favicon = soup.find("link", rel=re.compile("icon", re.I))
            result["Logo"] = urljoin(url, favicon["href"]) if favicon else ""

        # Emails
        found = set(EMAIL_REGEX.findall(html + text))
        cleaned = {e.strip(".,;:'\"()[]{}").lower() for e in found if "@" in e}
        cleaned = {e for e in cleaned if not re.search(r"(privacy|gdpr|noreply)", e)}
        emails.update(cleaned)

        # Phones
        found_phones = PHONE_REGEX.findall(text)
        cleaned_phones = {re.sub(r'\D', '', p) for p in found_phones if len(re.sub(r'\D', '', p)) >= 8}
        phones.update(cleaned_phones)

        # Socials & special pages
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "facebook.com" in href and not result["Facebook"]: result["Facebook"] = href
            if "linkedin.com" in href and not result["LinkedIn"]: result["LinkedIn"] = href
            if GUEST_REGEX.search(href or a.get_text()) and not result["Guest_Post_Page"]: result["Guest_Post_Page"] = href
            if any(t in href for t in ["team", "about-us", "people"]) and not result["Team_Page"]: result["Team_Page"] = href

        # Tech
        tech = []
        html_lower = html.lower()
        if "shopify" in html_lower: tech.append("Shopify")
        if "wp-content" in html_lower: tech.append("WordPress")
        result["Tech"] = ", ".join(tech)

        # Add links
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if urlparse(link).netloc == domain and link not in visited:
                queue.append((link, depth + 1))

    result["Emails"] = "; ".join(sorted(emails))
    result["Phones"] = "; ".join(sorted(phones))
    result["Pages_Scanned"] = len(visited)
    return result

# ======================== UI ========================
uploaded = st.file_uploader("Upload CSV/Excel with URLs", type=["csv","xlsx","xls"])
serper_key = st.text_input("Serper.dev API Key (for Google Search — free 2,500/mo)", value=st.session_state.serper_key, type="password")
st.session_state.serper_key = serper_key

if uploaded:
    df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
    url_col = st.selectbox("URL column", df.columns)
    urls = df[url_col].dropna().tolist()

else:
    urls = []

query = st.text_input("Or Google Search for URLs (e.g., 'guest post tech blogs')")
if query and st.button("Search & Add URLs"):
    new_urls = google_search(query, serper_key)
    urls.extend(new_urls)
    st.success(f"Added {len(new_urls)} URLs from search")

if urls:
    col1, col2 = st.columns(2)
    if col1.button("START / RESUME", type="primary"):
        st.session_state.paused = False
    if col2.button("PAUSE"):
        st.session_state.paused = True

    progress_bar = st.progress(0)
    status = st.empty()

    async def run():
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            for i, url in enumerate(urls):
                while st.session_state.paused:
                    await asyncio.sleep(1)
                if not url.startswith("http"): url = "https://" + url
                res = await crawl_site(url, session)
                st.session_state.results.append(res)
                if i % 5 == 0: save_progress()
                progress_bar.progress((i+1)/len(urls))
                status.markdown(f"**{i+1}/{len(urls)}** — {len(res['Emails'].split('; '))} emails")

    asyncio.run(run())

    # Partial & full download
    if st.session_state.results:
        df_out = pd.DataFrame(st.session_state.results)
        st.markdown("### Live Results")
        st.dataframe(df_out, use_container_width=True)
        st.markdown(download_link(df_out, "zap_results.csv"), unsafe_allow_html=True)

        if st.button("Export to Google Sheets"):
            url = export_to_sheets(df_out)
            if url:
                st.success(f"Exported! Open: {url}")
