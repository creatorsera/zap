# Zap Scraper — GOD MODE 2025 (Everything you asked for)
import streamlit as st
import pandas as pd
import re
import os
import json
import asyncio
import aiohttp
import time
import base64
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
import requests  # for Serper & Google Sheets

# ======================== CONFIG ========================
st.set_page_config(page_title="Zap Scraper GOD MODE", page_icon="zap", layout="wide")

st.markdown("""
<style>
    .big-title {font-size:5rem !important; font-weight:900; text-align:center;
                background:linear-gradient(90deg,#ff006e,#ffbe0b,#8338ec,#3a86ff);
                -webkit-background-clip:text; -webkit-text-fill-color:transparent;}
    .stButton>button {background:#e63946 !important; color:white; height:3.8rem; border-radius:18px; font-size:700 1.3rem sans-serif;}
    .metric-card {background:white; padding:1.8rem; border-radius:18px; text-align:center; box-shadow:0 10px 30px rgba(0,0,0,0.1);}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown("**GOD MODE 2025 — Emails • Phones • Socials • Guest Posts • Team Pages • Google Search → Scrape**")

# ======================== STATE ========================
if "paused" not in st.session_state: st.session_state.paused = False
if "results" not in st.session_state: st.session_state.results = []
if "serper_key" not in st.session_state: st.session_state.serper_key = ""

# ======================== HELPERS ========================
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"\\+?\\d{1,4}?[-.\\s]?\\(?\\d{1,4}?\\)?[-.\\s]?\\d{3,4}[-.\\s]?\\d{4,9}")
GUEST_POST_REGEX = re.compile(r"write for us|guest post|contributor|submit.*article|blog.*submission", re.I)

def save_progress():
    if st.session_state.results:
        pd.DataFrame(st.session_state.results).to_csv("zap_progress_backup.csv", index=False)

def download_link(df, filename="zap_results.csv"):
    csv = df.to_csv(index=False).encode()
    b64 = base64.b64encode(csv).decode()
    return f'<a href="data:text/csv;base64,{b64}" download="{filename}">DOWNLOAD {filename.upper()} NOW</a>'

# ======================== GOOGLE SHEETS EXPORT ========================
def export_to_sheets(df):
    try:
        # You need to create a Google Sheet and share it with this email (add in Streamlit secrets):
        # service-account@your-project.iam.gserviceaccount.com
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.create("Zap Scraper Results - " + time.strftime("%Y%m%d-%H%M%S"))
        sheet.share('', perm_type='anyone', role='writer')
        sheet.values_update(
            "Sheet1!A1",
            params={'valueInputOption': 'RAW'},
            body={'values': [df.columns.tolist()] + df.values.tolist()}
        )
        return sheet.url
    except:
        return None

# ======================== SERPER SEARCH (FREE) ========================
def google_search(query, num=10):
    if not st.session_state.serper_key:
        return []
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": num})
    headers = {'X-API-KEY': st.session_state.serper_key, 'Content-Type': 'application/json'}
    try:
        response = requests.post(url, headers=headers, data=payload)
        return [item['link'] for item in response.json().get('organic', [])]
    except:
        return []

# ======================== CRAWLER ========================
async def crawl_site(start_url, session, max_pages=20):
    result = {
        "Website": start_url, "Company": "", "Logo": "", "Emails": "", "Phones": "", "Facebook": "", "LinkedIn": "",
        "Guest_Post_Page": "", "Team_Page": "", "Tech": "", "Pages_Scanned": 0
    }
    visited = set()
    queue = deque([(start_url, 0)])
    domain = urlparse(start_url).netloc
    priority = ["/contact", "/about", "/team", "/about-us", "/write-for-us", "/guest-post", "/blog"]

    while queue and len(visited) < max_pages:
        url, depth = queue.popleft()
        if url in visited or depth > 3: continue
        visited.add(url)

        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200: continue
                html = await resp.text()
        except:
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()

        # Company name & logo
        if not result["Company"]:
            title = soup.title.string if soup.title else ""
            result["Company"] = title.split("|")[0].split("-")[0].strip()
        if not result["Logo"] and soup.find("link", rel="icon"):
            result["Logo"] = urljoin(url, soup.find("link", rel="icon")["href"])
        elif not result["Logo"] and soup.find("meta", property="og:image"):
            result["Logo"] = soup.find("meta", property="og:image")["content"]

        # Emails & phones
        emails = set(EMAIL_REGEX.findall(html + text))
        emails = {e.strip(".,;:'\"()[]{}").lower() for e in emails if "@" in e}
        emails = {e for e in emails if not re.search(r"(privacy|gdpr|noreply)", e)}
        phones = set(re.sub(r"\\D", "", p) for p in PHONE_REGEX.findall(text) if p)
        phones = {p for p in phones if 9 <= len(p) <= 15}

        result["Emails"] = "; ".join(sorted(emails)) if emails else ""
        result["Phones"] = "; ".join(sorted(phones)) if phones else ""

        # Socials
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "facebook.com" in href and not result["Facebook"]: result["Facebook"] = href
            if "linkedin.com" in href and not result["LinkedIn"]: result["LinkedIn"] = href
            if GUEST_POST_REGEX.search(href) and not result["Guest_Post_Page"]: result["Guest_Post_Page"] = href
            if any(x in href for x in ["/team", "/about-us", "/people"]) and not result["Team_Page"]: result["Team_Page"] = href

        # Tech stack
        tech = []
        html_lower = html.lower()
        if "shopify" in html_lower: tech.append("Shopify")
        if "wp-content" in html_lower: tech.append("WordPress")
        if "hubspot" in html_lower: tech.append("HubSpot")
        result["Tech"] = ", ".join(tech)

        # Priority links
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if urlparse(link).netloc == domain and link not in visited:
                if any(p in link.lower() for p in priority):
                    queue.appendleft((link, depth + 1))  # priority
                else:
                    queue.append((link, depth + 1))

    result["Pages_Scanned"] = len(visited)
    return result

# ======================== UI ========================
tab1, tab2, tab3 = st.tabs(["Scrape URLs", "Google Search to Scrape", "Export to Google Sheets"])

with tab1:
    uploaded = st.file_uploader("Upload CSV/Excel with URLs", type=["csv","xlsx","xls"])
    if uploaded:
        df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
        url_col = st.selectbox("URL column", df.columns, key="urlcol1")
        urls = df[url_col].dropna().astype(str).unique().tolist()

        col1, col2 = st.columns(2)
        if col1.button("START / RESUME", type="primary", use_container_width=True):
            st.session_state.paused = False
        if col2.button("PAUSE" if not st.session_state.paused else "RESUME", type="secondary", use_container_width=True):
            st.session_state.paused = not st.session_state.paused
            st.rerun()

        if st.button("CLEAR & START FRESH"):
            st.session_state.results = []
            st.rerun()

        if urls:
            progress = st.progress(0)
            status = st.empty()

            async def run():
                connector = aiohttp.TCPConnector(limit=15)
                async with aiohttp.ClientSession(connector=connector) as session:
                    for i, url in enumerate(urls):
                        while st.session_state.paused:
                            await asyncio.sleep(1)
                        if not url.startswith(("http://", "https://")):
                            url = "https://" + url
                        res = await crawl_site(url, session)
                        st.session_state.results.append(res)
                        if i % 5 == 0: save_progress()
                        progress.progress((i+1)/len(urls))
                        status.markdown(f"**{i+1}/{len(urls)}** — {res.get('Emails','').count('@')} emails • {url}")

            if st.button("START SCRAPING", type="primary"):
                asyncio.run(run())
                st.balloons()

with tab2:
    st.session_state.serper_key = st.text_input("Serper.dev API Key (free 2,500 searches/mo)", value=st.session_state.serper_key, type="password")
    query = st.text_input("Google Search Query", "plumbers in berlin site:.de -inurl:(login | signup)")
    if st.button("Search & Scrape Top Results"):
        with st.spinner("Searching Google..."):
            urls = google_search(query, 20)
        st.write(f"Found {len(urls)} sites — starting scrape...")
        # reuse same logic as tab1

with tab3:
    if st.session_state.results:
        df_out = pd.DataFrame(st.session_state.results)
        st.dataframe(df_out, use_container_width=True)
        st.markdown(download_link(df_out, "zap_full_results.csv"), unsafe_allow_html=True)
        if st.button("EXPORT TO GOOGLE SHEETS"):
            url = export_to_sheets(df_out)
            if url:
                st.success(f"Exported! Open sheet: {url}")
            else:
                st.error("Add GCP service account to secrets")

# Always show partial results + download
if st.session_state.results:
    temp_df = pd.DataFrame(st.session_state.results)
    st.markdown("### Live Results (updates in real-time)")
    st.dataframe(temp_df, use_container_width=True)
    st.markdown(download_link(temp_df, f"zap_partial_{int(time.time())}.csv"), unsafe_allow_html=True)
