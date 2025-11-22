# Zap Scraper — FINAL WORKING VERSION (2025)
# Async + crawls internal pages + finds real emails + Facebook/LinkedIn
import streamlit as st
import pandas as pd
import re
import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque

# ======================== MODERN UI ========================
st.set_page_config(page_title="Zap Scraper", page_icon="zap", layout="wide")

st.markdown("""
<style>
    .big-title {
        font-size: 4.5rem !important;
        font-weight: 900;
        text-align: center;
        background: linear-gradient(90deg, #ff006e, ffbe0b, 8338ec, 3a86ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0 0 1rem 0;
    }
    .stButton>button {
        background: #e63946 !important;
        color: white !important;
        height: 3.5rem;
        border-radius: 16px;
        font-size: 1.2rem;
        font-weight: bold;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 16px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="big-title">Zap Scraper</h1>', unsafe_allow_html=True)
st.markdown("**The only scraper you’ll ever need — finds hidden emails, Facebook & LinkedIn in seconds**")

# ======================== EMAIL REGEX (FIXED!) ========================
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.IGNORECASE)

# ======================== UPLOAD ========================
uploaded = st.file_uploader("Upload your CSV or Excel file with URLs", type=["csv", "xlsx", "xls"])

if not uploaded:
    st.info("Upload a file to begin")
    st.stop()

# Load file
df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
st.success(f"Loaded {len(df):,} websites")
st.dataframe(df.head(10))

url_col = st.selectbox("Which column has the URLs?", df.columns)
max_pages = st.slider("Max pages to crawl per site", 5, 50, 15, help="Higher = more emails, slower")

# ======================== CORE SCRAPER ========================
async def crawl_site(start_url, session, max_pages=15):
    collected = set()
    facebook = ""
    linkedin = ""
    visited = set()
    queue = deque([(start_url, 0)])
    domain = urlparse(start_url).netloc

    while queue and len(visited) < max_pages:
        url, depth = queue.popleft()
        if url in visited or depth > 3:
            continue
        visited.add(url)

        try:
            async with session.get(url, timeout=12) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text()
        except:
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ")

        # Extract emails
        emails = set(EMAIL_REGEX.findall(html + text))
        # Clean
        emails = {e.strip(".,;:'\"()[]{}").lower() for e in emails}
        emails = {e for e in emails if "@" in e and "." in e.split("@")[-1]}
        emails = {e for e in emails if not re.search(r"(privacy|gdpr|dpo|abuse|noreply)", e)}
        collected.update(emails)

        # Social links
        if not facebook:
            fb = soup.find("a", href=re.compile(r"facebook\.com", re.I))
            if fb and fb.get("href"):
                facebook = fb["href"]
        if not linkedin:
            ln = soup.find("a", href=re.compile(r"linkedin\.com", re.I))
            if ln and ln.get("href"):
                linkedin = ln["href"]

        # Find internal links
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if urlparse(link).netloc == domain and link not in visited:
                queue.append((link, depth + 1))

    return {
        "Website": start_url,
        "Emails": "; ".join(sorted(collected)) if collected else "",
        "Emails_Count": len(collected),
        "Facebook": facebook or "",
        "LinkedIn": linkedin or "",
        "Pages_Scanned": len(visited)
    }

# ======================== RUN ========================
if st.button("START SCRAPING", type="primary", use_container_width=True):
    urls = df[url_col].dropna().astype(str).unique().tolist()
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(urls)

    async def run():
        timeout = aiohttp.ClientTimeout(total=20)
        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for i, url in enumerate(urls):
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                result = await crawl_site(url, session, max_pages)
                results.append(result)
                progress_bar.progress((i + 1) / total)
                status_text.markdown(f"**{i+1}/{total}** → {result['Emails_Count']} emails from `{url}`")

    # Run the async beast
    asyncio.run(run())

    # Final results
    final_df = pd.DataFrame(results)
    st.balloons()
    st.success(f"DONE! Found {final_df['Emails_Count'].sum():,} emails from {len(results)} sites")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Emails", final_df['Emails_Count'].sum())
    col2.metric("Sites with Email", (final_df['Emails_Count'] > 0).sum())
    col3.metric("Avg per Site", round(final_df['Emails_Count'].mean(), 1))

    st.dataframe(final_df, use_container_width=True)

    csv = final_df.to_csv(index=False).encode()
    st.download_button(
        "DOWNLOAD FULL RESULTS",
        data=csv,
        file_name="zap_scraper_results.csv",
        mime="text/csv",
        use_container_width=True
    )
