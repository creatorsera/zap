# ⚡ Zap Scraper
A modern, fast, and reliable website analyzer that extracts data, detects blog types, identifies niche categories, and resumes progress even after interruptions.

Zap Scraper gives you powerful tools to analyze hundreds of websites quickly and reliably with a polished modern UI.

---

## 🚀 Features
- **Fast Website Scraping** using Requests + BeautifulSoup
- **Auto-Resume System** so your progress is never lost
- **Blog Detection** (is the website a blog or not?)
- **Niche Detection** using content analysis
- **Clean Modern Red UI** with custom `style.css`
- **Detailed CSV Export** with domain data
- **Streamlit-Optimized** — runs smoothly on the cloud
- **Retry System** for unstable websites
- Lightweight and developer-friendly

---

## 📁 Project Structure
```
zap-scraper/
 ├── main.py
 ├── style.css
 ├── requirements.txt
 └── README.md
```

---

## 🛠 Installation
1. Clone the repo:
```bash
git clone https://github.com/your-username/zap-scraper
cd zap-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run Streamlit:
```bash
streamlit run main.py
```

---

## 🌐 Deploy on Streamlit Cloud
1. Push all files to your GitHub repo.
2. Go to: https://share.streamlit.io
3. Click **Create App**.
4. Select:
   - Repo: `your-username/zap-scraper`
   - Branch: `main`
   - File: `main.py`

5. Deploy 🎉

---

## 📊 Output CSV
Zap exports data into:
```
Zap_Results.csv
```
Containing:
- Domain
- Title
- Description
- Blog Status
- Niche Category
- Status Code
- Timestamp

---

## 🔥 Screenshots (Optional)
Add your screenshots here:
```
/images/home.png
/images/results.png
```

---

## 🧠 Tech Stack
- **Python 3**
- **Streamlit**
- **Requests**
- **BeautifulSoup**
- **Pandas**
- **lxml**
- **tldextract**

---

## 🙋 FAQ
### **Does Zap work on mobile?**
Yes.

### **Can multiple users run Zap at the same time?**
Yes — every Streamlit session is isolated.

### **Does Zap save progress?**
Yes — it resumes automatically.

---

## 📜 License
This project is licensed under the MIT License.

