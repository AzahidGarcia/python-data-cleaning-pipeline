# 🧹 Data Cleaning Pipeline

Automated data cleaning and validation pipeline using Python and Pandas. Detects and fixes the most common data quality issues — duplicates, nulls, inconsistent formats, outliers — and generates a JSON quality report documenting every change.

---

## 📋 What It Does

| Step | Action |
|------|--------|
| 1 | **Load** raw CSV into a DataFrame |
| 2 | **Remove** exact duplicate rows |
| 3 | **Fix** whitespace in all string columns |
| 4 | **Standardize** text to title case |
| 5 | **Parse & standardize** date formats to YYYY-MM-DD |
| 6 | **Count & track** null values per column |
| 7 | **Flag** outliers using IQR method |
| 8 | **Validate** required fields |
| 9 | **Export** clean CSV + JSON quality report |

---

## 📁 Project Structure

```
python-data-cleaning-pipeline/
├── pipeline.py               # Main cleaning pipeline
├── requirements.txt          # Dependencies
├── data/
│   ├── input/
│   │   └── raw_dataset.csv   # Sample messy dataset
│   └── output/               # Clean CSV output (generated)
├── logs/                     # JSON quality reports (generated)
└── README.md
```

---

## ⚡ Quick Start

**1. Clone the repository**
```bash
git clone https://github.com/azahidgarcia/python-data-cleaning-pipeline.git
cd python-data-cleaning-pipeline
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Add your dataset**
```
Replace data/input/raw_dataset.csv with your own CSV file.
```

**4. Run the pipeline**
```bash
python pipeline.py
```

---

## 📊 Example Output

```
=======================================================
  Data Cleaning Pipeline
  Started: 2024-01-22 10:30:00
=======================================================
📂 Loading: data/input/raw_dataset.csv
   Rows: 18 | Columns: 8

🧹 Running cleaning steps...
   🔁 Duplicates removed: 2
   ✂️  Whitespace fixed in 5 column(s)
   🔤 Text standardized in: ['customer_name', 'status', 'region']
   📅 Date values standardized: 3
   🔍 Nulls detected: 5 across 2 column(s)
   ⚠️  Outliers flagged in 'quantity': 3
   ⚠️  Outliers flagged in 'unit_price': 1
   ❌ Rows failing required field validation: 2

✅ Clean dataset saved : data/output/clean_dataset_20240122.csv
✅ Quality report saved: logs/quality_report_20240122.json

📊 Summary:
   Original rows  : 18
   Clean rows     : 16
   Duplicates     : 2
   Nulls remaining: 5
   Outliers flagged: 4

🎉 Pipeline complete!
```

---

## 📄 Quality Report (JSON)

Every run generates a timestamped JSON report:

```json
{
  "generated_at": "2024-01-22 10:30:00",
  "rows_original": 18,
  "rows_after_cleaning": 16,
  "duplicates_removed": 2,
  "dates_standardized": 3,
  "total_nulls_remaining": 5,
  "null_counts_per_column": {
    "customer_name": 2,
    "order_date": 3
  },
  "outliers_flagged": 4,
  "rows_failed_validation": 2
}
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Core language |
| Pandas | Data processing and cleaning |

---

## 🔧 Customization

Edit `pipeline.py` to adapt to your dataset:
- Change `INPUT_FILE` path
- Add columns to `standardize_text()`
- Add columns to `fix_dates()`
- Add required fields to `validate_required_fields()`
- Add or remove cleaning steps as needed

---

## 👤 Author

**Azahid García** — Python Automation & Data Engineer

- 🌐 [Fiverr Profile](https://fiverr.com)
- 💼 [Upwork Profile](https://upwork.com)
- 🐙 [GitHub](https://github.com/azahidgarcia)

---

## 📄 License

MIT License — free to use and modify.
