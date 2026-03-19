"""
Data Cleaning Pipeline
Automated data cleaning, validation and quality reporting using Python and Pandas.
Author: Azahid García | github.com/azahidgarcia
"""

import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path


# ── Configuration ──────────────────────────────────────────────
INPUT_FILE    = "data/input/raw_dataset.csv"
OUTPUT_FOLDER = "data/output"
LOG_FOLDER    = "logs"


# ── Load Data ──────────────────────────────────────────────────
def load_data(filepath: str) -> pd.DataFrame:
    """Load CSV file into a DataFrame."""
    print(f"📂 Loading: {filepath}")
    df = pd.read_csv(filepath)
    print(f"   Rows: {len(df)} | Columns: {len(df.columns)}")
    return df


# ── Cleaning Functions ─────────────────────────────────────────
def remove_duplicates(df: pd.DataFrame, log: dict) -> pd.DataFrame:
    """Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    log["duplicates_removed"] = removed
    print(f"   🔁 Duplicates removed: {removed}")
    return df


def fix_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
    print(f"   ✂️  Whitespace fixed in {len(str_cols)} column(s)")
    return df


def standardize_text(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert specified text columns to title case."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.title()
    print(f"   🔤 Text standardized in: {columns}")
    return df


def fix_dates(df: pd.DataFrame, date_columns: list, log: dict) -> pd.DataFrame:
    """Parse and standardize date columns to YYYY-MM-DD format."""
    fixed = 0
    for col in date_columns:
        if col in df.columns:
            original = df[col].copy()
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            fixed += (df[col] != original).sum()
    log["dates_standardized"] = int(fixed)
    print(f"   📅 Date values standardized: {fixed}")
    return df


def flag_nulls(df: pd.DataFrame, log: dict) -> pd.DataFrame:
    """Track null counts per column."""
    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0].to_dict()
    log["null_counts_per_column"] = {k: int(v) for k, v in null_counts.items()}
    log["total_nulls"] = int(df.isnull().sum().sum())
    print(f"   🔍 Nulls detected: {log['total_nulls']} across {len(null_counts)} column(s)")
    return df


def flag_outliers(df: pd.DataFrame, numeric_columns: list, log: dict) -> pd.DataFrame:
    """Flag outliers using IQR method — adds a boolean column per numeric field."""
    flagged_total = 0
    for col in numeric_columns:
        if col in df.columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR
            flag_col = f"{col}_outlier_flag"
            df[flag_col] = ~df[col].between(lower, upper)
            count = int(df[flag_col].sum())
            flagged_total += count
            print(f"   ⚠️  Outliers flagged in '{col}': {count}")
    log["outliers_flagged"] = flagged_total
    return df


def validate_required_fields(df: pd.DataFrame,
                              required: list, log: dict) -> pd.DataFrame:
    """Flag rows where required fields are missing."""
    df["validation_flag"] = False
    for col in required:
        if col in df.columns:
            df["validation_flag"] |= df[col].isnull()
    invalid = int(df["validation_flag"].sum())
    log["rows_failed_validation"] = invalid
    print(f"   ❌ Rows failing required field validation: {invalid}")
    return df


# ── Quality Report ─────────────────────────────────────────────
def build_quality_report(df_original: pd.DataFrame,
                         df_clean: pd.DataFrame, log: dict) -> dict:
    """Assemble the full quality report."""
    return {
        "generated_at":            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows_original":           len(df_original),
        "rows_after_cleaning":     len(df_clean),
        "columns":                 list(df_clean.columns),
        "duplicates_removed":      log.get("duplicates_removed", 0),
        "dates_standardized":      log.get("dates_standardized", 0),
        "total_nulls_remaining":   log.get("total_nulls", 0),
        "null_counts_per_column":  log.get("null_counts_per_column", {}),
        "outliers_flagged":        log.get("outliers_flagged", 0),
        "rows_failed_validation":  log.get("rows_failed_validation", 0),
    }


# ── Export ─────────────────────────────────────────────────────
def export_results(df: pd.DataFrame, report: dict) -> None:
    """Save cleaned CSV and JSON quality report."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(LOG_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Clean dataset
    csv_path = os.path.join(OUTPUT_FOLDER, f"clean_dataset_{timestamp}.csv")
    df.to_csv(csv_path, index=False)
    print(f"\n✅ Clean dataset saved : {csv_path}")

    # Quality report
    log_path = os.path.join(LOG_FOLDER, f"quality_report_{timestamp}.json")
    with open(log_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"✅ Quality report saved: {log_path}")


# ── Main Pipeline ──────────────────────────────────────────────
def run_pipeline():
    print("=" * 55)
    print("  Data Cleaning Pipeline")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    log = {}

    # Load
    df_original = load_data(INPUT_FILE)
    df = df_original.copy()

    print("\n🧹 Running cleaning steps...")

    # Clean
    df = remove_duplicates(df, log)
    df = fix_whitespace(df)
    df = standardize_text(df, columns=["customer_name", "status", "region"])
    df = fix_dates(df, date_columns=["order_date"], log=log)
    df = flag_nulls(df, log)
    df = flag_outliers(df, numeric_columns=["quantity", "unit_price"], log=log)
    df = validate_required_fields(df, required=["customer_name", "order_date"], log=log)

    # Report & Export
    report = build_quality_report(df_original, df, log)
    export_results(df, report)

    print(f"\n📊 Summary:")
    print(f"   Original rows  : {report['rows_original']}")
    print(f"   Clean rows     : {report['rows_after_cleaning']}")
    print(f"   Duplicates     : {report['duplicates_removed']}")
    print(f"   Nulls remaining: {report['total_nulls_remaining']}")
    print(f"   Outliers flagged: {report['outliers_flagged']}")
    print("\n🎉 Pipeline complete!")
    print("=" * 55)


if __name__ == "__main__":
    run_pipeline()
