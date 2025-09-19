import pandas as pd
from datetime import datetime, timezone
from supabase import create_client, Client
import math

# ---------------- Supabase Connection ----------------
url = "https://oamdkywzggcmhldgmmav.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9hbWRreXd6Z2djbWhsZGdtbWF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODAwMTcxMCwiZXhwIjoyMDczNTc3NzEwfQ.dPka_ZzaT_6rJYCSDvSXyyds8ZAII03WtNyHfpSEI4w"
supabase: Client = create_client(url, key)

# ---------------- File Paths ----------------
magicbricks_file = "Magicbricks scrapp.csv"
housing_file = "housing_projects.csv"

# ---------------- Column Mapping ----------------
COLUMN_MAP = {
    "Project Name": "project_name",
    "Location": "location",
    "Price Range": "price_range",
    "BHK": "bhk",
    "Possession Status": "possession_status",
    "Amenities Count": "amenities_count",
    "Title": "title",
    "Address": "address",
    "Price": "price",
    "Offer": "offer"
}

# ---------------- Load & Prepare Data ----------------
def load_and_prepare(file_path: str, source_name: str) -> pd.DataFrame:
    print(f"📂 Loading {source_name} file: {file_path}")
    df = pd.read_csv(file_path)

    # Rename columns to snake_case
    df.rename(columns=COLUMN_MAP, inplace=True)

    # Add source + scraped_date
    df["source"] = source_name
    df["scraped_date"] = datetime.now(timezone.utc).isoformat()

    # Replace NaN/NaT with None
    df = df.where(pd.notnull(df), None)

    print(f"✅ {len(df)} rows loaded from {source_name}")
    return df

# ---------------- Clean Data for JSON ----------------
def clean_for_json(data: list) -> list:
    for row in data:
        for k, v in row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
    return data

# ---------------- Main ----------------
def main():
    # Load CSVs
    df_magic = load_and_prepare(magicbricks_file, "magicbricks")
    df_housing = load_and_prepare(housing_file, "housing")

    # Combine datasets
    combined_df = pd.concat([df_magic, df_housing], ignore_index=True)
    print(f"📊 Total rows combined: {len(combined_df)}")

    # Final NaN cleanup
    combined_df = combined_df.where(pd.notnull(combined_df), None)

    # Save backup CSV
    backup_file = f"properties_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    combined_df.to_csv(backup_file, index=False, encoding="utf-8")
    print(f"💾 Backup saved locally as {backup_file}")

    # Convert to list of dicts
    data = combined_df.to_dict(orient="records")
    data = clean_for_json(data)

    if data:
        # Print a sample row for schema check
        print("🔎 Sample row to upload:", data[0])

        batch_size = 200
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            try:
                supabase.table("properties").insert(batch).execute()
                print(f"✅ Uploaded rows {i+1} to {i+len(batch)}")
            except Exception as e:
                print(f"❌ Error uploading rows {i+1}-{i+len(batch)}: {e}")
    else:
        print("⚠️ No data to upload")

if __name__ == "__main__":
    main()
 