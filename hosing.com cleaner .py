import os
import math
import pandas as pd
from supabase import create_client, Client

# ---------------- Supabase Connection ----------------
url = "https://oamdkywzggcmhldgmmav.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9hbWRreXd6Z2djbWhsZGdtbWF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODAwMTcxMCwiZXhwIjoyMDczNTc3NzEwfQ.dPka_ZzaT_6rJYCSDvSXyyds8ZAII03WtNyHfpSEI4w"

supabase: Client = create_client(url, key)

# ---------------- Data Cleaning ----------------
input_file = "housing_projects_cleaned.csv"
df = pd.read_csv(input_file)

df = df.drop_duplicates()
df = df.dropna(how="all")

# ✅ Ensure numeric columns are cleaned
if "Min Price (Lakh)" in df.columns:
    df["Min Price (Lakh)"] = pd.to_numeric(df["Min Price (Lakh)"], errors="coerce")

if "Max Price (Lakh)" in df.columns:
    df["Max Price (Lakh)"] = pd.to_numeric(df["Max Price (Lakh)"], errors="coerce")

if "Max BHK" in df.columns:
    df["Max BHK"] = pd.to_numeric(df["Max BHK"], errors="coerce").fillna(0).astype(int)

output_file = "HousingProjects_cleaned.csv"

try:
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✅ File saved as {output_file}")
except PermissionError:
    alt_file = "HousingProjects_cleaned_new.csv"
    df.to_csv(alt_file, index=False, encoding="utf-8")
    output_file = alt_file
    print(f"⚠️ Original file was locked. Saved instead as {alt_file}")

# ---------------- Upload to Supabase ----------------
# Rename columns to SQL-friendly names
df = df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"))

# ✅ Fix column names to match Supabase schema
df = df.rename(columns={
    "min_price_(lakh)": "min_price_lakh",
    "max_price_(lakh)": "max_price_lakh"
})

# ✅ Replace NaN, NaT, inf, -inf with None
df = df.replace([float("inf"), float("-inf")], None)
df = df.where(pd.notnull(df), None)

# Convert DataFrame to list of dicts
data = df.to_dict(orient="records")

# 🔥 Ensure no NaN sneaks through
for row in data:
    for key, value in row.items():
        if isinstance(value, float) and math.isnan(value):
            row[key] = None

# ✅ Upload to Supabase
table_name = "HousingProjects"
batch_size = 500

if data:
    try:
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            response = supabase.table(table_name).insert(batch).execute()
            print(f"✅ Uploaded rows {i+1} to {i+len(batch)}")
        print(f"🎉 All {len(data)} rows uploaded to Supabase table '{table_name}'")
    except Exception as e:
        print(f"❌ Error uploading to Supabase: {e}")
else:
    print("⚠️ No data to upload to Supabase.")
 