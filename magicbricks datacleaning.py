import pandas as pd
import re
import math
from supabase import create_client, Client

# ---------------- Supabase Connection ----------------
url = "https://oamdkywzggcmhldgmmav.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9hbWRreXd6Z2djbWhsZGdtbWF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODAwMTcxMCwiZXhwIjoyMDczNTc3NzEwfQ.dPka_ZzaT_6rJYCSDvSXyyds8ZAII03WtNyHfpSEI4w"

supabase: Client = create_client(url, key)

# ---------------- File Paths ----------------
input_file = "magicbricks_raw.csv"
output_file = "magicbricks_cleaned.csv"

# ---------------- Price Cleaning Function ----------------
def clean_price(price):
    if pd.isna(price):
        return None
    
    price = str(price).strip()
    price = price.replace("₹", "").replace(",", "").strip()

    # Handle Crore (Cr)
    if "Cr" in price:
        match = re.findall(r"[\d\.]+", price)
        if match:
            return float(match[0]) * 1e7  # 1 Cr = 10,000,000

    # Handle Lakh
    if "Lakh" in price or "lac" in price.lower():
        match = re.findall(r"[\d\.]+", price)
        if match:
            return float(match[0]) * 1e5  # 1 Lakh = 100,000

    # Handle ranges (take min value)
    if "-" in price:
        parts = price.split("-")
        try:
            return clean_price(parts[0])  # take first part
        except:
            return None

    # Fallback: try to parse number directly
    match = re.findall(r"[\d\.]+", price)
    if match:
        return float(match[0])
    
    return None

# ---------------- Load & Clean Data ----------------
df = pd.read_csv(input_file)

# ✅ Use the actual column name from your CSV (Price Range)
price_col = "Price Range"  # change if needed

if price_col in df.columns:
    df["price_numeric"] = df[price_col].apply(clean_price)
else:
    print(f"⚠️ No '{price_col}' column found in CSV! Available: {list(df.columns)}")

# ---------------- Save Cleaned File ----------------
try:
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✅ Cleaned file saved as {output_file}")
except PermissionError:
    alt_file = "magicbricks_cleaned_new.csv"
    df.to_csv(alt_file, index=False, encoding="utf-8")
    print(f"⚠️ Original file locked. Saved instead as {alt_file}")
    output_file = alt_file

# ---------------- Prepare Data for Supabase ----------------
df = df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"))
df = df.replace([float("inf"), float("-inf")], None)
df = df.where(pd.notnull(df), None)

data = df.to_dict(orient="records")

for row in data:
    for k, v in row.items():
        if isinstance(v, float) and math.isnan(v):
            row[k] = None

# ---------------- Upload to Supabase ----------------
table_name = "Magicbricks"   # 👈 make sure this table exists in Supabase
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
