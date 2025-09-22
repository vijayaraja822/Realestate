import requests
import csv
from supabase import create_client, Client

# --- CONFIG ---
API_KEY = "AIzaSyDdtH7oluFQlT8v4blGDoVScCKLkLEohiQ"
INPUT_FILE = r"/scripts/channel_ids.txt"
OUTPUT_FILE = r"/scripts/youtube_stats.csv"

# Supabase config (use service_role key!)
SUPABASE_URL = "https://oamdkywzggcmhldgmmav.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9hbWRreXd6Z2djbWhsZGdtbWF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODAwMTcxMCwiZXhwIjoyMDczNTc3NzEwfQ.dPka_ZzaT_6rJYCSDvSXyyds8ZAII03WtNyHfpSEI4w"
TABLE_NAME = "youtube_stats"

# Connect to Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- FUNCTIONS ---
def get_channel_stats(channel_id, api_key):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={channel_id}&key={api_key}"
    response = requests.get(url)
    data = response.json()
    
    if "items" not in data or len(data["items"]) == 0:
        return None
    
    item = data["items"][0]
    stats = item["statistics"]
    snippet = item["snippet"]
    
    return {
        "channel_id": channel_id,
        "channel_name": snippet["title"],   # must match your Supabase column name
        "subscribers": int(stats.get("subscriberCount", 0)),
        "views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0))
    }

# --- MAIN ---
channel_ids = []
with open(INPUT_FILE, "r") as f:
    channel_ids = [line.strip() for line in f if line.strip()]

all_stats = []

for cid in channel_ids:
    stats = get_channel_stats(cid, API_KEY)
    if stats:
        all_stats.append(stats)

        # Insert into Supabase
        response = supabase.table(TABLE_NAME).insert(stats).execute()
        print(f"Inserted {cid} → Supabase response: {response}")
    else:
        print(f"Failed to get stats for {cid}")

# Write to CSV
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["channel_id", "channel_name", "subscribers", "views", "video_count"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for s in all_stats:
        writer.writerow(s)

print(f"✅ Stats saved to {OUTPUT_FILE} and Supabase")
