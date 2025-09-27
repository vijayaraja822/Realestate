import os
import pandas as pd
from fpdf import FPDF
import re

# Config
REPORT_FILE_TXT = "storage_report.txt"
REPORT_FILE_PDF = "storage_report.pdf"
SUPABASE_LIMIT_MB = 500        # Supabase free tier limit
MONTHLY_NEW_ROWS = 100_000     # Estimated new rows per month
LINE_WRAP_CHARS = 100          # Max characters per wrapped line
PDF_FONT_SIZE = 8              # Small font to fit text

def get_csv_files(folder="."):
    """Find all CSV files in folder, ignoring raw files."""
    return [f for f in os.listdir(folder) if f.endswith(".csv") and "raw" not in f.lower()]

def estimate_storage_for_csv(file_path):
    """Calculate average row size and estimated monthly storage."""
    df = pd.read_csv(file_path)
    file_size_bytes = os.path.getsize(file_path)
    num_rows = len(df)
    if num_rows == 0:
        return file_path, 0, 0, 0
    avg_row_size = file_size_bytes / num_rows
    monthly_estimate_bytes = avg_row_size * MONTHLY_NEW_ROWS
    return file_path, avg_row_size, monthly_estimate_bytes, file_size_bytes

def remove_emojis(text):
    """Remove emojis and non-ASCII characters."""
    return re.sub(r'[^\x00-\x7F]+', '', text)

def wrap_text_chars(text, max_chars=LINE_WRAP_CHARS):
    """Wrap text by exact number of characters, ignoring spaces."""
    return [text[i:i+max_chars] for i in range(0, len(text), max_chars)]

def generate_report(folder="."):
    csv_files = get_csv_files(folder)
    if not csv_files:
        print("❌ No cleaned CSV files found!")
        return

    # Merge CSVs
    dfs = [pd.read_csv(f) for f in csv_files]
    df_combined = pd.concat(dfs, ignore_index=True)
    print(f"✅ Combined dataset shape: {df_combined.shape}")

    report_lines = []
    total_monthly_bytes = 0

    report_lines.append("Real Estate Data Storage Report\n")
    report_lines.append("=================================\n")

    for csv_file in csv_files:
        fname, avg_row, monthly_bytes, file_size = estimate_storage_for_csv(csv_file)
        total_monthly_bytes += monthly_bytes

        report_lines.append(f"File: {fname}")
        report_lines.append(f"   Current size: {file_size / (1024*1024):.2f} MB")
        report_lines.append(f"   Average row size: {avg_row:.2f} bytes")
        report_lines.append(f"   Estimated monthly storage: {monthly_bytes / (1024*1024):.2f} MB\n")

    report_lines.append("=================================")
    report_lines.append(f"Total estimated monthly storage: {total_monthly_bytes / (1024*1024):.2f} MB")

    months_until_limit = SUPABASE_LIMIT_MB / (total_monthly_bytes / (1024*1024)) if total_monthly_bytes else float("inf")
    report_lines.append(f"Supabase free tier limit: {SUPABASE_LIMIT_MB} MB")
    if months_until_limit == float("inf"):
        report_lines.append("Conclusion: No data → Free tier will last indefinitely.")
    else:
        report_lines.append(f"At this rate, the free tier will last about {months_until_limit:.1f} months.")

    # --- Save text report ---
    with open(REPORT_FILE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"✅ Text report generated: {REPORT_FILE_TXT}")

    # --- Save PDF report ---
    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_font("Arial", size=PDF_FONT_SIZE)
    pdf_width = pdf.w - 2 * pdf.l_margin  # usable width

    for line in report_lines:
        line_clean = remove_emojis(line)
        wrapped_lines = wrap_text_chars(line_clean, LINE_WRAP_CHARS)
        for wrapped_line in wrapped_lines:
            pdf.multi_cell(pdf_width, 6, wrapped_line)

    pdf.output(REPORT_FILE_PDF)
    print(f"✅ PDF report generated: {REPORT_FILE_PDF}")

if __name__ == "__main__":
    generate_report(".")

