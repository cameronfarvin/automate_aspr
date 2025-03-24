import pandas as pd
import requests
import csv
from io import StringIO

# Input (from JavaScript) and output paths
input_csv_path = "./input_data/apsr_results.csv"
output_csv_path = "./output_data/combined_apsr_citations.csv"

# Load the JavaScript-generated CSV
print("Loading input CSV...")
df = pd.read_csv(input_csv_path)

# Dictionary to store citation data for each title
citation_data = {title: [] for title in df["title"]}

# Download and parse citation CSV
def download_citations(url):
    try:
        # Download the CSV
        res = requests.get(url, timeout=15)
        res.raise_for_status()

        # Parse the CSV content
        csv_text = res.text
        reader = csv.reader(StringIO(csv_text))
        headers = next(reader, None)

        # Extract the second column (citations)
        citations = []
        if headers and len(headers) >= 2:
            for row in reader:
                # We expect there to be exactly 2 columns, though we only use the second.
                if len(row) == 2:
                    citations.append(f"{row[1]}")
        return citations

    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return ["[DOWNLOAD FAILED]"]

# Fetch citations for each title
print("Fetching citations...")
for _, row in df.iterrows():
    title = row["title"]
    url = row["all_citing_papers_link"]
    citation_data[title] = download_citations(url)

# Determine longest list to standardize column lengths
max_len = max(len(citations) for citations in citation_data.values())

# Pad each list to match the longest list for use with Pandas DataFrames.
for key in citation_data:
    citation_data[key].extend([''] * (max_len - len(citation_data[key])))

# Write the consolidated CSV
print("Saving consolidated CSV...")
consolidated_df = pd.DataFrame(citation_data)
consolidated_df.to_csv(output_csv_path, index=False)

print(f"Finished: Output saved to {output_csv_path}")
