import pandas as pd

# read file
file_path = r"data/downloadedCrossref.csv"
output_path = r"data/categorizedPreprints.csv"

df = pd.read_csv(file_path)

# create a new column: if biorxiv_published_doi is not empty write "published", otherwise "preprint only"
df["custom_status"] = df["biorxiv_published_doi"].apply(
    lambda x: "published" if pd.notna(x) and str(x).strip() != "" else "preprint only"
)

# save the result
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"done: {output_path}")