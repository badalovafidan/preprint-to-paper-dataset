import pandas as pd

# read file
input_path = r"data/filteredBiorxivData.csv"
output_path = r"data/pivotedBiorxivData.csv"

df = pd.read_csv(input_path)

# version is converted to a number so that it can be sorted correctly
df["version"] = pd.to_numeric(df["version"], errors="coerce")
df = df.dropna(subset=["version"])  # delete nan
df["version"] = df["version"].astype(int)

# extract function for the 1st and last version
def extract_first_last_versions(group: pd.DataFrame) -> pd.Series:
    group_sorted = group.sort_values(by="version")
    first = group_sorted.iloc[0]
    last  = group_sorted.iloc[-1]

    merged = {
        "biorxiv_doi": first["doi"],
        "biorxiv_title_1st": first["title"],
        "biorxiv_title_last": last["title"],
        "biorxiv_authors_1st": first["authors"],
        "biorxiv_authors_last": last["authors"],
        "biorxiv_author_corresponding_1st": first["author_corresponding"],
        "biorxiv_author_corresponding_last": last["author_corresponding"],
        "biorxiv_author_corresponding_institution": last["author_corresponding_institution"],
        "biorxiv_submission_date_1st": first["date"],
        "biorxiv_submission_date_last": last["date"],        
        "biorxiv_version_last": last["version"],
        "biorxiv_type_last": last["type"],
        "biorxiv_license": last["license"],
        "biorxiv_category": last["category"],
        "biorxiv_jatsxml": last["jatsxml"],
        "biorxiv_abstract_1st": first["abstract"],
        "biorxiv_abstract_last": last["abstract"],
        "biorxiv_published_doi": last["published"],
    }

    return pd.Series(merged)

# group by doi and pivot
merged_df = (
    df.groupby("doi", group_keys=False)
      .apply(extract_first_last_versions)
      .reset_index(drop=True)
)

# write result
merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print("done:", output_path)