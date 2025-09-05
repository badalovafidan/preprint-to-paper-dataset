import pandas as pd

# file path
INPUT_CSV  = r"data/mergedPublicationDate.csv"
OUTPUT_CSV = r"data/calculatedDateDifference.csv"

df = pd.read_csv(INPUT_CSV)

# convert date time
date_cols = [
    "biorxiv_submission_date_1st",
    "biorxiv_submission_date_last",
    "crossref_publication_date",
]
for c in date_cols:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=True)

# difference between last and 1st submission date for all preprints in biorxiv
df["custom_biorxivVersion_dateDifference"] = (
    (df["biorxiv_submission_date_last"] - df["biorxiv_submission_date_1st"])
    .dt.days
)

#  difference between publish and submission date for only published and gray zone preprints
status = df.get("custom_status", pd.Series(index=df.index, dtype="object")).astype(str).str.lower()
mask_gp = status.isin(["gray zone", "published"])

df["custom_submission&publication_dateDiff"] = pd.NA
df.loc[mask_gp, "custom_submission&publication_dateDiff"] = (
    (df.loc[mask_gp, "crossref_publication_date"] - df.loc[mask_gp, "biorxiv_submission_date_last"])
    .dt.days
)

# write result
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"done: {OUTPUT_CSV}")