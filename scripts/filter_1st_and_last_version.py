import pandas as pd

# file path
in_csv  = r"data/downloadedBiorxiv.csv"
out_csv = r"data/filteredBiorxivData.csv"

# read file
df = pd.read_csv(in_csv)

# necessary fields: doi and version
if not {"doi", "version"}.issubset(df.columns):
    raise ValueError(f"columns: {list(df.columns)}")

# convert version to numeric
df["version"] = pd.to_numeric(df["version"], errors="coerce")

# filtering function for groups
def keep_first_and_last_if_first_exists(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("version")
    min_v = g["version"].min()

    # if version 1st does not exist, it is deleted.
    if pd.isna(min_v) or int(min_v) != 1:
        return g.iloc[0:0] 

    #  keep only version 1 and the last one
    max_v = g["version"].max()
    keep = g[g["version"].isin([1, max_v])].drop_duplicates(subset=["doi", "version"])
    return keep

# apply rules
filtered = (
    df.groupby("doi", group_keys=False)
      .apply(keep_first_and_last_if_first_exists)
      .reset_index(drop=True)
)

# write result
filtered.to_csv(out_csv, index=False, encoding="utf-8-sig")

# print 
print(f"done:  {out_csv}")