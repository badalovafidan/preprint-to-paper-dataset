import pandas as pd
import numpy as np

# path files
in_file  = r"data/standardizedCrossrefData.csv"
out_file = r"data/mergedPublicationDate.csv"
# read file
df = pd.read_csv(in_file, dtype="string", low_memory=False)

online_col = "crossref_online_publication_date"
issue_col  = "crossref_issue_online_date"

# clean gaps
for c in (online_col, issue_col):
    if c in df.columns:
        df[c] = df[c].astype("string").str.strip()

# checking values nan or empty
m_online = df[online_col].notna() & (df[online_col] != "")
m_issue  = df[issue_col].notna()  & (df[issue_col]  != "")

# if there is a publication online date, select only it
# select the issue date if there is only an issue date 
df["crossref_publication_date"] = np.where(
    m_online, df[online_col],
    np.where(m_issue, df[issue_col], pd.NA)
)

# publication date type column
df["crossref_publication_type"] = np.where(
    m_online, "online published",
    np.where(m_issue, "issue", pd.NA)
)

# write result
df.to_csv(out_file, index=False)
print("done", out_file)