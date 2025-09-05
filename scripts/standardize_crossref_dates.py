import pandas as pd
from datetime import datetime

# read file
df = pd.read_csv("data/verifiedMissingCrossrefData.csv")

# function for formating the date MM/DD/YYYY
def standardize_date(date_str):
    try:
        if pd.isna(date_str) or str(date_str).strip() == "":
            return ""

        s = str(date_str).strip()

        # if date format is correct then dont change
        try:
            datetime.strptime(s, "%m/%d/%Y")
            return s
        except:
            pass

        # for YYYY-MM-DD
        if len(s.split("-")) == 3:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%m/%d/%Y")
        # for YYYY-MM
        elif len(s.split("-")) == 2:
            year, month = s.split("-")
            return f"{int(month):02d}/01/{year}"
        # for YYYY
        elif len(s.split("-")) == 1 and len(s) == 4:
            return f"12/31/{s}"
        else:
            return ""
    except:
        return ""

# renew dates in the same column
df['crossref_online_publication_date'] = df['crossref_online_publication_date'].apply(standardize_date)
df['crossref_issue_online_date'] = df['crossref_issue_online_date'].apply(standardize_date)

# output
df.to_csv("data/standardizedCrossrefData.csv", index=False)

print("done")