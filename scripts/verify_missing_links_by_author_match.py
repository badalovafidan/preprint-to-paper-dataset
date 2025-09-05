import pandas as pd
import re

# file path
file_path   = r"data/foundMissingCrossrefData.csv"
output_path = r"data/verifiedMissingCrossrefData.csv"

# read file
df = pd.read_csv(file_path)

# nested tokenization [['family1','given1'], ['family2','given2']]
def tokenize_nested(authors_str):
    if pd.isna(authors_str) or str(authors_str).strip() == "":
        return []
    author_list = [a.strip() for a in str(authors_str).split(";") if a.strip()]
    nested_tokens = []
    for author in author_list:
        tokens = re.findall(r"\w+", author.lower(), flags=re.UNICODE)
        if tokens:
            nested_tokens.append(tokens)
    return nested_tokens

# calculation author match score
def author_match_score(biorxiv_str, crossref_str):
    biorxiv_authors  = tokenize_nested(biorxiv_str)
    crossref_authors = tokenize_nested(crossref_str)
    if not biorxiv_authors or not crossref_authors:
        return 0.0
    matched = 0
    used = set() 
    for cr_author in crossref_authors:
        cr_tokens = set(cr_author)
        for i, bx_author in enumerate(biorxiv_authors):
            if i in used:
                continue
            if cr_tokens.intersection(bx_author):
                matched += 1
                used.add(i)
                break
    denom = max(len(crossref_authors), len(biorxiv_authors))
    score = matched / denom if denom else 0.0
    return round(score, 3)

# author match score
df["author_match_score_new"] = df.apply(
    lambda row: author_match_score(row.get("biorxiv_authors_last", ""),
                                   row.get("crossref_authors", "")),
    axis=1
)

# author count diff
df["author_count_diff"] = df.apply(
    lambda row: abs(len(tokenize_nested(row.get("biorxiv_authors_last", ""))) -
                    len(tokenize_nested(row.get("crossref_authors", "")))),
    axis=1
)

# update custom_status to "gray zone" 
mask_found = df["crossref_title"].astype(str).str.strip().ne("") | \
             df["biorxiv_published_doi"].astype(str).str.strip().ne("")

mask_preprint_only = df["custom_status"].astype(str).str.lower().eq("preprint only")

df.loc[mask_found & mask_preprint_only, "custom_status"] = "gray zone"


# write result
df.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"done: {output_path}")