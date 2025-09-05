import pandas as pd          # read/write csv files
import requests              # request to API
import time                  # for sleep function
import unicodedata           # normalization text
from difflib import SequenceMatcher  # calculation similarity

# file path
INPUT_CSV  = "data/categorizedPreprints.csv"
OUTPUT_CSV = "data/foundMissingCrossrefData.csv"
# seconds between API requests
SLEEP_SECONDS = 1
# threshold for similarity percentage 
TITLE_MATCH_THRESHOLD = 0.75

HEADERS = {
    "User-Agent": "biorxiv-missing-links/1.0 (mailto:your.email@example.com)"
}

# function normalization text
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s_norm = unicodedata.normalize("NFKD", s)
    s_ascii = "".join(ch for ch in s_norm if not unicodedata.combining(ch))
    return " ".join(s_ascii.split()).strip()

# calculation function of similarity between two titles
def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# convert date object to string
def dateparts_to_str(obj) -> str:
    parts = (obj or {}).get("date-parts", [])
    if not parts or not parts[0]:
        return ""
    return "-".join(str(x) for x in parts[0])

# author list
def build_author_string(author_list) -> str:
    if not author_list:
        return ""
    names = []
    for a in author_list:
        given  = normalize_text(a.get("given", "")).strip()
        family = normalize_text(a.get("family", "")).strip()
        if family and given:
            names.append(f"{family}, {given}")
        elif family:
            names.append(family)
        elif given:
            names.append(given)
    return "; ".join(names)

# paper types
ALLOWED_TYPES = {"journal-article", "proceedings-article"}

# search for title and get the best similar paper
def best_crossref_match(title: str):
    url = "https://api.crossref.org/works"
    params = {
        "query.title": title,
        "rows": 10,
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        items = r.json().get("message", {}).get("items", [])
    except Exception:
        return None

    best = None
    best_score = -1.0
    for it in items:
        cr_type = (it.get("type") or "").lower()
        if cr_type not in ALLOWED_TYPES:
            continue  # only ALLOWED_TYPES

        cr_title_list = it.get("title") or [""]
        cr_title = cr_title_list[0] if cr_title_list else ""
        score = title_similarity(title, cr_title)

        if score >= TITLE_MATCH_THRESHOLD and score > best_score:
            best = it
            best_score = score

    if best is None:
        return None

    cr_title   = (best.get("title") or [""])[0]
    cr_journal = (best.get("container-title") or [""])[0]
    cr_authors = build_author_string(best.get("author"))
    cr_online  = dateparts_to_str(best.get("published-online"))
    cr_issue   = dateparts_to_str(best.get("published-print"))
    cr_doi     = best.get("DOI", "")

    return {
        "crossref_title": cr_title,
        "crossref_journal": cr_journal,
        "crossref_authors": cr_authors,
        "crossref_online_publication_date": cr_online,
        "crossref_issue_online_date": cr_issue,
        "biorxiv_published_doi": cr_doi,
        "title_match_score": round(best_score, 2),
    }

# read input file
df = pd.read_csv(INPUT_CSV)

# check the output columns exist (if not, create them)
for col in [
    "biorxiv_published_doi",
    "crossref_title",
    "crossref_journal",
    "crossref_authors",
    "crossref_online_publication_date",
    "crossref_issue_online_date",
    "title_match_score",
]:
    if col not in df.columns:
        df[col] = ""

# filter "preprint only" rows
mask = df["custom_status"].astype(str).str.lower().eq("preprint only")
subset_idx = df.index[mask]

# search on each selected row in crossref
for n, idx in enumerate(subset_idx, 1):
    # if biorxiv_title_last is available, use it, if not, use biorxiv_title_1st.
    q_title = str(
        df.at[idx, "biorxiv_title_last"] if "biorxiv_title_last" in df.columns and pd.notna(df.at[idx, "biorxiv_title_last"])
        else df.at[idx, "biorxiv_title_1st"]
    ).strip()

    if not q_title:
        continue

    print(f"[{n}/{len(subset_idx)}] searching: {q_title[:80]}…")
    match = best_crossref_match(q_title)

    if match:
        for k, v in match.items():
            df.at[idx, k] = v
            
    # avoid making requests to api too quickly
    time.sleep(SLEEP_SECONDS) 

# write result
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"done: {OUTPUT_CSV}")