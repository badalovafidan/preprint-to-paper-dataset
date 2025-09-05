import pandas as pd
import requests
import time

# file path
INPUT_CSV  = "data/pivotedBiorxivData.csv"
OUTPUT_CSV = "data/downloadedCrossref.csv"

# simple user agent for crossref, recommended for polite requests
HEADERS = {
    "User-Agent": "biorxiv-crossref-linkage/1.0 (mailto:your.email@example.com)"
}

# function that gets all the necessary fields from the Crossref API
def fetch_crossref_metadata(doi: str):   
    try:
        url = f"https://api.crossref.org/works/{doi}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        msg = r.json().get("message", {})

        # title
        title_list = msg.get("title") or []
        crossref_title = title_list[0] if title_list else ""

        # journal name
        cont_list = msg.get("container-title") or []
        crossref_journal = cont_list[0] if cont_list else ""

        # authors
        authors = msg.get("author") or []
        author_strs = []
        for a in authors:
            given = (a.get("given") or "").strip()
            family = (a.get("family") or "").strip()
            if family and given:
                author_strs.append(f"{family}, {given}")
            elif family:
                author_strs.append(family)
            elif given:
                author_strs.append(given)
        crossref_authors = "; ".join(author_strs)

        # dates (published-online / published-print)
        def dateparts_to_str(obj):
            parts = (obj or {}).get("date-parts", [])
            if not parts or not parts[0]:
                return ""
            items = [str(x) for x in parts[0]]
            return "-".join(items)

        crossref_online_publication_date = dateparts_to_str(msg.get("published-online"))
        crossref_issue_online_date = dateparts_to_str(msg.get("published-print"))

        return {
            "crossref_title": crossref_title,
            "crossref_journal": crossref_journal,
            "crossref_authors": crossref_authors,
            "crossref_online_publication_date": crossref_online_publication_date,
            "crossref_issue_online_date": crossref_issue_online_date,
        }
    except requests.exceptions.RequestException as e:
        return {
            "crossref_title": "",
            "crossref_journal": "",
            "crossref_authors": "",
            "crossref_online_publication_date": "",
            "crossref_issue_online_date": "",
        }
    except Exception as e:
        return {
            "crossref_title": "",
            "crossref_journal": "",
            "crossref_authors": "",
            "crossref_online_publication_date": "",
            "crossref_issue_online_date": "",
        }

def main():
    # read csv
    df = pd.read_csv(INPUT_CSV)

    # creates new columns
    for col in [
        "crossref_title",
        "crossref_journal",
        "crossref_authors",
        "crossref_online_publication_date",
        "crossref_issue_online_date",
    ]:
        if col not in df.columns:
            df[col] = ""

    # only rows with published DOI
    published_mask = df["biorxiv_published_doi"].notna() & (df["biorxiv_published_doi"].astype(str).str.strip() != "")
    rows = df[published_mask].copy()

    # request crossref line by line
    for idx in rows.index:
        doi = str(df.at[idx, "biorxiv_published_doi"]).strip()
        if not doi:
            continue

        meta = fetch_crossref_metadata(doi)

        df.at[idx, "crossref_title"] = meta["crossref_title"]
        df.at[idx, "crossref_journal"] = meta["crossref_journal"]
        df.at[idx, "crossref_authors"] = meta["crossref_authors"]
        df.at[idx, "crossref_online_publication_date"] = meta["crossref_online_publication_date"]
        df.at[idx, "crossref_issue_online_date"] = meta["crossref_issue_online_date"]

        print(f"{doi} | {meta['crossref_title'][:60]}")

        # slow down politely, good practice for crossref
        time.sleep(1.0)

        # save regularly, this is useful if the dataset is large
        if idx % 100 == 0:
            df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    # save data
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Done: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()