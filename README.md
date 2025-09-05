# BioRxiv–Crossref Preprint–Publication Matching

## Introduction
Code for collecting, enriching, and matching BioRxiv and Crossref preprint/publication metadata. Provides a pipeline to query APIs, calculate custom fields (title/author match score, author count difference, date differences), and detect “Gray Zone” cases (articles published but missing DOIs in BioRxiv).

## Functionality
This repository provides **codes** of preprint–publication linkage research based on BioRxiv and Crossref sources. The project:
- Collects preprint metadata via the BioRxiv API,
- Completes journal metadata from the Crossref API for preprints with “Published” status,
- Merges published preprint date fields (online vs issue) and calculates date differences,
- Detects “Gray Zone” cases (publications without DOI links in BioRxiv but highly likely published) based on **title similarity**,
- Calculates **author-based** metrics such as **author similarity** and **count difference**.

### Requirements
- Python **3.9+**

### Install dependencies
```bash
pip install -r requirements.txt
```

## How to run

### **Step 1: BioRxiv Metadata Retrieval**
1. `download_biorxiv.py` – Download BioRxiv preprint metadata 

```bash
python download_biorxiv.py
```
2. `filter_1st_and_last_version.py` – Keep only first & last versions of each preprint 

```bash
python filter_1st_and_last_version.py
```
3. `pivot_biorxiv_data.py` – Pivot table

```bash
python pivot_biorxiv_data.py
```

### **Step 2: Crossref Metadata Retrieval**
- `download_crossref.py` – Download publication metadata from Crossref for all published DOIs  from BioRxiv

```bash
python download_crossref.py
```

### **Step 3: Categorization of Preprints**
- `categorize_preprints.py` – Label preprints as:
  - **published** (with DOI link)  
  - **preprint only** (no publication yet)  
  
```bash
python categorize_preprints.py
```

### **Step 4: Identification of Unlinked Publications In BioRxiv - Gray Zone**
- `find_missing_links_crossref.py` – Search BioRxiv title in Crossref for *preprint only* cases by **title similarity**  

```bash
python find_missing_links_crossref.py
```

### **Step 5: Gray Zone Verification (Author Metrics)**
- `verify_missing_links_by_author_match.py` – Confirm title matches using **author similarity** and calculate **author match score**  

```bash
python verify_missing_links_by_author_match.py
```

### **Step 6: Final Dataset Generation**
1. `standardize_crossref_dates.py` – Normalize publication dates (MM/DD/YYYY format)  
    
```bash
python standardize_crossref_dates.py
```
2. `merge_publication_dates.py` – Merge online & issue publication dates  

```bash
python merge_publication_dates.py
```
3. `calculate_date_difference.py` – Calculate:
   - `custom_biorxivVersion_dateDifference` (first vs last submission)  
   - `custom_submission&publication_dateDiff` (submission vs publication)  

```bash
python calculate_date_difference.py
```


## Citation

- BioRxiv API: ` https://api.biorxiv.org/details/[server]/[interval]/[cursor]/[format] `
- Crossref API: ` https://api.crossref.org/works/[doi] `
