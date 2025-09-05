import requests
import pandas as pd

# parameters
BASE_URL = "https://api.biorxiv.org/details/biorxiv/2016-01-01/2018-12-31/"
START_CURSOR = 0    # start point of request
MAX_CURSOR = 51166  # limit for the request
STEP = 100          # each query returns 100 results.
OUT_FILE = "data/downloadedBiorxiv.csv"

# sends a GET request to the given URL and returns a JSON response.
def fetch_data(url: str):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None


def main():
    # initial request
    initial_response = fetch_data(BASE_URL + "0/json")
    if not initial_response:
        print("initial api call failed.")
        return

    print("successfully loaded.")

    try:
        initial_data = initial_response['messages'][0]
        count_new_papers = initial_data.get("count_new_papers")

        if count_new_papers is None:
            raise KeyError("count_new_papers not found")

        print(f"Total preprints: {count_new_papers}")

        # list that keeps all the info
        all_data = []

        # navigate all pages with the cursor
        for cursor in range(START_CURSOR, MAX_CURSOR, STEP):
            url = f"{BASE_URL}{cursor}/json"
            data = fetch_data(url)
            if data and 'collection' in data:
                all_data.extend(data['collection'])

        # convert to dataframe
        df = pd.DataFrame(all_data)

        # write in csv
        df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    except KeyError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()