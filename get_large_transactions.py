import requests
import pandas as pd
from datetime import datetime
import os
import time
import concurrent.futures

# --- CONFIGURATION ---
OUTPUT_FILENAME = "txns_2023_to_2025.xlsx"
RESUME_FILE = "resume_cursor.txt"
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 9, 30, 23, 59, 59)
USD_THRESHOLD = 0
SOURCE_TOKEN_FILTER = "BTC"

# --- SCRIPT ---

def get_chain_name(chain_id):
    """Converts a chain ID to its name."""
    # (Chain map remains the same)
    chain_map = {
        1: "Ethereum", 10: "Optimism", 56: "BNB Smart Chain", 100: "Gnosis Chain",
        137: "Polygon", 250: "Fantom", 42161: "Arbitrum", 43114: "Avalanche",
        20000000000001: "btc",
    }
    return chain_map.get(chain_id, f"Unknown Chain (ID: {chain_id})")

def fetch_single_page(url):
    response = None
    for attempt in range(5): # Retry up to 5 times
        response = requests.get(url)
        if response.status_code == 200:
            break # Success
        elif 500 <= response.status_code < 600:
            print(f"API returned a server error ({response.status_code}). Retrying in 10 seconds... (Attempt {attempt + 1}/5)")
            time.sleep(10)
        else:
            break # Don't retry for non-server errors (e.g., 4xx)

    response.raise_for_status() # Raise an exception for the last failed attempt or non-retryable errors
    data = response.json()
    return data, data.get("next")

def fetch_and_process_data():
    """Fetches, filters, and saves transaction data with resume capability."""

    # --- Prepare Excel File ---
    column_order = [
        "Time", "Source Token", "Source Chain Name", "Source Wallet Address",
        "Destination Token", "Destination Chain Name", "Destination Wallet Address",
        "USD Value", "Integrator", "Bridge App Name"
    ]
    if not os.path.exists(OUTPUT_FILENAME):
        print(f"Creating new file '{OUTPUT_FILENAME}' with headers.")
        pd.DataFrame(columns=column_order).to_excel(OUTPUT_FILENAME, index=False)

    # --- Resume Logic ---
    next_page_url = "https://li.quest/v2/analytics/transfers"
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, 'r') as f:
            resume_cursor = f.read().strip()
            if resume_cursor:
                next_page_url = f"https://li.quest/v2/analytics/transfers?next={resume_cursor}"
                print(f"Resuming from saved cursor: {resume_cursor}")

    start_date_timestamp = START_DATE.timestamp()
    end_date_timestamp = END_DATE.timestamp()
    print(f"\nFetching transactions from {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}...")



    saved_records_count = 0
    # --- Main Fetching Loop (Concurrent) ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        current_page_url = "https://li.quest/v2/analytics/transfers"
        if os.path.exists(RESUME_FILE):
            with open(RESUME_FILE, 'r') as f:
                resume_cursor = f.read().strip()
                if resume_cursor:
                    current_page_url = f"https://li.quest/v2/analytics/transfers?next={resume_cursor}"
                    print(f"Resuming from saved cursor: {resume_cursor}")

        # Submit initial tasks
        for _ in range(5): # Start with 5 concurrent fetches
            if current_page_url:
                future = executor.submit(fetch_single_page, current_page_url)
                futures[future] = current_page_url # Store URL for context
                current_page_url = None # Prevent re-submitting the same URL immediately

        while futures:
            try:
                for future in concurrent.futures.as_completed(futures):
                    original_url = futures.pop(future) # Get URL for context
                    data, next_cursor = future.result()
                    transfers = data.get("data", [])

                    if not transfers:
                        print("No more transfers found.")
                        break # Break from inner loop, will eventually exit outer while

                    first_tx_time = datetime.fromtimestamp(transfers[0].get("sending", {}).get("timestamp", 0))
                    last_tx_time = datetime.fromtimestamp(transfers[-1].get("sending", {}).get("timestamp", 0))
                    print(f"Fetched a page of {len(transfers)} transfers. Timestamps from {first_tx_time} to {last_tx_time}. Filtering and saving...")

                    # Filter and append in real-time
                    new_records = []
                    for tx in transfers:
                        sending_info = tx.get("sending", {})
                        timestamp = sending_info.get("timestamp")
                        if timestamp and start_date_timestamp <= timestamp <= end_date_timestamp and sending_info.get("token", {}).get("symbol", "N/A").upper() == SOURCE_TOKEN_FILTER:
                            source_token_symbol = sending_info.get("token", {}).get("symbol", "N/A")
                            amount_usd_str = sending_info.get("amountUSD")
                            try:
                                amount_usd = float(amount_usd_str)
                            except (ValueError, TypeError):
                                amount_usd = 0.0

                            receiving_info = tx.get("receiving", {})
                            metadata = tx.get("metadata", {})
                            bridge_name = tx.get("tool", "Unknown")

                            new_records.append({
                                "Time": datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                                "Source Token": source_token_symbol,
                                "Source Chain Name": get_chain_name(sending_info.get("chainId")),
                                "Source Wallet Address": tx.get("fromAddress", "N/A"),
                                "Destination Token": receiving_info.get("token", {}).get("symbol", "N/A"),
                                "Destination Chain Name": get_chain_name(receiving_info.get("chainId")),
                                "Destination Wallet Address": tx.get("toAddress", "N/A"),
                                "USD Value": amount_usd,
                                "Integrator": metadata.get("integrator", "N/A"),
                                "Bridge App Name": bridge_name,
                            })

                    if new_records:
                        df_to_append = pd.DataFrame(new_records)
                        with pd.ExcelWriter(OUTPUT_FILENAME, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                            df_to_append.to_excel(writer, header=False, index=False, startrow=writer.sheets['Sheet1'].max_row)
                        print(f"Found and saved {len(new_records)} matching transactions.")

                    # --- Stop if we are past the date range ---
                    last_tx_timestamp = transfers[-1].get("sending", {}).get("timestamp")
                    if last_tx_timestamp and last_tx_timestamp < start_date_timestamp:
                        print("Reached the beginning of the desired date range. Stopping fetch.")
                        break # Break from inner loop, will eventually exit outer while

                    # Submit next task if available
                    if next_cursor:
                        next_page_url = f"https://li.quest/v2/analytics/transfers?next={next_cursor}"
                        future = executor.submit(fetch_single_page, next_page_url)
                        futures[future] = next_page_url
                    else:
                        print("Reached the end of all transaction history.")
                        if os.path.exists(RESUME_FILE):
                            os.remove(RESUME_FILE) # Clean up resume file on successful completion
                        break # Break from inner loop, will eventually exit outer while

            except requests.exceptions.RequestException as e:
                print(f"A network error occurred: {e}. Please wait and run the script again to resume.")
                break # Stop execution, the resume file has the last good cursor
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break

    print(f"\nScript finished.")

if __name__ == "__main__":
    fetch_and_process_data()