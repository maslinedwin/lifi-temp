import requests
import pandas as pd
from datetime import datetime
import os
import time
import concurrent.futures
import logging
from database import LiFiDatabase

# --- CONFIGURATION ---
OUTPUT_FILENAME = "txns_2023_to_2025.xlsx"
RESUME_FILE = "resume_cursor.txt"
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 9, 30, 23, 59, 59)
USD_THRESHOLD = 0
SOURCE_TOKEN_FILTER = "BTC"

# Database instance
db = LiFiDatabase()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_single_page(url):
    """Fetch a single page of transactions from the API."""
    response = None
    for attempt in range(5):  # Retry up to 5 times
        response = requests.get(url)
        if response.status_code == 200:
            break  # Success
        elif 500 <= response.status_code < 600:
            logger.warning(f"API returned a server error ({response.status_code}). Retrying in 10 seconds... (Attempt {attempt + 1}/5)")
            time.sleep(10)
        else:
            break  # Don't retry for non-server errors (e.g., 4xx)

    response.raise_for_status()  # Raise an exception for the last failed attempt or non-retryable errors
    data = response.json()
    return data, data.get("next")

def fetch_and_process_data_db(progress_callback=None):
    """
    Fetches, filters, and saves transaction data to database with resume capability.

    Args:
        progress_callback: Optional function to call with progress updates
    """

    def update_progress(message, current=0, total=0):
        if progress_callback:
            progress_callback(message, current, total)
        logger.info(f"Progress: {message} ({current}/{total})")

    update_progress("Initializing database connection")

    # --- Resume Logic ---
    next_page_url = "https://li.quest/v2/analytics/transfers"
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, 'r') as f:
            resume_cursor = f.read().strip()
            if resume_cursor:
                next_page_url = f"https://li.quest/v2/analytics/transfers?next={resume_cursor}"
                logger.info(f"Resuming from saved cursor: {resume_cursor}")

    start_date_timestamp = START_DATE.timestamp()
    end_date_timestamp = END_DATE.timestamp()

    update_progress(f"Fetching transactions from {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")

    saved_records_count = 0
    total_processed = 0

    # --- Main Fetching Loop (Concurrent) ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        current_page_url = next_page_url

        # Submit initial tasks
        for _ in range(5):  # Start with 5 concurrent fetches
            if current_page_url:
                future = executor.submit(fetch_single_page, current_page_url)
                futures[future] = current_page_url  # Store URL for context
                current_page_url = None  # Prevent re-submitting the same URL immediately

        while futures:
            try:
                for future in concurrent.futures.as_completed(futures):
                    original_url = futures.pop(future)  # Get URL for context
                    data, next_cursor = future.result()
                    transfers = data.get("data", [])

                    if not transfers:
                        update_progress("No more transfers found")
                        break  # Break from inner loop, will eventually exit outer while

                    total_processed += len(transfers)

                    first_tx_time = datetime.fromtimestamp(transfers[0].get("sending", {}).get("timestamp", 0))
                    last_tx_time = datetime.fromtimestamp(transfers[-1].get("sending", {}).get("timestamp", 0))

                    update_progress(f"Processing batch of {len(transfers)} transfers ({first_tx_time} to {last_tx_time})",
                                  total_processed, total_processed + 100)

                    # Filter transactions based on criteria
                    filtered_transactions = []
                    for tx in transfers:
                        sending_info = tx.get("sending", {})
                        timestamp = sending_info.get("timestamp")

                        # Apply filters
                        if timestamp and start_date_timestamp <= timestamp <= end_date_timestamp:
                            # If SOURCE_TOKEN_FILTER is specified, apply it
                            if SOURCE_TOKEN_FILTER and SOURCE_TOKEN_FILTER != "ALL":
                                if sending_info.get("token", {}).get("symbol", "").upper() == SOURCE_TOKEN_FILTER.upper():
                                    filtered_transactions.append(tx)
                            else:
                                # No token filter, include all transactions in date range
                                filtered_transactions.append(tx)

                    # Insert filtered transactions into database
                    if filtered_transactions:
                        inserted_count = db.bulk_insert_transactions(filtered_transactions)
                        saved_records_count += inserted_count
                        update_progress(f"Saved {inserted_count} new transactions (Total: {saved_records_count})")
                    else:
                        update_progress("No transactions matched filters in this batch")

                    # Save resume cursor
                    if next_cursor:
                        with open(RESUME_FILE, 'w') as f:
                            f.write(next_cursor)

                    # --- Stop if we are past the date range ---
                    last_tx_timestamp = transfers[-1].get("sending", {}).get("timestamp")
                    if last_tx_timestamp and last_tx_timestamp < start_date_timestamp:
                        update_progress("Reached the beginning of the desired date range. Stopping fetch.")
                        break  # Break from inner loop, will eventually exit outer while

                    # Submit next task if available
                    if next_cursor:
                        next_page_url = f"https://li.quest/v2/analytics/transfers?next={next_cursor}"
                        future = executor.submit(fetch_single_page, next_page_url)
                        futures[future] = next_page_url
                    else:
                        update_progress("Reached the end of all transaction history.")
                        if os.path.exists(RESUME_FILE):
                            os.remove(RESUME_FILE)  # Clean up resume file on successful completion
                        break  # Break from inner loop, will eventually exit outer while

            except requests.exceptions.RequestException as e:
                error_msg = f"A network error occurred: {e}. Please wait and run the script again to resume."
                update_progress(error_msg)
                logger.error(error_msg)
                break  # Stop execution, the resume file has the last good cursor
            except Exception as e:
                error_msg = f"An unexpected error occurred: {e}"
                update_progress(error_msg)
                logger.error(error_msg)
                break

    update_progress(f"Database update complete! Saved {saved_records_count} transactions.")

    # Also create Excel export for compatibility
    try:
        update_progress("Creating Excel export for compatibility")
        filters = {}
        if SOURCE_TOKEN_FILTER and SOURCE_TOKEN_FILTER != "ALL":
            filters['token_symbol'] = SOURCE_TOKEN_FILTER

        excel_file = db.export_to_excel(OUTPUT_FILENAME, filters)
        update_progress(f"Excel file created: {excel_file}")

    except Exception as e:
        logger.warning(f"Could not create Excel export: {e}")

    return saved_records_count

def fetch_and_process_data():
    """Legacy function for backward compatibility."""
    return fetch_and_process_data_db()

if __name__ == "__main__":
    print("🚀 Starting LiFi transaction fetching with database storage...")

    def progress_callback(message, current=0, total=0):
        print(f"📊 {message}")

    try:
        count = fetch_and_process_data_db(progress_callback)
        print(f"✅ Successfully processed {count} transactions!")

        # Show database statistics
        stats = db.get_statistics()
        print(f"📈 Database now contains {stats['total_transactions']} total transactions")
        print(f"💰 Total volume: ${stats['total_volume_usd']:,.2f}")

    except KeyboardInterrupt:
        print("❌ Process interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"Fatal error: {e}")