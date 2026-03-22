import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
import requests
import io

#this model accounts for the wholesale electricity price in NZ
#you need to download chromedriver to run this code.
#good working file

# Configuration
BASE_URL = "https://www.emi.ea.govt.nz/Wholesale/Download/DataReport/CSV/W_GD_C?"
CHROME_DRIVER_PATH = r""  # Update this path
OUTPUT_CSV = "grid_demand_aggregated.csv"

# Date range for iterative runs
START_DATE = datetime(2023, 1, 1)  # Start date (YYYY, MM, DD)
END_DATE = datetime(2023, 2, 28)  # End date (YYYY, MM, DD)

# Function to generate date ranges in chunks (e.g., monthly)
def generate_date_ranges(start_date, end_date, chunk_size="monthly"):
    current_date = start_date
    while current_date <= end_date:
        if chunk_size == "monthly":
            # Calculate the last day of the current month
            next_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        elif chunk_size == "yearly":
            # Calculate the last day of the current year
            next_date = current_date.replace(year=current_date.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            raise ValueError("Invalid chunk_size. Use 'monthly' or 'yearly'.")
        
        next_date = min(next_date, end_date)  # Ensure we don't exceed END_DATE
        yield current_date, next_date
        current_date = next_date + timedelta(days=1)  # Move to next chunk

# Configure Chrome options to prevent downloads
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "download_restrictions": 3  # Prevent all automatic downloads
})


# Initialize Selenium WebDriver
service = Service(CHROME_DRIVER_PATH)
driver = webdriver.Chrome(service=service)

# List to store all data
all_data = []

try:
    for date_from, date_to in generate_date_ranges(START_DATE, END_DATE, chunk_size="monthly"):
        # Format dates as YYYYMMDD
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")

        # Construct the URL
        URL = f"{BASE_URL}DateFrom={date_from_str}&DateTo={date_to_str}&RegionType=POC&TimeScale=TP&_rsdr=M1&_si=v|4"
        # URL = f"{BASE_URL}DateFrom={date_from_str}&DateTo={date_to_str}&RegionType=POC&_rsdr=W1&_si=v|4"
        #https://www.emi.ea.govt.nz/Wholesale/Download/DataReport/CSV/W_GD_C?DateFrom=20250301&DateTo=20250331&RegionType=POC&_rsdr=M1&_si=v|4
        print(URL)
        print(f"Fetching data from {date_from_str} to {date_to_str}...")

                
        
        # Open the website
        driver.get(URL)
        time.sleep(5)  # Wait for the page to load

        # Read CSV file from the generated URL
        try:
            df = pd.read_csv(URL, skiprows=11, header=0)  # Skip first 11 rows, set header at row 12
            all_data.append(df)  # Append the DataFrame to list
            print(f"Data from {date_from_str} to {date_to_str} fetched successfully.")

        except Exception as e:
            print(f"Failed to fetch data for {date_from_str} to {date_to_str}: {e}")

finally:
    # Close the browser
    driver.quit()

# Concatenate all data into a single DataFrame
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Aggregated CSV saved as {OUTPUT_CSV}.")
else:
    print("No data collected.")

