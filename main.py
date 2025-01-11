import os
import csv
import time
import logging
import requests
import concurrent.futures

# ------------------- CONFIGURATION -------------------
YEAR = "2024-2025"
OUTPUT_CSV = "panchayat_data_with_members11.csv"
LOG_FILENAME = "panchayat_scraper.log"

# Increase or decrease for more/less concurrency
MAX_WORKERS = 50

# API Endpoints
URL_STATES = f"https://egramswaraj.gov.in/ElectedStateReport.do?year={YEAR}"
URL_DISTRICTS = "https://egramswaraj.gov.in/ElectedZpReport.do?stateId=29&year=2024-2025"
URL_BLOCKS = "https://egramswaraj.gov.in/ElectedBpReport.do?stateId={state_id}&code={district_code}&year={year}"
URL_GRAMPANCHAYATS = "https://egramswaraj.gov.in/ElectedGpReport.do?stateId={state_id}&code={block_code}&year={year}"
URL_MEMBERS = "https://egramswaraj.gov.in/ElectedMemberRepresent.do?code={gp_code}"

# ------------------- LOGGING SETUP -------------------
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join("logs", LOG_FILENAME), mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ------------------- HELPER FUNCTIONS ----------------
def fetch_states():
    """Fetch all states from the egramswaraj endpoint."""
    logging.info("Fetching all states from %s", URL_STATES)
    resp = requests.get(URL_STATES)
    resp.raise_for_status()
    states = resp.json()
    logging.info("Fetched %d states.", len(states))
    return states

def fetch_districts(state):
    """Fetch all districts for a given state."""
    state_id = state["code"]
    url = URL_DISTRICTS.format(state_id=state_id, year=YEAR)
    resp = requests.get(url)
    resp.raise_for_status()
    districts = resp.json()
    logging.info("Fetched %d districts for state %s (code=%s).",
                 len(districts), state["name"], state_id)
    return districts

def fetch_blocks(state_id, dist):
    """Fetch all blocks for a given district."""
    dist_code = dist["code"]
    url = URL_BLOCKS.format(state_id=state_id, district_code=dist_code, year=YEAR)
    resp = requests.get(url)
    resp.raise_for_status()
    blocks = resp.json()
    logging.info("Fetched %d blocks for district %s (code=%s).",
                 len(blocks), dist["name"], dist_code)
    return blocks

def fetch_gram_panchayats(state_id, block):
    """Fetch all Gram Panchayats for a given block."""
    block_code = block["code"]
    url = URL_GRAMPANCHAYATS.format(state_id=state_id, block_code=block_code, year=YEAR)
    resp = requests.get(url)
    resp.raise_for_status()
    gps = resp.json()
    logging.info("Fetched %d GPs for block %s (code=%s).",
                 len(gps), block["name"], block_code)
    return gps

def fetch_members(gp):
    """Fetch all elected members for a given Gram Panchayat code."""
    gp_code = gp["code"]
    url = URL_MEMBERS.format(gp_code=gp_code)
    resp = requests.get(url)
    resp.raise_for_status()
    members = resp.json()
    logging.info("Fetched %d members for GP %s (code=%s).",
                 len(members), gp["name"], gp_code)
    return members

# ------------------- MAIN SCRIPT ---------------------
def main():
    start_time = time.time()
    logging.info("Starting Panchayat Scraper...")

    # Prepare CSV
    try:
        csv_file = open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8")
        writer = csv.writer(csv_file)
        writer.writerow([
            "StateCode", "StateName",
            "DistrictCode", "DistrictName",
            "BlockCode", "BlockName",
            "GramPanchayatCode", "GramPanchayatName",
            "ElectedId", "ElectedName",
            "MobileNumber", "EmailId", "DesignationName"
        ])
    except Exception as e:
        logging.error("Unable to open CSV file for writing: %s", e)
        return

    # 1. Fetch states
    try:
        states = fetch_states()
    except Exception as e:
        logging.error("Failed to fetch states: %s", e)
        return

    # 2. For each state → fetch districts concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as state_executor:
        future_to_state = {}
        for state in states:
            future = state_executor.submit(fetch_districts, state)
            future_to_state[future] = state

        # As each state's districts are fetched, expand
        for future in concurrent.futures.as_completed(future_to_state):
            state = future_to_state[future]
            state_id = state["code"]
            state_name = state["name"]
            try:
                districts = future.result()
            except Exception as e:
                logging.error("Failed to fetch districts for state %s (code=%s): %s", state_name, state_id, e)
                continue

            # 3. For each district → fetch blocks concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as dist_executor:
                future_to_dist = {}
                for dist in districts:
                    dist_future = dist_executor.submit(fetch_blocks, state_id, dist)
                    future_to_dist[dist_future] = dist

                for dist_future in concurrent.futures.as_completed(future_to_dist):
                    dist_obj = future_to_dist[dist_future]
                    dist_code = dist_obj["code"]
                    dist_name = dist_obj["name"]
                    try:
                        blocks = dist_future.result()
                    except Exception as e:
                        logging.error("Failed to fetch blocks for district %s (code=%s): %s", dist_name, dist_code, e)
                        continue

                    # 4. For each block → fetch GPs concurrently
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as block_executor:
                        future_to_block = {}
                        for block_ in blocks:
                            block_future = block_executor.submit(fetch_gram_panchayats, state_id, block_)
                            future_to_block[block_future] = block_

                        for block_future in concurrent.futures.as_completed(future_to_block):
                            block_obj = future_to_block[block_future]
                            block_code = block_obj["code"]
                            block_name = block_obj["name"]
                            try:
                                gps = block_future.result()
                            except Exception as e:
                                logging.error("Failed to fetch GPs for block %s (code=%s): %s", block_name, block_code, e)
                                continue

                            # 5. For each GP → fetch members concurrently
                            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as gp_executor:
                                future_to_gp = {}
                                for gp in gps:
                                    gp_future = gp_executor.submit(fetch_members, gp)
                                    future_to_gp[gp_future] = gp

                                for gp_future in concurrent.futures.as_completed(future_to_gp):
                                    gp_obj = future_to_gp[gp_future]
                                    gp_code = gp_obj["code"]
                                    gp_name = gp_obj["name"]
                                    try:
                                        members = gp_future.result()
                                    except Exception as e:
                                        logging.error("Failed to fetch members for GP %s (code=%s): %s", gp_name, gp_code, e)
                                        # Even if members fail, we might still log a blank row
                                        writer.writerow([
                                            state_id, state_name,
                                            dist_code, dist_name,
                                            block_code, block_name,
                                            gp_code, gp_name,
                                            "", "", "", "", ""
                                        ])
                                        continue

                                    if not members:
                                        # No members, log blank
                                        writer.writerow([
                                            state_id, state_name,
                                            dist_code, dist_name,
                                            block_code, block_name,
                                            gp_code, gp_name,
                                            "", "", "", "", ""
                                        ])
                                    else:
                                        # Write each member record
                                        for mem in members:
                                            writer.writerow([
                                                state_id, state_name,
                                                dist_code, dist_name,
                                                block_code, block_name,
                                                gp_code, gp_name,
                                                mem.get("electedid", ""),
                                                mem.get("electedname", ""),
                                                mem.get("mobno", ""),
                                                mem.get("emailid", ""),
                                                mem.get("designationname", "")
                                            ])

    csv_file.close()
    elapsed = time.time() - start_time
    logging.info("Scraping completed in %.2f seconds. CSV saved to '%s'.", elapsed, OUTPUT_CSV)

if __name__ == "__main__":
    main()
