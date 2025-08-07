import pandas as pd
import time
import uuid
import json
import subprocess
from pathlib import Path
from snowflake.snowpark import Session, DataFrame
from typing import List

# --- CONFIGURATION ---
conn_params = {
    "account": "ctb38038",
    "user": "dbt",
    "password": "dbtPassword123",
    "role": "transform",
    "warehouse": "COMPUTE_WH",
    "database": "AIRBNB",
    "schema": "DEV"
}

test_results_table = "test_results"
dbt_model = "dim_listing_cleansed"
sleep_seconds = 15


# --- DBT TEST EXECUTION ---
def run_dbt_tests() -> Path:
    print(f"Running dbt tests on: {dbt_model}...")
    result = subprocess.run(
        ["dbt", "test", "--store-failures", "--select", dbt_model],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"WARNING: dbt test run failed with return code {result.returncode}")
    return Path("target/run_results.json")


# --- LOAD TEST RESULTS TO SNOWFLAKE ---
def load_test_results_to_snowflake(json_path: Path, batch_id: str, session: Session) -> None:
    with open(json_path) as f:
        test_data = json.load(f)

    results: List[dict] = []
    timestamp = pd.Timestamp.now()

    for result in test_data.get("results", []):
        results.append({
            "test_name": result.get("unique_id", "unknown"),
            "test_result": result.get("status", "unknown"),
            "test_timestamp": timestamp,
            "batch_id": batch_id
        })

    if results:
        df = pd.DataFrame(results)
        snow_df: DataFrame = session.create_dataframe(df)
        snow_df.write.mode("append").save_as_table(test_results_table)
        print(f"Logged {len(df)} test results for batch {batch_id}")
    else:
        print("No test results found to log.")


# --- MAIN LOOP ---
def main_loop():
    session = Session.builder.configs(conn_params).create()
    try:
        batch_id = str(uuid.uuid4())
        print(f"\nStarting new batch: {batch_id}")

        json_path = run_dbt_tests()
        load_test_results_to_snowflake(json_path, batch_id, session)

        time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        print("Stream stopped by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        session.close()
        print("Snowflake session closed.")



if __name__ == "__main__":
    main_loop()
