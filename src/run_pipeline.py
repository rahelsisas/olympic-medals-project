import subprocess
import sys

def run_step(script_path, step_name):
    print(f"\nRunning: {step_name}")

    subprocess.run([sys.executable, script_path], check=True)

    print(f"Completed: {step_name}")


if __name__ == "__main__":
    print("Starting full data pipeline...\n")

    run_step("extract/world_bank_api.py", "World Bank Data Extraction")
    run_step("transform/clean_olympics.py", "Olympic Data Cleaning")
    run_step("transform/merge_datasets.py", "Dataset Merging & Feature Engineering")
    run_step("transform/spark_processing.py", "Spark Processing")

    print("\n Pipeline completed successfully!")