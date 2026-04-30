import subprocess
import sys
import os

SRC_DIR = os.path.dirname(os.path.abspath(__file__))


def run_step(script_path, step_name):
    print(f"Running: {step_name}")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        print(f"Completed: {step_name}")
    except subprocess.CalledProcessError as e:
        print(f"\nPipeline failed at step: {step_name}")
        print(f"Script: {script_path}")
        print(f"Return code: {e.returncode}")
        raise


if __name__ == "__main__":
    print("Starting full data pipeline...\n")

    run_step(os.path.join(SRC_DIR, "extract", "world_bank_api.py"), "World Bank Data Extraction")
    run_step(os.path.join(SRC_DIR, "transform", "clean_olympics.py"), "Olympic Data Cleaning")
    run_step(os.path.join(SRC_DIR, "transform", "merge_datasets.py"), "Dataset Merging & Feature Engineering")
    run_step(os.path.join(SRC_DIR, "transform", "spark_processing.py"), "Spark Processing")

    print("\nPipeline completed successfully!")
