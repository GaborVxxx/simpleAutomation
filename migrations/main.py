import json
import subprocess
from typing import List, Union
import sys
import logging
import datetime
import os
import atexit
import psutil  # pip install psutil

''' run the script from the directory with "python main.py" '''

LOCK_FILE = 'main.lock'
CONFIG_FILE = 'config.json'
SEPARATOR = "-" * 30  # Adjust as needed.

def setup_logging():
    """Configure logging to console, main.log, and error.log for errors."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    # Main log file handler.
    main_handler = logging.FileHandler('main.log', mode='a')
    main_handler.setFormatter(formatter)
    logger.addHandler(main_handler)

    # Console handler.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Error log file handler.
    error_handler = logging.FileHandler('error.log', mode='a')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

def acquire_lock():
    """Acquire a lock by creating a lock file. Exit if it already exists."""
    if os.path.exists(LOCK_FILE):
        logging.error("Lock file exists. Another instance might be running. Exiting.")
        sys.exit(1)
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        logging.info(f"Acquired lock with file {LOCK_FILE}.")
    except Exception as e:
        logging.error(f"Failed to create lock file: {e}")
        sys.exit(1)

def release_lock():
    """Remove the lock file if it exists."""
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
        logging.info(f"Released lock and removed {LOCK_FILE}.")

atexit.register(release_lock)

def load_config(config_file=CONFIG_FILE):
    """Load the configuration from a JSON file."""
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        logging.info(f"Loaded configuration from {config_file}.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file {config_file}: {e}")
        sys.exit(1)

def get_ids(filename: str) -> List[Union[int, str]]:
    """
    Call the get_ids.py file, which simulates an API call by sleeping
    for 4 seconds and then returning an array of IDs as a JSON string.
    """
    logging.info(f"Calling {filename} to retrieve IDs...")
    result = subprocess.run([sys.executable, filename],
                            capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Error retrieving IDs: {result.stderr}")
        return []
    try:
        ids = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing IDs JSON: {e}")
        return []
    logging.info(f"Successfully retrieved {len(ids)} IDs.")
    return ids

def get_memory_usage() -> float:
    """Return the current process memory usage in megabytes."""
    process = psutil.Process(os.getpid())
    mem_bytes = process.memory_info().rss
    return mem_bytes / (1024 * 1024) # 1024 * 1024 equals 1,048,576, which is the number of bytes in one megabyte

def get_total_system_memory() -> float:
    """Return total system memory in megabytes."""
    total_bytes = psutil.virtual_memory().total
    return total_bytes / (1024 * 1024)

def main():
    setup_logging()
    acquire_lock()
    logging.info(SEPARATOR)
    logging.info("Starting data migration process.")

    config = load_config()
    track_memory = config.get("track_memory", True)
    dynamic_batch = config.get("dynamic_batch_size_based_on_memory_usage", False)
    default_chunk_size = config.get("chunk_size", 10)
    chunk_size = default_chunk_size
    max_memory_usage_percent = config.get("max_memory_usage", 50)  # e.g. 50%

    if track_memory:
        overall_start_mem = get_memory_usage()
        total_system_mem = get_total_system_memory()
        logging.info(f"Initial memory usage: {overall_start_mem:.2f} MB")
        logging.info(f"Total system memory: {total_system_mem:.2f} MB")

    # Retrieve IDs from the external file.
    get_ids_file = config.get("get_ids")
    ids = get_ids(get_ids_file)
    total_ids = len(ids)
    logging.info(f"Total IDs to process: {total_ids}")
    logging.info(SEPARATOR)

    index = 0
    chunk_counter = 0
    # List to track the chunk size (batch size) used for each chunk.
    batch_sizes_history = []
    # Arrays to track memory details if enabled.
    memory_usage_deltas = []
    memory_usage_percentages = []

    while index < total_ids:
        chunk_counter += 1
        # Append the current chunk size to our history.
        batch_sizes_history.append(chunk_size)
        current_chunk = ids[index:index + chunk_size]
        logging.info(SEPARATOR)
        logging.info(f"Processing chunk {chunk_counter} with {len(current_chunk)} records.")
        chunk_start_time = datetime.datetime.now()

        mem_before = get_memory_usage() if track_memory else None
        if track_memory:
            logging.info(f"Memory before processing chunk: {mem_before:.2f} MB")

        # Convert chunk to JSON and process.
        chunk_arg = json.dumps(current_chunk)
        process_chunk_file = config.get("process_chunk")
        result = subprocess.run([sys.executable, process_chunk_file, chunk_arg],
                                capture_output=True, text=True)
        chunk_end_time = datetime.datetime.now()
        duration = (chunk_end_time - chunk_start_time).total_seconds()

        if track_memory:
            mem_after = get_memory_usage()
            delta = mem_after - mem_before
            memory_usage_deltas.append(delta)
            percentage = (delta / total_system_mem) * 100
            memory_usage_percentages.append(percentage)
            logging.info(f"Memory after processing chunk: {mem_after:.2f} MB")
            logging.info(f"Chunk {chunk_counter} processed in {duration:.2f} seconds with a memory delta of {delta:.2f} MB ({percentage:.4f}% of total system memory).")
        else:
            logging.info(f"Chunk {chunk_counter} processed in {duration:.2f} seconds.")

        if result.returncode != 0:
            logging.error(f"Error processing chunk {chunk_counter}: {result.stderr}")
            break
        else:
            logging.info(f"Chunk {chunk_counter} processed successfully.")
            logging.info(f"Output: {result.stdout.strip()}")
        logging.info(SEPARATOR)

        # Update index.
        index += len(current_chunk)

        # If dynamic batch sizing is enabled and memory is tracked, adjust chunk_size.
        if track_memory and dynamic_batch and len(current_chunk) > 0:
            avg_mem_per_record = delta / len(current_chunk) if delta > 0 else 0
            if avg_mem_per_record > 0:
                allowed_delta_mb = (max_memory_usage_percent / 100) * total_system_mem
                new_chunk_size = int(allowed_delta_mb / avg_mem_per_record)
                new_chunk_size = max(1, new_chunk_size)
                logging.info(f"Adjusting chunk size from {chunk_size} to {new_chunk_size} based on average memory usage per record ({avg_mem_per_record:.4f} MB).")
                chunk_size = new_chunk_size
            else:
                chunk_size = default_chunk_size

    if track_memory:
        overall_end_mem = get_memory_usage()
        logging.info(f"Final memory usage: {overall_end_mem:.2f} MB")
        logging.info(f"Memory usage delta per chunk (MB): {memory_usage_deltas}")
        logging.info(f"Memory usage percentage per chunk (%): {memory_usage_percentages}")

    logging.info(SEPARATOR)
    logging.info(f"Batch sizes used per chunk: {batch_sizes_history}")
    logging.info("Data migration process completed.")
    logging.info(SEPARATOR)

if __name__ == "__main__":
    main()
