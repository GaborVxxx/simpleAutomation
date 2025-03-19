import os
import sys
import atexit
import subprocess
import logging
import json
import datetime

LOCK_FILE = 'main.lock'
CONFIG_FILE = 'config.json'
SEPARATOR = "-" * 30  # Adjust the separator length as needed.


def setup_logging():
    """Configure logging to output to console, main.log, and error.log for errors."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    # File handler for main log.
    main_handler = logging.FileHandler('main.log', mode='a')
    main_handler.setFormatter(formatter)
    logger.addHandler(main_handler)

    # Console handler.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Error file handler (only errors and above).
    error_handler = logging.FileHandler('error.log', mode='a')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)


def setup_benchmark_logging():
    """Set up a dedicated logger for benchmarking that writes to benchmarks.log."""
    benchmark_logger = logging.getLogger("benchmark")
    benchmark_logger.setLevel(logging.INFO)
    bh = logging.FileHandler("benchmarks.log", mode="a")
    bh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    bh.setFormatter(formatter)
    benchmark_logger.addHandler(bh)
    return benchmark_logger


def acquire_lock():
    """Acquire a lock by creating a lock file. Exit if the file exists."""
    if os.path.exists(LOCK_FILE):
        logging.error("Lock file exists. Another instance might be running. Exiting this run.")
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


def run_file(script, benchmark_logger):
    """Execute a script synchronously while tracking its execution time."""
    start_time = datetime.datetime.now()
    benchmark_logger.info(f"Script {script} started at {start_time}")
    try:
        logging.info(f"Starting {script} synchronously")
        result = subprocess.run(["python", script], check=True, capture_output=True, text=True)
        logging.info(f"Finished {script} with output:\n{result.stdout}")
        success = True
    except subprocess.CalledProcessError as error:
        logging.error(f"Error running {script}:\n{error.stderr}")
        success = False
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    benchmark_logger.info(f"Script {script} ended at {end_time} with duration {duration}")
    return success


def run_batch_sync(scripts, benchmark_logger):
    """Run scripts in the batch one after the other (synchronously)."""
    batch_start = datetime.datetime.now()
    benchmark_logger.info(f"Batch started at {batch_start}")

    for script in scripts:
        if not run_file(script, benchmark_logger):
            logging.error("Batch halted due to an error.")
            return False

    batch_end = datetime.datetime.now()
    duration = batch_end - batch_start
    benchmark_logger.info(f"Batch ended at {batch_end} with duration {duration}")
    benchmark_logger.info(SEPARATOR)
    return True


def run_batch_async(scripts, benchmark_logger):
    """Run scripts in the batch concurrently (asynchronously) and wait for all to finish."""
    batch_start = datetime.datetime.now()
    benchmark_logger.info(f"Batch started at {batch_start}")

    processes = []
    # Launch all scripts concurrently.
    for script in scripts:
        start_time = datetime.datetime.now()
        benchmark_logger.info(f"Script {script} started at {start_time}")
        try:
            proc = subprocess.Popen(["python", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append((script, start_time, proc))
        except Exception as e:
            logging.error(f"Failed to start {script}: {e}")
            return False

    # Wait for all processes to finish.
    for script, start_time, proc in processes:
        stdout, stderr = proc.communicate()
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        benchmark_logger.info(f"Script {script} ended at {end_time} with duration {duration}")
        if proc.returncode == 0:
            logging.info(f"Finished {script} with output:\n{stdout}")
        else:
            logging.error(f"Error running {script}:\n{stderr}")
            return False
    batch_end = datetime.datetime.now()
    duration = batch_end - batch_start
    benchmark_logger.info(f"Batch ended at {batch_end} with duration {duration}")
    benchmark_logger.info(SEPARATOR)
    return True


def run_batch(scripts, execution_mode, benchmark_logger):
    """Run a batch of scripts using the specified execution mode."""
    if execution_mode.lower() == "async":
        return run_batch_async(scripts, benchmark_logger)
    else:
        return run_batch_sync(scripts, benchmark_logger)


def main():
    setup_logging()
    benchmark_logger = setup_benchmark_logging()
    acquire_lock()
    config = load_config()

    execution_mode = config.get("execution_mode", "sync")
    batches = config.get("batches", [])
    logging.info(f"Execution mode for batches: {execution_mode}")

    # Log overall process start time and execution mode.
    process_start = datetime.datetime.now()
    benchmark_logger.info(f"Process started at {process_start} (Execution Mode: {execution_mode.upper()})")
    benchmark_logger.info(SEPARATOR)

    # Process each batch sequentially.
    for i, batch in enumerate(batches, start=1):
        logging.info(f"Starting Batch {i}")
        benchmark_logger.info(f"Starting Batch {i}")
        if run_batch(batch, execution_mode, benchmark_logger):
            logging.info(f"Batch {i} completed successfully.")
        else:
            logging.error(f"Batch {i} failed. Aborting subsequent batches.")
            break

    process_end = datetime.datetime.now()
    duration = process_end - process_start
    benchmark_logger.info(f"Process ended at {process_end} with total duration {duration}")
    benchmark_logger.info(SEPARATOR)


if __name__ == '__main__':
    main()
