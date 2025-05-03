#!/usr/bin/env python3
"""
Refactored main.py for matrix_system:
- Original orchestrator logic now lives under matrix_system/
- Scripts to process are located in process_files/ next to matrix_system/
- All logs, lock, and config files live under matrix_system/
"""
import os
import sys
import atexit
import subprocess
import logging
import json
import datetime
from pathlib import Path

# Track if this process successfully acquired the lock
HAS_LOCK = False


# Directories
BASE_DIR = Path(__file__).parent             # matrix_system/
PROJECT_ROOT = BASE_DIR.parent              # repo root
PROCESS_DIR = PROJECT_ROOT / 'process_files' # process_files/ next to matrix_system/

# Path constants
LOCK_FILE = BASE_DIR / 'main.lock'
CONFIG_FILE = BASE_DIR / 'config.json'
SEPARATOR = '-' * 30  # separator for logs


def setup_logging():
    """Configure logging to console, main.log, and error.log."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    # Main log
    main_log = BASE_DIR / 'main.log'
    main_handler = logging.FileHandler(main_log, mode='a')
    main_handler.setFormatter(formatter)
    logger.addHandler(main_handler)

    # Console log
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Error log
    error_log = BASE_DIR / 'error.log'
    error_handler = logging.FileHandler(error_log, mode='a')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)


def setup_benchmark_logging():
    """Set up a dedicated benchmark logger writing to benchmarks.log."""
    benchmark_logger = logging.getLogger('benchmark')
    benchmark_logger.setLevel(logging.INFO)
    bench_log = BASE_DIR / 'benchmarks.log'
    bh = logging.FileHandler(bench_log, mode='a')
    bh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    bh.setFormatter(formatter)
    benchmark_logger.addHandler(bh)
    return benchmark_logger


def acquire_lock():
    """Acquire a lock by creating a lock file, checking for stale processes."""
    # If lock exists, check if PID inside is still running
    if LOCK_FILE.exists():
        try:
            existing_pid = int(LOCK_FILE.read_text())
            # os.kill(pid, 0) raises OSError if process does not exist or no permission
            os.kill(existing_pid, 0)
            logging.error(f"Lock file exists and process {existing_pid} is still running. Exiting.")
            sys.exit(1)
        except ValueError:
            logging.warning(f"Lock file {LOCK_FILE} contains invalid PID. Removing stale lock.")
            LOCK_FILE.unlink()
        except OSError:
            # Process not running; stale lock
            logging.warning(f"Stale lock detected for PID {existing_pid}. Removing lock file.")
            LOCK_FILE.unlink()
        except Exception as e:
            logging.error(f"Error checking existing lock: {e}")
            sys.exit(1)

    try:
        LOCK_FILE.write_text(str(os.getpid()))
        global HAS_LOCK
        HAS_LOCK = True
        logging.info(f"Acquired lock with file {LOCK_FILE} (PID: {os.getpid()}).")
    except Exception as e:
        logging.error(f"Failed to create lock file: {e}")
        sys.exit(1)

# Register release_lock to run on normal exit
def release_lock():
    """Remove the lock file if this process holds it."""
    global HAS_LOCK
    if not HAS_LOCK:
        # Skip removal: this process never acquired the lock
        return
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
            logging.info(f"Released lock and removed {LOCK_FILE}.")
    except Exception as e:
        logging.error(f"Failed to remove lock file: {e}")

# Register release_lock to run on normal exit
atexit.register(release_lock)


def load_config(config_file=None):
    """Load configuration JSON from matrix_system config.

    If config_file is provided (str or Path), load from that filename in matrix_system/;
    otherwise, load from default CONFIG_FILE (matrix_system/config.json)."""
    try:
        # Determine path to config
        if config_file:
            cfg_path = BASE_DIR / str(config_file)
        else:
            cfg_path = CONFIG_FILE
        data = cfg_path.read_text(encoding='utf-8')
        config = json.loads(data)
        logging.info(f"Loaded configuration from {cfg_path}.")
        return config
    except Exception as e:
        logging.error(f"Error reading configuration file {cfg_path}: {e}")
        sys.exit(1)

# Continue with existing run_file definition
def run_file(script_name, benchmark_logger):
    """Execute a single script in process_files/ synchronously with timing."""
    script_path = PROCESS_DIR / script_name
    start_time = datetime.datetime.now()
    benchmark_logger.info(f"Script {script_name} started at {start_time}")
    try:
        logging.info(f"Starting {script_path} synchronously")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"Finished {script_path} with output:\n{result.stdout}")
        success = True
    except subprocess.CalledProcessError as error:
        logging.error(f"Error running {script_path}:\n{error.stderr}")
        success = False
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    benchmark_logger.info(f"Script {script_name} ended at {end_time} with duration {duration}")
    return success


def run_batch_sync(scripts, benchmark_logger):
    """Run scripts one after another (sync)."""
    batch_start = datetime.datetime.now()
    benchmark_logger.info(f"Batch started at {batch_start}")
    for script in scripts:
        if not run_file(script, benchmark_logger):
            logging.error("Batch halted due to an error.")
            return False
    batch_end = datetime.datetime.now()
    benchmark_logger.info(f"Batch ended at {batch_end} with duration {batch_end - batch_start}")
    benchmark_logger.info(SEPARATOR)
    return True


def run_batch_async(scripts, benchmark_logger):
    """Run scripts concurrently (async) and wait for completion."""
    batch_start = datetime.datetime.now()
    benchmark_logger.info(f"Batch started at {batch_start}")
    processes = []
    for script in scripts:
        start_time = datetime.datetime.now()
        benchmark_logger.info(f"Script {script} started at {start_time}")
        try:
            proc = subprocess.Popen(
                [sys.executable, str(PROCESS_DIR / script)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            processes.append((script, start_time, proc))
        except Exception as e:
            logging.error(f"Failed to start {script}: {e}")
            return False
    for script, start_time, proc in processes:
        stdout, stderr = proc.communicate()
        end_time = datetime.datetime.now()
        benchmark_logger.info(f"Script {script} ended at {end_time} with duration {end_time - start_time}")
        if proc.returncode == 0:
            logging.info(f"Finished {script} with output:\n{stdout}")
        else:
            logging.error(f"Error running {script}:\n{stderr}")
            return False
    batch_end = datetime.datetime.now()
    benchmark_logger.info(f"Batch ended at {batch_end} with duration {batch_end - batch_start}")
    benchmark_logger.info(SEPARATOR)
    return True


def run_batch(scripts, execution_mode, benchmark_logger):
    """Dispatch batch execution based on mode."""
    if execution_mode.lower() == 'async':
        return run_batch_async(scripts, benchmark_logger)
    return run_batch_sync(scripts, benchmark_logger)


def main():
    setup_logging()
    benchmark_logger = setup_benchmark_logging()
    acquire_lock()
    config = load_config()

    execution_mode = config.get('execution_mode', 'sync')
    batches = config.get('batches', [])
    logging.info(f"Execution mode: {execution_mode}")

    process_start = datetime.datetime.now()
    benchmark_logger.info(f"Process started at {process_start} (Mode: {execution_mode.upper()})")
    benchmark_logger.info(SEPARATOR)

    for idx, batch in enumerate(batches, 1):
        logging.info(f"Starting Batch {idx}")
        benchmark_logger.info(f"Starting Batch {idx}")
        if not run_batch(batch, execution_mode, benchmark_logger):
            logging.error(f"Batch {idx} failed. Aborting.")
            break
    process_end = datetime.datetime.now()
    benchmark_logger.info(f"Process ended at {process_end} with duration {process_end - process_start}")
    benchmark_logger.info(SEPARATOR)


if __name__ == '__main__':
    main()
