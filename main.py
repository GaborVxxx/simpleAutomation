import os
import sys
import atexit
import subprocess
import logging

LOCK_FILE = 'main.lock'


def setup_logging():
    """Configure logging to output to both console and main.log file."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        filename='main.log',  # Log file name
        filemode='a'  # Append mode
    )
    # Add a console handler so logs also appear on the terminal.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logging.getLogger().addHandler(console_handler)


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


# Ensure the lock is released when the script exits.
atexit.register(release_lock)


def run_file(script):
    """Execute a script and return True if it ran successfully."""
    try:
        logging.info(f"Starting {script}")
        # Execute the script as a separate process
        result = subprocess.run(["python", script], check=True, capture_output=True, text=True)
        logging.info(f"Finished {script} with output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as error:
        logging.error(f"Error running {script}:\n{error.stderr}")
        return False


def run_batch(scripts):
    """Run a batch of scripts sequentially."""
    for script in scripts:
        if not run_file(script):
            logging.error("Batch halted due to an error.")
            return False
    return True


def main():
    setup_logging()
    # Try to acquire a lock. If a previous instance is running, this will exit.
    acquire_lock()

    # Define batches: each inner list is a batch containing one or more scripts.
    batches = [
        ["file1.py", "file2.py"],  # Batch 1
        ["file3.py"],  # Batch 2
        # Additional batches can be added here
    ]

    # Iterate over all defined batches sequentially.
    for i, batch in enumerate(batches, start=1):
        logging.info(f"Starting Batch {i}")
        if run_batch(batch):
            logging.info(f"Batch {i} completed successfully.")
        else:
            logging.error(f"Batch {i} failed. Aborting subsequent batches.")
            break


if __name__ == '__main__':
    main()
