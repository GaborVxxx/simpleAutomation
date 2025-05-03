#!/usr/bin/env python3
"""
Directed Graph System Orchestrator:
- Reads dependency graph from config.json under directed_graph_system/
- Executes scripts from process_files/ as nodes, respecting 'in' dependencies
- Launches ready nodes asynchronously via subprocess.Popen
- Tracks completion, updates dependencies, and spawns dependents when ready
- Uses lock file (main.lock) with stale-lock detection
- Writes logs to main.log, error.log, and benchmarks.log under directed_graph_system/
"""
import os
import sys
import json
import logging
import atexit
import subprocess
import datetime
import time
from pathlib import Path
from collections import deque

# Track if this process successfully acquired the lock
HAS_LOCK = False

# Directories
BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
PROCESS_DIR  = PROJECT_ROOT / 'process_files'

# Path constants
LOCK_FILE   = BASE_DIR / 'main.lock'
CONFIG_FILE = BASE_DIR / 'config.json'
MAIN_LOG    = BASE_DIR / 'main.log'
ERROR_LOG   = BASE_DIR / 'error.log'
BENCH_LOG   = BASE_DIR / 'benchmarks.log'
SEPARATOR   = '-' * 30

# ─── Logging ──────────────────────────────────────────────────────────────
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    # main.log
    fh_main = logging.FileHandler(MAIN_LOG, mode='a')
    fh_main.setFormatter(fmt)
    logger.addHandler(fh_main)
    # console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # error.log
    fh_err = logging.FileHandler(ERROR_LOG, mode='a')
    fh_err.setLevel(logging.ERROR)
    fh_err.setFormatter(fmt)
    logger.addHandler(fh_err)

# ─── Benchmark Logging ────────────────────────────────────────────────────
def setup_benchmark():
    bench = logging.getLogger('benchmark')
    bench.setLevel(logging.INFO)
    fh = logging.FileHandler(BENCH_LOG, mode='a')
    fh.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
    bench.addHandler(fh)
    return bench

# ─── Locking ──────────────────────────────────────────────────────────────
def acquire_lock():
    global HAS_LOCK
    if LOCK_FILE.exists():
        pid_text = LOCK_FILE.read_text().strip()
        try:
            pid = int(pid_text)
        except ValueError:
            logging.warning(f"Invalid lock PID '{pid_text}'; removing stale lock.")
            LOCK_FILE.unlink()
        else:
            try:
                os.kill(pid, 0)
            except OSError:
                logging.warning(f"Stale lock for PID {pid}; removing.")
                LOCK_FILE.unlink()
            else:
                logging.error(f"Lock held by active process {pid}. Exiting.")
                sys.exit(1)
    try:
        LOCK_FILE.write_text(str(os.getpid()))
        logging.info(f"Acquired lock (PID {os.getpid()}).")
        HAS_LOCK = True
    except Exception as e:
        logging.error(f"Failed to create lock file: {e}")
        sys.exit(1)

def release_lock():
    """Remove the lock file if this process holds it."""
    global HAS_LOCK
    if not HAS_LOCK:
        return
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
            logging.info("Released lock and removed lock file.")
            HAS_LOCK = False
    except Exception as e:
        logging.error(f"Failed to remove lock file: {e}")

atexit.register(release_lock)

# ─── Graph Loading ────────────────────────────────────────────────────────
def load_graph():
    """Load the 'nodes' mapping from directed_graph_system/config.json."""
    try:
        data = json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
        return data['nodes']
    except Exception as e:
        logging.error(f"Error loading config.json: {e}")
        sys.exit(1)

# ─── Node Execution ────────────────────────────────────────────────────────
def launch_node(node, bench):
    """Start a node script and return (process, start_time) or (None, None) on failure."""
    path = PROCESS_DIR / node
    start = datetime.datetime.now()
    bench.info(f"{node} started at {start}")
    try:
        proc = subprocess.Popen([
            sys.executable, str(path)
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return proc, start
    except Exception as e:
        logging.error(f"Failed to start {node}: {e}")
        return None, None

# ─── Orchestration ────────────────────────────────────────────────────────
def main():
    setup_logging()
    bench = setup_benchmark()
    acquire_lock()

    graph = load_graph()
    # Compute in-degree and adjacency from 'in' lists
    in_degree = {n: len(d['in']) for n, d in graph.items()}
    adj = {n: [] for n in graph}
    for n, d in graph.items():
        for prereq in d['in']:
            if prereq not in adj:
                logging.error(f"Config error: prereq '{prereq}' not defined.")
                sys.exit(1)
            adj[prereq].append(n)

    queue = deque([n for n, deg in in_degree.items() if deg == 0])
    running = {}
    completed = set()

    logging.info("Starting directed graph execution")
    bench.info(SEPARATOR)

    while queue or running:
        # Launch all ready nodes
        while queue:
            n = queue.popleft()
            proc, start = launch_node(n, bench)
            if not proc:
                logging.error(f"Error launching node {n}")
                sys.exit(1)
            running[n] = (proc, start)

        # Poll running processes
        time.sleep(0.5)
        for n, (proc, start) in list(running.items()):
            ret = proc.poll()
            if ret is not None:
                stdout, stderr = proc.communicate()
                end = datetime.datetime.now()
                bench.info(f"{n} ended at {end}, duration {end - start}")
                if ret == 0:
                    logging.info(f"{n} output:\n{stdout}")
                    completed.add(n)
                    for child in adj[n]:
                        in_degree[child] -= 1
                        if in_degree[child] == 0:
                            queue.append(child)
                else:
                    logging.error(f"{n} failed (code {ret}):\n{stderr}")
                    sys.exit(1)
                del running[n]

    # Final check
    if len(completed) != len(graph):
        logging.error("Cycle detected or missing dependency; aborting.")
        sys.exit(1)

    logging.info("All nodes completed successfully.")
    bench.info(SEPARATOR)
    release_lock()

if __name__ == '__main__':
    main()
