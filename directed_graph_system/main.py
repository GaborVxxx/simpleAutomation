#!/usr/bin/env python3
"""
Directed Graph System Orchestrator with Resource & Deadline Tracking:
- Reads dependency graph and optional settings from config.json under directed_graph_system/
- Validates global deadline and system resources before launching nodes
- Executes node scripts from process_files/ asynchronously via subprocess.Popen
- Monitors per-node timeouts, kills prolonged tasks
- Tracks completion, updates dependencies, and spawns dependents when ready
- Uses lock file (main.lock) with stale-lock detection
- Logs to main.log, error.log, and benchmarks.log under directed_graph_system/
"""
import os
import sys
import json
import logging
import atexit
import subprocess
import datetime
import time
import shutil
from pathlib import Path
from collections import deque

# External dependency for resource monitoring
try:
    import psutil
except ImportError:
    logging.basicConfig(level=logging.ERROR)
    logging.error("The 'psutil' module is required for resource monitoring. Please install it via 'pip install psutil'.")
    sys.exit(1)

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
    logger.addHandler(logging.FileHandler(MAIN_LOG, mode='a'))
    # console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # error.log
    err = logging.FileHandler(ERROR_LOG, mode='a')
    err.setLevel(logging.ERROR)
    err.setFormatter(fmt)
    logger.addHandler(err)

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

# ─── Configuration Loading ────────────────────────────────────────────────
def load_config():
    """Load configuration JSON with optional deadlines and resource settings."""
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
        logging.info(f"Loaded configuration from {CONFIG_FILE}.")
        return cfg
    except Exception as e:
        logging.error(f"Error loading config.json: {e}")
        sys.exit(1)

def load_graph():
    """Extract and return only the 'nodes' mapping from config."""
    return load_config().get('nodes', {})

# ─── Resource & Deadline Helpers ─────────────────────────────────────────
def wait_for_resources(res_cfg, poll_interval=5, timeout=None):
    """Blocks until system resources meet thresholds in res_cfg dict or raises TimeoutError."""
    last_log    = 0.0
    log_interval= max(poll_interval, 1)
    start       = time.time()

    while True:
        # Timeout enforcement for resource wait
        if timeout is not None and (time.time() - start) > timeout:
            raise TimeoutError(f"Resources not available within {timeout}s")

        ok = True
        # CPU utilization check
        cpu_pct = res_cfg.get('cpu_percent')
        if cpu_pct is not None and psutil.cpu_percent(interval=0.5) > cpu_pct:
            ok = False
        # Memory utilization check
        mem_pct = res_cfg.get('memory_percent')
        if ok and mem_pct is not None and psutil.virtual_memory().percent > mem_pct:
            ok = False
        # Disk free space check (cross-platform)
        disk_free = res_cfg.get('disk_free_mb')
        if ok and disk_free is not None:
            free_mb = shutil.disk_usage(PROCESS_DIR).free / (1024 * 1024)
            if free_mb < disk_free:
                ok = False
        # Load average check (Unix-only)
        load1_cfg = res_cfg.get('load_avg_1m')
        if ok and load1_cfg is not None:
            try:
                load1 = psutil.getloadavg()[0]
            except (AttributeError, OSError):
                load1 = None
            if load1 is not None and load1 > load1_cfg:
                ok = False
        if ok:
            return
        # Throttle log to once per interval
        now = time.time()
        if now - last_log >= log_interval:
            logging.info("Resources busy—waiting for availability...")
            last_log = now
        time.sleep(poll_interval)

# ─── Node Execution ────────────────────────────────────────────────────────
def launch_node(node, bench):
    """Start a node script and return process handle and start time."""
    path = PROCESS_DIR / node
    start= datetime.datetime.now()
    bench.info(f"{node} started at {start}")
    try:
        proc = subprocess.Popen(
            [sys.executable, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return proc, start
    except Exception as e:
        logging.error(f"Failed to start {node}: {e}")
        return None, None

# ─── Orchestration ────────────────────────────────────────────────────────
def main():
    setup_logging()
    bench     = setup_benchmark()
    acquire_lock()

    cfg       = load_config()
    deadline  = cfg.get('deadline_seconds')
    res_cfg   = cfg.get('resources', {})
    graph     = cfg.get('nodes', {})

    if deadline is not None and deadline <= 0:
        logging.error(
            "Global deadline of %s seconds expired before start; aborting run.",
            deadline
        )
        sys.exit(1)

    # Build in-degree and adjacency
    in_degree = {n: len(d.get('in', [])) for n, d in graph.items()}
    adj       = {n: [] for n in graph}
    timeouts  = {n: d.get('timeout') for n, d in graph.items()}
    for n, d in graph.items():
        for pr in d.get('in', []):
            if pr not in adj:
                logging.error(f"Config error: prereq '{pr}' not defined.")
                sys.exit(1)
            adj[pr].append(n)

    start_time= time.time()
    queue     = deque([n for n, deg in in_degree.items() if deg == 0])
    running   = {}
    completed = set()

    logging.info("Starting directed graph execution")
    bench.info(SEPARATOR)

    while queue or running:
        # Global deadline enforcement
        if deadline is not None and (time.time() - start_time) > deadline:
            logging.error("Global deadline exceeded; aborting run.")
            sys.exit(1)

        # Launch ready nodes with resource check
        while queue:
            node      = queue.popleft()
            if res_cfg:
                # calculate remaining time for resource wait
                remaining = None
                if deadline is not None:
                    elapsed   = time.time() - start_time
                    remaining = max(deadline - elapsed, 0)
                try:
                    wait_for_resources(res_cfg, poll_interval=5, timeout=remaining)
                except TimeoutError as e:
                    logging.error(f"Resource wait timeout: {e}")
                    sys.exit(1)
            proc, start= launch_node(node, bench)
            if not proc:
                logging.error(f"Error launching node {node}")
                sys.exit(1)
            running[node] = (proc, start)

        # Poll running processes
        time.sleep(0.5)
        now = time.time()
        for node, (proc, start_dt) in list(running.items()):
            timeout = timeouts.get(node)
            elapsed = now - start_dt.timestamp()
            if timeout is not None and elapsed > timeout:
                proc.kill()
                logging.error(f"Node {node} timed out after {timeout} seconds.")
                sys.exit(1)
            ret = proc.poll()
            if ret is not None:
                stdout, stderr = proc.communicate()
                end = datetime.datetime.now()
                bench.info(f"{node} ended at {end}, duration {end - start_dt}")
                if ret == 0:
                    logging.info(f"{node} output:\n{stdout}")
                    completed.add(node)
                    for child in adj[node]:
                        in_degree[child] -= 1
                        if in_degree[child] == 0:
                            queue.append(child)
                else:
                    logging.error(f"{node} failed (code {ret}):\n{stderr}")
                    sys.exit(1)
                del running[node]

    # Final check for cycles
    if len(completed) != len(graph):
        logging.error("Cycle detected or missing dependency; aborting.")
        sys.exit(1)

    logging.info("All nodes completed successfully.")
    bench.info(SEPARATOR)
    release_lock()

if __name__ == '__main__':
    main()
