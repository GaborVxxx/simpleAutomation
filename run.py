# run.py - launcher for selecting and invoking the correct mainX.py
#!/usr/bin/env python3

""" We tell the system what process to run AKA what main.py to run """

""" 
Run it like:
./run.py
OR
python run.py
"""

import json
import subprocess
import sys
from pathlib import Path

# 1. Locate the run configuration
config_path = Path(__file__).parent / "run.json"
if not config_path.exists():
    print(f"Error: run.json not found at {config_path}", file=sys.stderr)
    sys.exit(1)

# 2. Load configuration
try:
    with config_path.open(encoding='utf-8') as f:
        cfg = json.load(f)
except json.JSONDecodeError as e:
    print(f"Error parsing run.json: {e}", file=sys.stderr)
    sys.exit(1)

# 3. Determine entry point
entry = cfg.get("entry_point")
if not entry:
    print("Error: 'entry_point' not specified in run.json", file=sys.stderr)
    sys.exit(1)

entry_path = Path(__file__).parent / entry
if not entry_path.exists():
    print(f"Error: entry_point '{entry}' not found at {entry_path}", file=sys.stderr)
    sys.exit(1)

# 4. Invoke the selected main script
#    Pass through any CLI arguments
result = subprocess.run([sys.executable, str(entry_path)] + sys.argv[1:])
sys.exit(result.returncode)