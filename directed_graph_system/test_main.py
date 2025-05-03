import unittest
import os
import sys
import json
import tempfile
import shutil
import datetime
from unittest.mock import patch, MagicMock
from pathlib import Path

"""
Run it like: python3 -m unittest discover -v
"""

# Import the orchestrator module
import directed_graph_system.main as dg_main

class TestDirectedGraphSystem(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory to stand in for BASE_DIR
        self.tmpdir = Path(tempfile.mkdtemp())
        # Patch constants in the module
        dg_main.BASE_DIR = self.tmpdir
        dg_main.PROJECT_ROOT = self.tmpdir / 'project'
        dg_main.PROJECT_ROOT.mkdir()
        # Create process_files directory
        self.process_dir = dg_main.PROJECT_ROOT / 'process_files'
        self.process_dir.mkdir()
        dg_main.PROCESS_DIR = self.process_dir
        # Set LOCK_FILE and CONFIG_FILE in BASE_DIR
        dg_main.LOCK_FILE = self.tmpdir / 'main.lock'
        dg_main.MAIN_LOG = self.tmpdir / 'main.log'
        dg_main.ERROR_LOG = self.tmpdir / 'error.log'
        dg_main.BENCH_LOG = self.tmpdir / 'benchmarks.log'
        dg_main.CONFIG_FILE = self.tmpdir / 'config.json'
        # Ensure no lock on start
        if dg_main.LOCK_FILE.exists():
            dg_main.LOCK_FILE.unlink()
        # Reset HAS_LOCK
        dg_main.HAS_LOCK = False

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_load_graph_success(self):
        # Write a valid config.json
        graph = {
            "nodes": {
                "a.py": {"in": []},
                "b.py": {"in": ["a.py"]}
            }
        }
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        loaded = dg_main.load_graph()
        self.assertEqual(loaded, graph['nodes'])

    def test_load_graph_failure(self):
        # Write invalid JSON
        dg_main.CONFIG_FILE.write_text("not json")
        with self.assertRaises(SystemExit):
            dg_main.load_graph()

    def test_acquire_and_release_lock(self):
        # Acquire lock first time
        dg_main.acquire_lock()
        self.assertTrue(dg_main.HAS_LOCK)
        self.assertTrue(dg_main.LOCK_FILE.exists())
        # Release lock
        dg_main.release_lock()
        self.assertFalse(dg_main.LOCK_FILE.exists())
        self.assertFalse(dg_main.HAS_LOCK)

    def test_acquire_lock_stale_and_active(self):
        # Simulate stale lock (invalid PID)
        dg_main.LOCK_FILE.write_text("notanint")
        dg_main.acquire_lock()  # should remove stale and reacquire
        self.assertTrue(dg_main.LOCK_FILE.exists())
        pid1 = dg_main.LOCK_FILE.read_text()
        dg_main.release_lock()
        # Simulate active lock
        dg_main.LOCK_FILE.write_text(str(os.getpid()))
        with self.assertRaises(SystemExit):
            dg_main.acquire_lock()
        # Should not remove lock because HAS_LOCK is False
        self.assertTrue(dg_main.LOCK_FILE.exists())

    @patch('subprocess.Popen')
    def test_launch_node_success_and_error(self, mock_popen):
        # Prepare a dummy process
        dummy = MagicMock()
        mock_popen.return_value = dummy
        proc, start = dg_main.launch_node('foo.py', MagicMock())
        mock_popen.assert_called_once()
        self.assertIs(proc, dummy)
        self.assertIsInstance(start, datetime.datetime)
        # Simulate failure to start
        mock_popen.side_effect = Exception('fail')
        proc2, start2 = dg_main.launch_node('bar.py', MagicMock())
        self.assertIsNone(proc2)
        self.assertIsNone(start2)

    @patch('directed_graph_system.main.os.kill')
    @patch('directed_graph_system.main.subprocess.Popen')
    def test_main_execution_and_cycle(self, mock_popen, mock_kill):
        # Setup graph.json with one linear dependency
        graph = {"nodes": {"f1.py": {"in": []}, "f2.py": {"in": ["f1.py"]}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        # Create dummy scripts in process_files
        (self.process_dir / 'f1.py').write_text('')
        (self.process_dir / 'f2.py').write_text('')
        # Prepare Popen behavior: simulate f1 then f2
        class DummyProc:
            def __init__(self, rc): self.rc = rc; self._polled = False
            def poll(self):
                if not self._polled:
                    self._polled = True; return None
                return self.rc
            def communicate(self): return ("out","err")
        mock_popen.side_effect = [DummyProc(0), DummyProc(0)]
        # Run main (should complete without SystemExit)
        dg_main.main()
        # Now test cycle detection
        graph_cycle = {"nodes": {"a.py":{"in":["b.py"]},"b.py":{"in":["a.py"]}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph_cycle))
        (self.process_dir / 'a.py').write_text('')
        (self.process_dir / 'b.py').write_text('')
        with self.assertRaises(SystemExit):
            dg_main.main()

    def test_missing_dependency(self):
        # Graph refers to undefined prereq
        graph = {"nodes": {"x.py": {"in": ["missing.py"]}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        # Should error due to undefined prereq
        with self.assertRaises(SystemExit):
            dg_main.main()

if __name__ == '__main__':
    unittest.main(verbosity=2)
