import unittest
import subprocess
import json
import os
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import patch, MagicMock

"""
Run it like: python3 -m unittest discover -v
"""

# Import from the refactored module
from matrix_system.main import (
    run_file,
    run_batch_sync,
    run_batch_async,
    load_config,
    acquire_lock,
    release_lock,
    LOCK_FILE,
    PROCESS_DIR,
    CONFIG_FILE,
    HAS_LOCK
)

class TestMatrixSystemMain(unittest.TestCase):
    def setUp(self):
        # Redirect benchmark logs to a StringIO for inspection
        self.log_stream = StringIO()
        self.bench_logger = unittest.mock.MagicMock()
        handler = unittest.mock.MagicMock()
        handler.stream = self.log_stream
        self.bench_logger.handlers = [handler]

        # Ensure clean slate for lock file
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()

    def tearDown(self):
        # Remove lock and temp config if left behind
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
        # Clean up any test config we may have written
        tmp = CONFIG_FILE.parent / 'temp_test_config.json'
        if tmp.exists():
            tmp.unlink()

    @patch('matrix_system.main.subprocess.run')
    def test_run_file_success(self, mock_run):
        # Simulate successful run
        result_obj = MagicMock(stdout="OK", returncode=0)
        mock_run.return_value = result_obj

        ok = run_file("dummy.py", self.bench_logger)
        self.assertTrue(ok)
        mock_run.assert_called_once_with(
            [sys.executable, str(PROCESS_DIR / "dummy.py")],
            check=True, capture_output=True, text=True
        )

    @patch('matrix_system.main.subprocess.run')
    def test_run_file_failure(self, mock_run):
        # Simulate CalledProcessError
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="cmd", stderr="ERR"
        )
        ok = run_file("fail.py", self.bench_logger)
        self.assertFalse(ok)

    @patch('matrix_system.main.run_file')
    def test_run_batch_sync_all_ok(self, mock_run_file):
        mock_run_file.return_value = True
        ok = run_batch_sync(["a.py","b.py"], self.bench_logger)
        self.assertTrue(ok)

    @patch('matrix_system.main.run_file')
    def test_run_batch_sync_failure(self, mock_run_file):
        # Second script fails
        mock_run_file.side_effect = [True, False]
        ok = run_batch_sync(["a.py","b.py"], self.bench_logger)
        self.assertFalse(ok)

    @patch('matrix_system.main.subprocess.Popen')
    def test_run_batch_async_success(self, mock_popen):
        proc = MagicMock()
        proc.communicate.return_value = ("out","")
        proc.returncode = 0
        mock_popen.return_value = proc

        ok = run_batch_async(["x.py","y.py"], self.bench_logger)
        self.assertTrue(ok)

    @patch('matrix_system.main.subprocess.Popen')
    def test_run_batch_async_failure(self, mock_popen):
        # First OK, second fails
        p1 = MagicMock(); p1.communicate.return_value = ("",""); p1.returncode = 0
        p2 = MagicMock(); p2.communicate.return_value = ("","err"); p2.returncode = 1
        mock_popen.side_effect = [p1, p2]

        ok = run_batch_async(["x.py","y.py"], self.bench_logger)
        self.assertFalse(ok)

    def test_load_config_valid(self):
        data = {"execution_mode":"sync","batches":[["f.py"]]}
        tmp = CONFIG_FILE.parent / 'temp_test_config.json'
        with tmp.open('w') as f:
            json.dump(data, f)
        cfg = load_config(tmp.name)
        self.assertEqual(cfg, data)

    def test_acquire_lock_and_stale_cleanup(self):
        # Write a stale PID
        LOCK_FILE.write_text("999999")
        # Patch os.kill to throw OSError (process not found)
        with patch('matrix_system.main.os.kill', side_effect=OSError):
            # Should remove stale lock and then recreate it
            acquire_lock()
            pid_text = LOCK_FILE.read_text().strip()
            self.assertEqual(pid_text, str(os.getpid()))

    def test_acquire_lock_active_process_blocks(self):
        # Write a fake active PID (patch os.kill to do nothing)
        LOCK_FILE.write_text("1234")
        with patch('matrix_system.main.os.kill', return_value=None):
            with self.assertRaises(SystemExit):
                acquire_lock()

        # Lock file remains
        self.assertTrue(LOCK_FILE.exists())

    def test_release_lock(self):
        # First acquire the lock so HAS_LOCK=True and the file exists
        acquire_lock()
        self.assertTrue(LOCK_FILE.exists())
        # Now release it
        release_lock()
        self.assertFalse(LOCK_FILE.exists())
        # And confirm HAS_LOCK was reset (optional)
        self.assertFalse(HAS_LOCK)

if __name__ == '__main__':
    unittest.main(verbosity=2)
