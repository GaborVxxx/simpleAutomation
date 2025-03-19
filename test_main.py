import unittest
import subprocess
import json
import os
from io import StringIO
import logging
from unittest.mock import patch, MagicMock

# Import functions from your main.py module.
# Adjust the names below if you have structured your module differently.
import main


class TestMain(unittest.TestCase):
    def setUp(self):
        # Create a benchmark logger that writes to a StringIO object so we can inspect its output.
        self.log_stream = StringIO()
        self.benchmark_logger = logging.getLogger("benchmark_test")
        self.benchmark_logger.setLevel(logging.INFO)
        # Clear any existing handlers to avoid duplicate logs.
        self.benchmark_logger.handlers.clear()
        stream_handler = logging.StreamHandler(self.log_stream)
        formatter = logging.Formatter('%(asctime)s: %(message)s')
        stream_handler.setFormatter(formatter)
        self.benchmark_logger.addHandler(stream_handler)

    def tearDown(self):
        # Clean up handlers after each test.
        handlers = self.benchmark_logger.handlers[:]
        for handler in handlers:
            self.benchmark_logger.removeHandler(handler)

    @patch('main.subprocess.run')
    def test_run_file_success(self, mock_run):
        # Simulate a successful subprocess.run call.
        dummy_result = MagicMock()
        dummy_result.stdout = "Success output"
        dummy_result.returncode = 0
        mock_run.return_value = dummy_result

        result = main.run_file("dummy_success.py", self.benchmark_logger)
        self.assertTrue(result)
        log_output = self.log_stream.getvalue()
        self.assertIn("dummy_success.py started", log_output)
        self.assertIn("dummy_success.py ended", log_output)

    @patch('main.subprocess.run')
    def test_run_file_failure(self, mock_run):
        # Simulate subprocess.run raising a CalledProcessError.
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd="python dummy_failure.py",
            stderr="Error occurred"
        )
        result = main.run_file("dummy_failure.py", self.benchmark_logger)
        self.assertFalse(result)
        log_output = self.log_stream.getvalue()
        self.assertIn("dummy_failure.py started", log_output)
        self.assertIn("dummy_failure.py ended", log_output)

    @patch('main.run_file')
    def test_run_batch_sync_success(self, mock_run_file):
        # Simulate that each script in the batch runs successfully.
        mock_run_file.return_value = True
        scripts = ["script1.py", "script2.py"]
        result = main.run_batch_sync(scripts, self.benchmark_logger)
        self.assertTrue(result)
        log_output = self.log_stream.getvalue()
        self.assertIn("Batch started", log_output)
        self.assertIn("Batch ended", log_output)
        self.assertIn("-" * 30, log_output)  # Check for the separator.

    @patch('main.run_file')
    def test_run_batch_sync_failure(self, mock_run_file):
        # Simulate that one script in the batch fails.
        mock_run_file.side_effect = [True, False]
        scripts = ["script1.py", "script2.py"]
        result = main.run_batch_sync(scripts, self.benchmark_logger)
        self.assertFalse(result)

    @patch('main.subprocess.Popen')
    def test_run_batch_async_success(self, mock_popen):
        # Simulate two processes that run successfully.
        dummy_proc = MagicMock()
        dummy_proc.communicate.return_value = ("Output", "")
        dummy_proc.returncode = 0

        mock_popen.return_value = dummy_proc
        scripts = ["script1.py", "script2.py"]
        result = main.run_batch_async(scripts, self.benchmark_logger)
        self.assertTrue(result)
        log_output = self.log_stream.getvalue()
        self.assertIn("Batch started", log_output)
        self.assertIn("Batch ended", log_output)
        self.assertIn("-" * 30, log_output)

    @patch('main.subprocess.Popen')
    def test_run_batch_async_failure(self, mock_popen):
        # Simulate one process succeeds and another fails.
        dummy_proc_success = MagicMock()
        dummy_proc_success.communicate.return_value = ("Output", "")
        dummy_proc_success.returncode = 0

        dummy_proc_failure = MagicMock()
        dummy_proc_failure.communicate.return_value = ("", "Error occurred")
        dummy_proc_failure.returncode = 1

        # First call returns success, second returns failure.
        mock_popen.side_effect = [dummy_proc_success, dummy_proc_failure]
        scripts = ["script1.py", "script2.py"]
        result = main.run_batch_async(scripts, self.benchmark_logger)
        self.assertFalse(result)

    def test_load_config_success(self):
        # Create a temporary config file for testing.
        config_data = {
            "execution_mode": "sync",
            "batches": [["script1.py"], ["script2.py"]]
        }
        temp_config_file = "temp_config.json"
        with open(temp_config_file, "w") as f:
            json.dump(config_data, f)

        loaded_config = main.load_config(temp_config_file)
        self.assertEqual(loaded_config, config_data)

        os.remove(temp_config_file)


if __name__ == '__main__':
    unittest.main()
