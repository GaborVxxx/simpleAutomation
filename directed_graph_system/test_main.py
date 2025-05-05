import unittest
import os
import sys
import json
import tempfile
import shutil
import datetime
import time
from unittest.mock import patch, MagicMock
from pathlib import Path

# Import the orchestrator module
import directed_graph_system.main as dg_main

"""
Run it like: python3 -m unittest discover -v
"""

class TestDirectedGraphSystem(unittest.TestCase):
    def setUp(self):
        # Setup temporary BASE_DIR
        self.tmpdir = Path(tempfile.mkdtemp())
        dg_main.BASE_DIR = self.tmpdir
        dg_main.PROJECT_ROOT = self.tmpdir / 'project'
        dg_main.PROJECT_ROOT.mkdir()
        self.process_dir = dg_main.PROJECT_ROOT / 'process_files'
        self.process_dir.mkdir()
        dg_main.PROCESS_DIR = self.process_dir
        # Patch file paths
        dg_main.LOCK_FILE = self.tmpdir / 'main.lock'
        dg_main.MAIN_LOG = self.tmpdir / 'main.log'
        dg_main.ERROR_LOG = self.tmpdir / 'error.log'
        dg_main.BENCH_LOG = self.tmpdir / 'benchmarks.log'
        dg_main.CONFIG_FILE = self.tmpdir / 'config.json'
        # Ensure clean state
        if dg_main.LOCK_FILE.exists():
            dg_main.LOCK_FILE.unlink()
        dg_main.HAS_LOCK = False

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_load_graph_success(self):
        data = {"nodes": {"a.py": {"in": []}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(data))
        loaded = dg_main.load_graph()
        self.assertEqual(loaded, data['nodes'])

    def test_load_graph_missing_nodes(self):
        # Config without 'nodes' should return {}
        dg_main.CONFIG_FILE.write_text(json.dumps({}))
        loaded = dg_main.load_graph()
        self.assertEqual(loaded, {})

    def test_load_graph_failure(self):
        dg_main.CONFIG_FILE.write_text("not-a-json")
        with self.assertRaises(SystemExit):
            dg_main.load_config()

    def test_acquire_and_release_lock(self):
        dg_main.acquire_lock()
        self.assertTrue(dg_main.HAS_LOCK)
        self.assertTrue(dg_main.LOCK_FILE.exists())
        self.assertEqual(dg_main.LOCK_FILE.read_text(), str(os.getpid()))
        dg_main.release_lock()
        self.assertFalse(dg_main.LOCK_FILE.exists())
        self.assertFalse(dg_main.HAS_LOCK)

    def test_acquire_lock_stale_and_active(self):
        # Stale PID in lock file
        dg_main.LOCK_FILE.write_text("badpid")
        dg_main.acquire_lock()
        self.assertTrue(dg_main.HAS_LOCK)
        dg_main.release_lock()
        # Active PID blocks acquisition
        dg_main.LOCK_FILE.write_text(str(os.getpid()))
        with self.assertRaises(SystemExit):
            dg_main.acquire_lock()

    @patch('subprocess.Popen')
    def test_launch_node(self, mock_popen):
        dummy_proc = MagicMock()
        mock_popen.return_value = dummy_proc
        proc, start = dg_main.launch_node('foo.py', MagicMock())
        self.assertIs(proc, dummy_proc)
        self.assertIsInstance(start, datetime.datetime)
        # Simulate exception when spawning
        mock_popen.side_effect = Exception('oops')
        proc2, start2 = dg_main.launch_node('bar.py', MagicMock())
        self.assertIsNone(proc2)
        self.assertIsNone(start2)

    @patch.object(dg_main.psutil, 'cpu_percent', side_effect=[100, 40])
    @patch('time.sleep', return_value=None)
    def test_wait_for_resources_cpu_then_pass(self, _, mock_cpu):
        dg_main.wait_for_resources({'cpu_percent': 50}, poll_interval=0, timeout=1)

    @patch.object(dg_main.psutil, 'virtual_memory', side_effect=[MagicMock(percent=100), MagicMock(percent=40)])
    @patch('time.sleep', return_value=None)
    def test_wait_for_resources_memory_then_pass(self, _, mock_mem):
        dg_main.wait_for_resources({'memory_percent': 50}, poll_interval=0, timeout=1)

    @patch('shutil.disk_usage', side_effect=[MagicMock(free=1024 * 1024 * 10), MagicMock(free=1024 * 1024 * 200)])
    @patch('time.sleep', return_value=None)
    def test_wait_for_resources_disk_then_pass(self, _, mock_disk):
        dg_main.wait_for_resources({'disk_free_mb': 100}, poll_interval=0, timeout=1)

    @patch.object(dg_main.psutil, 'getloadavg', side_effect=[(5, 0, 0), (0, 0, 0)])
    @patch('time.sleep', return_value=None)
    def test_wait_for_resources_load_then_pass(self, _, mock_load):
        dg_main.wait_for_resources({'load_avg_1m': 1}, poll_interval=0, timeout=1)

    def test_wait_for_resources_immediate(self):
        with patch.object(dg_main.psutil, 'cpu_percent', return_value=10), \
             patch.object(dg_main.psutil, 'virtual_memory', return_value=MagicMock(percent=10)), \
             patch('shutil.disk_usage', return_value=MagicMock(free=1024 * 1024 * 100)), \
             patch.object(dg_main.psutil, 'getloadavg', side_effect=AttributeError):
            start = time.time()
            dg_main.wait_for_resources(
                {'cpu_percent': 50, 'memory_percent': 50, 'disk_free_mb': 1, 'load_avg_1m': 1},
                poll_interval=0, timeout=1
            )
            self.assertLess(time.time() - start, 0.1)

    @patch.object(dg_main.psutil, 'cpu_percent', return_value=100)
    def test_wait_for_resources_timeout(self, mock_cpu):
        with self.assertRaises(TimeoutError):
            dg_main.wait_for_resources({'cpu_percent': 50}, poll_interval=0, timeout=0.1)

    @patch('subprocess.Popen')
    def test_main_empty_graph_no_deadline(self, mock_popen):
        dg_main.CONFIG_FILE.write_text(json.dumps({'nodes': {}}))
        dg_main.main()

    @patch('subprocess.Popen')
    def test_main_nonempty_graph_no_deadline(self, mock_popen):
        dg_main.CONFIG_FILE.write_text(json.dumps({'nodes': {'a.py': {'in': [], 'timeout': None}}}))
        (self.process_dir / 'a.py').write_text('')
        fake = MagicMock()
        fake.poll.side_effect = [0]
        fake.communicate.return_value = ('', '')
        with patch.object(dg_main, 'launch_node', return_value=(fake, datetime.datetime.now())):
            dg_main.main()

    @patch('subprocess.Popen')
    def test_main_deadline_exceeded(self, mock_popen):
        dg_main.CONFIG_FILE.write_text(json.dumps({'deadline_seconds': 0, 'nodes': {}}))
        with self.assertRaises(SystemExit):
            dg_main.main()

    def test_zero_global_deadline(self):
        dg_main.CONFIG_FILE.write_text(json.dumps({'deadline_seconds': 0, 'nodes': {}}))
        with self.assertRaises(SystemExit):
            dg_main.main()

    def test_node_timeout_zero(self):
        graph = {'nodes': {'a.py': {'in': [], 'timeout': 0}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        (self.process_dir / 'a.py').write_text('')
        P = MagicMock()
        P.poll.return_value = None
        P.kill.return_value = None
        with patch.object(dg_main, 'launch_node', return_value=(P, datetime.datetime.now())):
            with self.assertRaises(SystemExit):
                dg_main.main()

    def test_node_timeout_exceeded(self):
        graph = {'deadline_seconds': 10, 'nodes': {'a.py': {'in': [], 'timeout': 1}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        (self.process_dir / 'a.py').write_text('')
        fake_start = datetime.datetime.now() - datetime.timedelta(seconds=2)
        P = MagicMock(poll=MagicMock(return_value=None), kill=MagicMock())
        with patch.object(dg_main, 'launch_node', return_value=(P, fake_start)):
            with self.assertRaises(SystemExit):
                dg_main.main()

    def test_node_no_timeout(self):
        graph = {'nodes': {'a.py': {'in': [], 'timeout': None}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        (self.process_dir / 'a.py').write_text('')

        class P:
            def __init__(self): self.done = False
            def poll(self):
                if not self.done:
                    self.done = True
                    return None
                return 0
            def communicate(self): return ('ok', '')

        with patch.object(dg_main, 'launch_node', return_value=(P(), datetime.datetime.now())):
            dg_main.main()

    def test_missing_dependency(self):
        graph = {'nodes': {'x.py': {'in': ['missing.py'], 'timeout': None}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        with self.assertRaises(SystemExit):
            dg_main.main()

    def test_cycle_detection(self):
        graph = {'nodes': {
            'a.py': {'in': ['b.py'], 'timeout': None},
            'b.py': {'in': ['a.py'], 'timeout': None}
        }}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        (self.process_dir / 'a.py').write_text('')
        (self.process_dir / 'b.py').write_text('')
        with self.assertRaises(SystemExit):
            dg_main.main()

    def test_multi_node_dag_ordering(self):
        graph = {'nodes': {
            'a.py': {'in': [], 'timeout': None},
            'b.py': {'in': ['a.py'], 'timeout': None},
            'c.py': {'in': ['a.py'], 'timeout': None},
            'd.py': {'in': ['b.py', 'c.py'], 'timeout': None}
        }}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        for f in ['a.py', 'b.py', 'c.py', 'd.py']:
            (self.process_dir / f).write_text('')
        calls = []

        def fake_launch(node, bench):
            calls.append(node)
            P = MagicMock()
            P.poll.side_effect = [None, 0]
            P.communicate.return_value = ('', '')
            return (P, datetime.datetime.now())

        with patch.object(dg_main, 'launch_node', side_effect=fake_launch):
            dg_main.main()
        self.assertEqual(calls, ['a.py', 'b.py', 'c.py', 'd.py'])

    @patch('time.sleep', return_value=None)
    def test_resource_timeout_abort(self, mock_sleep):
        # Include a small deadline to ensure the resource wait will timeout
        graph = {'deadline_seconds': 0.1,
                 'resources': {'cpu_percent': 0},
                 'nodes': {'a.py': {'in': [], 'timeout': None}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        (self.process_dir / 'a.py').write_text('')
        with patch.object(dg_main.psutil, 'cpu_percent', return_value=100):
            with self.assertRaises(SystemExit):
                dg_main.main()

    def test_integration_resource_and_deadline(self):
        graph = {'deadline_seconds': 0.2, 'resources': {'cpu_percent': 50},
                 'nodes': {'a.py': {'in': [], 'timeout': None}}}
        dg_main.CONFIG_FILE.write_text(json.dumps(graph))
        (self.process_dir / 'a.py').write_text('')
        seq = [100, 10]

        def fake_cpu(*args, **kwargs): return seq.pop(0)

        Pfinish = MagicMock()
        Pfinish.poll.side_effect = [None, 0]
        Pfinish.communicate.return_value = ('', '')
        with patch.object(dg_main.psutil, 'cpu_percent', side_effect=fake_cpu), \
             patch('time.sleep', return_value=None), \
             patch.object(dg_main, 'launch_node', return_value=(Pfinish, datetime.datetime.now())):
            dg_main.main()

if __name__=='__main__':
    unittest.main(verbosity=2)
