import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import tartape
from tartape.exceptions import TarIntegrityError
from tartape.schemas import VerificationReport


class TestTapeVerification(unittest.TestCase):
    """Unified test suite for Tape.verify(), Canary heuristics, and OS noise tolerance."""

    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name) / "test_data"
        self.root_path.mkdir()

        # Create basic directory structure
        (self.root_path / "subdir").mkdir()
        (self.root_path / "file1.txt").write_text("Hello World 1")
        (self.root_path / "subdir" / "file2.txt").write_text("Hello World 2")
        (self.root_path / "z_last_file.txt").write_text("Last alphabetical file")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_report_truthiness_and_contract(self):
        """Ensure VerificationReport satisfies boolean truthiness and tracks metrics."""
        tape = tartape.record(self.root_path)

        report = tape.verify()
        self.assertIsInstance(report, VerificationReport)
        self.assertTrue(report.is_valid)
        self.assertTrue(bool(report))
        self.assertEqual(report.error_count, 0)
        self.assertEqual(report.mode, "deep")
        self.assertEqual(report.checked_count, tape.file_count)
        self.assertIn("PASSED", report.summary())

    def test_deep_audit_collects_multiple_discrepancies(self):
        """Ensure deep mode audits without early abort, gathering all discrepancies."""
        tape = tartape.record(self.root_path)

        time.sleep(0.01)  # Ensure mtime diff
        (self.root_path / "file1.txt").write_text("Modified content with more bytes!")
        (self.root_path / "subdir" / "file2.txt").unlink()

        report = tape.verify(deep=True)
        self.assertFalse(report.is_valid)
        self.assertFalse(bool(report))
        self.assertEqual(report.error_count, 2)

        reasons = {d.reason for d in report.discrepancies}
        self.assertIn("missing", reasons)
        self.assertTrue("size_mismatch" in reasons or "mtime_mismatch" in reasons)
        self.assertIn("FAILED", report.summary())

    def test_raise_exception_flag(self):
        """Ensure raise_exception=True raises TarIntegrityError on discrepancy."""
        tape = tartape.record(self.root_path)
        (self.root_path / "file1.txt").unlink()

        with self.assertRaises(TarIntegrityError):
            tape.verify(raise_exception=True)

    def test_os_noise_exclusion_tolerance(self):
        """Ensure standard OS artifacts (.DS_Store, Thumbs.db) do not fail integrity or streaming."""
        tape = tartape.record(self.root_path)

        # Simulate macOS Finder and Windows Explorer dropping noise
        (self.root_path / ".DS_Store").write_bytes(b"\x00\x00\x00\x01")
        (self.root_path / "subdir" / ".DS_Store").write_bytes(b"\x00\x00\x00\x01")
        (self.root_path / "Thumbs.db").write_bytes(b"dummy thumbnail cache")

        # Deep verification must still pass cleanly
        report = tape.verify(deep=True)
        self.assertTrue(
            report.is_valid,
            f"Verification failed due to OS noise: {[d.message for d in report.discrepancies]}",
        )

        # Streaming must also work without integrity errors
        with tape.as_file() as stream:
            data = stream.read(512)
            self.assertGreater(len(data), 0)

    def test_untracked_non_excluded_file_fails_verification(self):
        """Ensure untracked files that are not excluded fail root structural integrity."""
        tape = tartape.record(self.root_path)
        (self.root_path / "intruder.txt").write_text("I was added after T0")

        report = tape.verify(deep=True)
        self.assertFalse(report.is_valid)
        reasons = [d.reason for d in report.discrepancies]
        self.assertIn("untracked_item", reasons)

    def test_canary_heuristics_and_boundary_detection(self):
        """Ensure canary mode audits fast (< 60 checks) and catches boundary tampering."""
        # Create dataset > 50 files to exercise true canary sentinel selection
        large_root = Path(self.temp_dir.name) / "large_dataset"
        large_root.mkdir()
        for i in range(60):
            (large_root / f"file_{i:03d}.txt").write_text(f"Content {i}")

        tape = tartape.record(large_root)

        # 1. Clean canary check
        clean_report = tape.verify(deep=False)
        self.assertTrue(clean_report.is_valid)
        self.assertEqual(clean_report.mode, "canary")
        self.assertGreater(clean_report.checked_count, 0)
        self.assertLess(clean_report.checked_count, 60)

        # 2. Tamper with the last boundary sentinel
        time.sleep(0.01)
        (large_root / "file_059.txt").write_text("Mutated last boundary file")

        canary_report = tape.verify(deep=False)
        self.assertFalse(canary_report.is_valid)

    def test_streaming_jit_catches_unverified_file(self):
        """Ensure runtime JIT guard catches modified files during playback."""
        tape = tartape.record(self.root_path)

        time.sleep(0.01)
        (self.root_path / "subdir" / "file2.txt").write_text("Corrupted after record")

        with self.assertRaises(TarIntegrityError):
            for _ in tape.play(fast_verify=True):
                pass


if __name__ == "__main__":
    unittest.main()
