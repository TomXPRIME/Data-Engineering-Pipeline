"""Tests for simulator fundamental emit patterns."""
import pytest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.simulators.comprehensive_simulator import ComprehensiveSimulator


class TestFundamentalEmitPattern:
    """Verify simulator uses seed+incremental, not batch dump."""

    def test_no_batch_dump_method_exists(self):
        """_emit_all_fundamentals should be deleted."""
        import pipeline.simulators.comprehensive_simulator as mod
        assert not hasattr(mod.ComprehensiveSimulator, '_emit_all_fundamentals'), \
            "_emit_all_fundamentals should be deleted"

    def test_seed_method_exists(self):
        """_seed_all_fundamentals should exist."""
        sim = ComprehensiveSimulator.__new__(ComprehensiveSimulator)
        assert hasattr(sim, '_seed_all_fundamentals'), \
            "_seed_all_fundamentals should exist"