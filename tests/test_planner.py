import unittest

from shard_split_planner import SplitPlanner
from shard_split_planner.models import (
    PHASE_BACKFILL,
    PHASE_DOUBLE_READ,
    PHASE_MIRROR,
    PHASE_ORDER,
    PHASE_PREPARE,
    PHASE_ROUTE_SWITCH,
    PHASE_SHRINK,
    ROLLBACK_ABORT_SINGLE_PARENT,
    ROLLBACK_KEEP_DOUBLE_READ,
    ROLLBACK_KEEP_MIRRORING,
)

from tests.helpers import make_request


class TestPhaseOrdering(unittest.TestCase):
    def setUp(self):
        self.plan = SplitPlanner().plan(make_request())

    def test_phase_order(self):
        self.assertEqual(self.plan.phase_names(), PHASE_ORDER)
        self.assertEqual(
            PHASE_ORDER,
            [
                PHASE_PREPARE,
                PHASE_MIRROR,
                PHASE_BACKFILL,
                PHASE_DOUBLE_READ,
                PHASE_ROUTE_SWITCH,
                PHASE_SHRINK,
            ],
        )

    def test_routing_versions(self):
        versions = {p.name: p.routing_version for p in self.plan.phases}
        # 切换前保持旧路由版本，切换时单调 +1
        for name in (PHASE_PREPARE, PHASE_MIRROR, PHASE_BACKFILL, PHASE_DOUBLE_READ):
            self.assertEqual(versions[name], 7, name)
        for name in (PHASE_ROUTE_SWITCH, PHASE_SHRINK):
            self.assertEqual(versions[name], 8, name)

    def test_child_ranges_cover_parent(self):
        self.assertEqual(self.plan.left_range, ("a", "k050"))
        self.assertEqual(self.plan.right_range, ("k050", "z"))


class TestBoundaryCoverage(unittest.TestCase):
    def setUp(self):
        self.plan = SplitPlanner().plan(make_request())

    def test_boundary_keys_straddle_split_point(self):
        keys = self.plan.boundary_keys
        self.assertEqual(len(keys), 8)  # 每侧 4 个
        self.assertTrue(all(k < "k050" for k in keys[:4]))
        self.assertTrue(all(k >= "k050" for k in keys[4:]))
        self.assertEqual(keys[3], "k049")
        self.assertEqual(keys[4], "k050")

    def test_mirror_phase_covers_all_boundary_keys(self):
        mirror = next(p for p in self.plan.phases if p.name == PHASE_MIRROR)
        self.assertEqual(
            sorted(mirror.details["covered_boundary_keys"]),
            sorted(self.plan.boundary_keys),
        )

    def test_mirror_window_covers_replication_lag(self):
        # 镜像窗口至少为复制延迟的 2 倍
        self.assertGreaterEqual(self.plan.mirror_window_ms, 100.0)

    def test_boundary_keys_in_validation_samples(self):
        sampled = {s.key for s in self.plan.validation_samples}
        for key in self.plan.boundary_keys:
            self.assertIn(key, sampled)

    def test_sample_expected_shard_sides(self):
        for sample in self.plan.validation_samples:
            if sample.key < "k050":
                self.assertEqual(sample.expected_shard, "parent_left")
            else:
                self.assertEqual(sample.expected_shard, "child_right")


class TestRollbackPoints(unittest.TestCase):
    def setUp(self):
        self.plan = SplitPlanner().plan(make_request())

    def test_every_phase_has_rollback_point(self):
        for phase in self.plan.phases:
            self.assertIsNotNone(phase.rollback_point, phase.name)
            self.assertEqual(phase.rollback_point.phase, phase.name)

    def test_rollback_actions(self):
        expected = {
            PHASE_PREPARE: ROLLBACK_ABORT_SINGLE_PARENT,
            PHASE_MIRROR: ROLLBACK_ABORT_SINGLE_PARENT,
            PHASE_BACKFILL: ROLLBACK_KEEP_MIRRORING,
            PHASE_DOUBLE_READ: ROLLBACK_KEEP_MIRRORING,
            PHASE_ROUTE_SWITCH: ROLLBACK_KEEP_DOUBLE_READ,
            PHASE_SHRINK: ROLLBACK_KEEP_DOUBLE_READ,
        }
        for name, action in expected.items():
            self.assertEqual(self.plan.rollback_for(name).action, action, name)


class TestValidation(unittest.TestCase):
    def test_split_point_out_of_range_rejected(self):
        with self.assertRaises(ValueError):
            SplitPlanner().plan(make_request(split_point="zzz"))

    def test_empty_replicas_rejected(self):
        with self.assertRaises(ValueError):
            SplitPlanner().plan(make_request(replica_locations=[]))


if __name__ == "__main__":
    unittest.main()
