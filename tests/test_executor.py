import unittest

from shard_split_planner import (
    BackfillError,
    PlanExecutor,
    ShrinkError,
    SplitPlanner,
    StaleRouteError,
    TOMBSTONE,
)
from shard_split_planner.executor import MODE_MIRRORING, MODE_SINGLE_PARENT, MODE_SPLIT
from shard_split_planner.models import (
    PHASE_BACKFILL,
    PHASE_SHRINK,
)

from tests.helpers import make_request


def make_executor(**overrides):
    request = make_request(**overrides)
    plan = SplitPlanner().plan(request)
    return PlanExecutor(plan, request)


class TestHotBoundaryWrites(unittest.TestCase):
    def test_boundary_writes_mirrored_and_consistent_after_switch(self):
        ex = make_executor()
        # 历史数据
        ex.write("k049", "old-left", 1)
        ex.write("k050", "old-right", 1)
        ex.run_prepare()
        ex.run_mirror()
        # 镜像期间热点边界写入
        for version in range(2, 12):
            ex.write("k049", "hot-left-%d" % version, version)
            ex.write("k050", "hot-right-%d" % version, version)
        ex.run_backfill()
        ex.run_double_read()
        ex.run_route_switch()
        ex.run_shrink()
        # 切换后边界键两侧各只有唯一版本
        self.assertEqual(ex.read_record("k049").value, "hot-left-11")
        self.assertEqual(ex.read_record("k050").value, "hot-right-11")
        # 父范围已收缩：右侧键不在父分片
        self.assertIsNone(ex.parent.get_record("k050"))
        self.assertIsNotNone(ex.parent.get_record("k049"))


class TestLateArrivingWrites(unittest.TestCase):
    def setUp(self):
        self.ex = make_executor()
        self.ex.run_prepare()
        self.ex.run_mirror()
        self.ex.run_backfill()
        self.ex.run_double_read()
        self.ex.run_route_switch()

    def test_old_route_version_write_is_fenced(self):
        with self.assertRaises(StaleRouteError):
            self.ex.write("k060", "late", 99, client_version=7)
        # 被拒绝而不是静默接受：子分片与父副本都没有该键
        self.assertIsNone(self.ex.children["right"].get_record("k060"))
        self.assertIsNone(self.ex.parent.get_record("k060"))

    def test_old_route_version_read_is_fenced(self):
        with self.assertRaises(StaleRouteError):
            self.ex.read_record("k010", client_version=7)

    def test_current_version_write_accepted(self):
        self.ex.write("k060", "fresh", 100, client_version=8)
        self.assertEqual(self.ex.read_record("k060").value, "fresh")


class TestDuplicateKeys(unittest.TestCase):
    def test_backfill_does_not_overwrite_newer_live_write(self):
        ex = make_executor()
        ex.write("k070", "v1", 1)
        ex.run_prepare()
        ex.run_mirror()
        # 镜像期间实时写入更高版本
        ex.write("k070", "v2-live", 5)
        # 回填携带父分片快照（同为 v5，幂等）；即使携带旧版本也不会覆盖
        ex.children["right"].apply("k070", "stale-backfill", 3)
        ex.run_backfill()
        self.assertEqual(ex.children["right"].get_record("k070").value, "v2-live")
        self.assertEqual(ex.children["right"].get_record("k070").version, 5)

    def test_duplicate_delivery_is_idempotent(self):
        ex = make_executor()
        ex.run_prepare()
        ex.run_mirror()
        ex.write("k003", "x", 4)
        ex.write("k003", "x", 4)  # 重复投递
        ex.run_backfill()
        record = ex.children["left"].get_record("k003")
        self.assertEqual(record.value, "x")
        self.assertEqual(record.version, 4)


class TestDeletesAndTombstones(unittest.TestCase):
    def test_tombstone_survives_backfill_and_switch(self):
        ex = make_executor()
        ex.write("k055", "to-be-deleted", 1)
        ex.run_prepare()
        ex.run_mirror()
        ex.delete("k055", 5)  # 镜像期间删除
        ex.run_backfill()
        ex.run_double_read()
        ex.run_route_switch()
        record = ex.read_record("k055")
        self.assertTrue(record.is_tombstone)
        self.assertEqual(record.version, 5)

    def test_backfill_does_not_resurrect_deleted_key(self):
        ex = make_executor()
        ex.run_prepare()
        ex.run_mirror()
        ex.delete("k056", 9)  # 只有 tombstone，无历史值
        # 模拟迟到的旧版本回填记录：不得复活该键
        ex.children["right"].apply("k056", "ghost", 2)
        record = ex.children["right"].get_record("k056")
        self.assertTrue(record.is_tombstone)
        self.assertEqual(record.version, 9)


class TestBackfillFailureRollback(unittest.TestCase):
    def test_backfill_failure_keeps_mirroring(self):
        ex = make_executor()
        ex.write("k010", "a", 1)
        ex.run_prepare()
        ex.run_mirror()
        with self.assertRaises(BackfillError):
            ex.run_backfill(fail_on_key="k010")
        # 回滚到镜像双写状态，路由版本不变
        self.assertEqual(ex.router.mode, MODE_MIRRORING)
        self.assertEqual(ex.router.version, 7)
        self.assertEqual(ex.rolled_back_at, PHASE_BACKFILL)
        # 镜像状态下写入仍然双写，重试回填可恢复
        ex.write("k010", "b", 2)
        ex.run_backfill()
        ex.run_double_read()
        ex.run_route_switch()
        ex.run_shrink()
        self.assertEqual(ex.read_record("k010").value, "b")

    def test_abort_to_single_parent(self):
        ex = make_executor()
        ex.run_prepare()
        ex.run_mirror()
        ex.rollback("mirror_writes")
        self.assertEqual(ex.router.mode, MODE_SINGLE_PARENT)
        self.assertIsNone(ex.children["left"])
        self.assertIsNone(ex.children["right"])
        # 单父范围继续服务
        ex.write("k099", "solo", 1)
        self.assertEqual(ex.parent.get_record("k099").value, "solo")


class TestShrinkFailureRollback(unittest.TestCase):
    def test_shrink_blocked_by_inflight_old_writes_keeps_double_read(self):
        ex = make_executor()
        ex.write("k060", "x", 1)
        ex.run_prepare()
        ex.run_mirror()
        ex.run_backfill()
        ex.run_double_read()
        ex.run_route_switch()
        ex.inflight_old_version_writes = 1
        with self.assertRaises(ShrinkError):
            ex.run_shrink()
        # 保留双读状态：父分片仍持有全量数据
        self.assertEqual(ex.router.mode, MODE_SPLIT)
        self.assertTrue(ex.router.keep_parent_sync)
        self.assertIsNotNone(ex.parent.get_record("k060"))
        self.assertEqual(ex.rolled_back_at, PHASE_SHRINK)
        # 在途写入排空后可重试收缩
        ex.inflight_old_version_writes = 0
        ex.run_shrink()
        self.assertIsNone(ex.parent.get_record("k060"))


class TestFullHappyPath(unittest.TestCase):
    def test_run_all(self):
        ex = make_executor()
        for i in range(100):
            ex.write("k%03d" % i, "v%d" % i, 1)
        ex.run_all()
        self.assertEqual(ex.router.version, 8)
        self.assertEqual(ex.router.mode, MODE_SPLIT)
        for i in range(100):
            record = ex.read_record("k%03d" % i)
            self.assertEqual(record.value, "v%d" % i)


if __name__ == "__main__":
    unittest.main()
