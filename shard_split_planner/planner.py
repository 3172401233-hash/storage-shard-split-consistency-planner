"""分裂规划器：只产出计划，不直接迁移数据。"""
import bisect
import hashlib
from typing import Dict, List, Tuple

from .models import (
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
    KeyWeight,
    PhaseSpec,
    RollbackPoint,
    SplitPlan,
    SplitRequest,
    ValidationSample,
)

# 镜像写入窗口相对复制延迟的安全系数
LAG_SAFETY_FACTOR = 2.0


class SplitPlanner:
    """根据输入生成分裂计划。

    规划器不直接迁移数据；它输出阶段顺序、每阶段路由版本、
    校验样本与回滚点，由存储引擎/执行器按序执行。
    """

    def plan(self, request: SplitRequest) -> SplitPlan:
        request.validate()
        start, end = request.parent_range
        split = request.split_point

        boundary_keys = self._boundary_keys(request)
        mirror_window_ms = self._mirror_window_ms(request, boundary_keys)
        replica_assignment = self._assign_replicas(request)
        samples = self._validation_samples(request, boundary_keys)

        old_version = request.current_version
        new_version = old_version + 1
        routing_versions = {
            PHASE_PREPARE: old_version,
            PHASE_MIRROR: old_version,
            PHASE_BACKFILL: old_version,
            PHASE_DOUBLE_READ: old_version,
            PHASE_ROUTE_SWITCH: new_version,
            PHASE_SHRINK: new_version,
        }
        rollback_points = self._rollback_points()

        phases = []
        for name in PHASE_ORDER:
            phases.append(PhaseSpec(
                name=name,
                routing_version=routing_versions[name],
                details=self._phase_details(
                    name, request, boundary_keys, mirror_window_ms,
                    replica_assignment, samples,
                ),
                rollback_point=rollback_points[name],
            ))

        return SplitPlan(
            split_point=split,
            left_range=(start, split),
            right_range=(split, end),
            phases=phases,
            validation_samples=samples,
            boundary_keys=boundary_keys,
            mirror_window_ms=mirror_window_ms,
            replica_assignment=replica_assignment,
        )

    def _boundary_keys(self, request: SplitRequest) -> List[str]:
        """分裂点两侧各 boundary_window 个键视为边界键，切换前必须被镜像覆盖。"""
        keys = sorted(kw.key for kw in request.key_distribution)
        idx = bisect.bisect_left(keys, request.split_point)
        window = request.boundary_window
        lo = max(0, idx - window)
        hi = min(len(keys), idx + window)
        return keys[lo:hi]

    def _mirror_window_ms(self, request: SplitRequest, boundary_keys: List[str]) -> float:
        """镜像写入需持续的时长：覆盖复制延迟 + 边界键至少各被写一次的期望时间。"""
        lag_part = request.replication_lag_ms * LAG_SAFETY_FACTOR
        boundary_rate = sum(
            kw.writes_per_sec
            for kw in request.key_distribution
            if kw.key in set(boundary_keys)
        )
        if request.write_rate > 0 and boundary_rate > 0:
            # 边界键按其在总写入中的占比估算被覆盖一次所需的时长
            coverage_part = 1000.0 * len(boundary_keys) / max(
                boundary_rate, 1e-9
            )
        else:
            coverage_part = 0.0
        return lag_part + coverage_part

    def _assign_replicas(self, request: SplitRequest) -> Dict[str, List[str]]:
        locations = list(request.replica_locations)
        half = max(1, (len(locations) + 1) // 2)
        return {
            "parent_left": locations[:half],
            "child_right": locations[half:] or locations[:half],
        }

    def _validation_samples(
        self, request: SplitRequest, boundary_keys: List[str]
    ) -> List[ValidationSample]:
        split = request.split_point
        samples = []
        seen = set()

        def add(key: str, reason: str) -> None:
            if key in seen:
                return
            seen.add(key)
            side = "parent_left" if key < split else "child_right"
            sample_id = hashlib.sha1(
                ("%s|%s" % (reason, key)).encode("utf-8")
            ).hexdigest()[:12]
            samples.append(ValidationSample(
                key=key, expected_shard=side, reason=reason, sample_id=sample_id,
            ))

        for key in boundary_keys:
            add(key, "boundary")

        hottest = sorted(
            request.key_distribution,
            key=lambda kw: (-kw.writes_per_sec, kw.key),
        )[: request.hot_sample_count]
        for kw in hottest:
            add(kw.key, "hot")

        ordered = sorted(kw.key for kw in request.key_distribution)
        stride = max(1, request.sample_stride)
        for key in ordered[::stride]:
            add(key, "stride")

        return samples

    def _rollback_points(self) -> Dict[str, RollbackPoint]:
        return {
            PHASE_PREPARE: RollbackPoint(
                PHASE_PREPARE, ROLLBACK_ABORT_SINGLE_PARENT,
                "子分片未启用，直接放弃，保持单父范围",
            ),
            PHASE_MIRROR: RollbackPoint(
                PHASE_MIRROR, ROLLBACK_ABORT_SINGLE_PARENT,
                "停止镜像写入并丢弃子分片，恢复单父范围",
            ),
            PHASE_BACKFILL: RollbackPoint(
                PHASE_BACKFILL, ROLLBACK_KEEP_MIRRORING,
                "回填失败：保持镜像双写，可重试回填或回退单父范围",
            ),
            PHASE_DOUBLE_READ: RollbackPoint(
                PHASE_DOUBLE_READ, ROLLBACK_KEEP_MIRRORING,
                "校验不一致：保持镜像双写，禁止路由切换",
            ),
            PHASE_ROUTE_SWITCH: RollbackPoint(
                PHASE_ROUTE_SWITCH, ROLLBACK_KEEP_DOUBLE_READ,
                "切换后失败：保留双读状态与父副本，不收缩父范围",
            ),
            PHASE_SHRINK: RollbackPoint(
                PHASE_SHRINK, ROLLBACK_KEEP_DOUBLE_READ,
                "收缩失败：保留双读状态，父分片继续保留全量数据",
            ),
        }

    def _phase_details(
        self,
        name: str,
        request: SplitRequest,
        boundary_keys: List[str],
        mirror_window_ms: float,
        replica_assignment: Dict[str, List[str]],
        samples: List[ValidationSample],
    ) -> Dict[str, object]:
        split = request.split_point
        if name == PHASE_PREPARE:
            return {
                "left_range": [request.parent_range[0], split],
                "right_range": [split, request.parent_range[1]],
                "replica_assignment": replica_assignment,
            }
        if name == PHASE_MIRROR:
            return {
                "mirror_window_ms": mirror_window_ms,
                "covered_boundary_keys": list(boundary_keys),
                "fence_token": request.current_version,
            }
        if name == PHASE_BACKFILL:
            return {
                "dedup": "last_write_wins_by_version",
                "copy_tombstones": True,
            }
        if name == PHASE_DOUBLE_READ:
            return {
                "sample_count": len(samples),
                "sample_ids": [s.sample_id for s in samples],
            }
        if name == PHASE_ROUTE_SWITCH:
            return {
                "new_routing_version": request.current_version + 1,
                "fence": "reject client_version < %d" % (request.current_version + 1),
            }
        if name == PHASE_SHRINK:
            return {
                "parent_keeps": [request.parent_range[0], split],
                "precondition": "no in-flight writes with old routing version",
            }
        return {}
