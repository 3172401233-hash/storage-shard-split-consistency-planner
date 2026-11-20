"""数据模型：分裂请求、阶段规格、校验样本与回滚点。"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# 阶段名称常量（按执行顺序）
PHASE_PREPARE = "prepare_children"
PHASE_MIRROR = "mirror_writes"
PHASE_BACKFILL = "backfill"
PHASE_DOUBLE_READ = "double_read"
PHASE_ROUTE_SWITCH = "route_switch"
PHASE_SHRINK = "shrink_parent"

PHASE_ORDER = [
    PHASE_PREPARE,
    PHASE_MIRROR,
    PHASE_BACKFILL,
    PHASE_DOUBLE_READ,
    PHASE_ROUTE_SWITCH,
    PHASE_SHRINK,
]

# 回滚动作
ROLLBACK_ABORT_SINGLE_PARENT = "abort_to_single_parent"
ROLLBACK_KEEP_MIRRORING = "keep_mirroring"
ROLLBACK_KEEP_DOUBLE_READ = "keep_double_read"


@dataclass
class KeyWeight:
    """键分布中的一项：键与其写入速率（次/秒）。"""
    key: str
    writes_per_sec: float


@dataclass
class SplitRequest:
    """分裂规划输入。"""
    parent_range: Tuple[str, str]          # [start, end)
    split_point: str
    key_distribution: List[KeyWeight]
    current_version: int                   # 当前路由版本
    write_rate: float                      # 父分片总写入速率（次/秒）
    replica_locations: List[str]           # 可用副本位置
    replication_lag_ms: float              # 副本复制延迟
    boundary_window: int = 4               # 分裂点每侧视为边界的键数
    hot_sample_count: int = 8              # 校验样本中的热点键数量
    sample_stride: int = 7                 # 校验样本的等距抽样步长

    def validate(self) -> None:
        start, end = self.parent_range
        if not start <= self.split_point < end:
            raise ValueError(
                "split_point %r 不在父范围 [%r, %r) 内" % (self.split_point, start, end)
            )
        if self.write_rate < 0:
            raise ValueError("write_rate 不能为负")
        if self.replication_lag_ms < 0:
            raise ValueError("replication_lag_ms 不能为负")
        if not self.replica_locations:
            raise ValueError("至少需要一个副本位置")


@dataclass
class RollbackPoint:
    """某阶段失败时的回滚点。"""
    phase: str
    action: str                            # ROLLBACK_* 常量之一
    description: str


@dataclass
class PhaseSpec:
    """单个阶段的规划结果。"""
    name: str
    routing_version: int                   # 该阶段生效的路由版本
    details: Dict[str, Any] = field(default_factory=dict)
    rollback_point: Optional[RollbackPoint] = None


@dataclass
class ValidationSample:
    """双读校验样本。"""
    key: str
    expected_shard: str                    # "parent_left" 或 "child_right"
    reason: str                            # "boundary" | "hot" | "stride"
    sample_id: str


@dataclass
class SplitPlan:
    """完整分裂计划。"""
    split_point: str
    left_range: Tuple[str, str]
    right_range: Tuple[str, str]
    phases: List[PhaseSpec]
    validation_samples: List[ValidationSample]
    boundary_keys: List[str]
    mirror_window_ms: float
    replica_assignment: Dict[str, List[str]]

    def phase_names(self) -> List[str]:
        return [p.name for p in self.phases]

    def rollback_for(self, phase_name: str) -> Optional[RollbackPoint]:
        for phase in self.phases:
            if phase.name == phase_name:
                return phase.rollback_point
        return None
