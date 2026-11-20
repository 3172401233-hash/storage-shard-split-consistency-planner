"""计划执行器（模拟）：在内存版本化存储上执行分裂计划。

用于验证一致性语义：镜像写入、版本去重回填、tombstone 保留、
路由 fencing、双读校验与回滚。真实系统中这些步骤由存储引擎执行。
"""
from typing import Any, Dict, List, Optional

from .models import (
    PHASE_BACKFILL,
    PHASE_DOUBLE_READ,
    PHASE_MIRROR,
    PHASE_PREPARE,
    PHASE_ROUTE_SWITCH,
    PHASE_SHRINK,
    ROLLBACK_ABORT_SINGLE_PARENT,
    ROLLBACK_KEEP_DOUBLE_READ,
    ROLLBACK_KEEP_MIRRORING,
    SplitPlan,
    SplitRequest,
)


class _Tombstone(object):
    _instance = None

    def __repr__(self):
        return "TOMBSTONE"


TOMBSTONE = _Tombstone()


class StaleRouteError(Exception):
    """客户端携带过期路由版本：请求被 fencing 拒绝，而不是被静默接受。"""


class BackfillError(Exception):
    pass


class ConsistencyError(Exception):
    pass


class ShrinkError(Exception):
    pass


class Record(object):
    __slots__ = ("value", "version")

    def __init__(self, value: Any, version: int):
        self.value = value
        self.version = version

    @property
    def is_tombstone(self) -> bool:
        return self.value is TOMBSTONE

    def __eq__(self, other):
        return (
            isinstance(other, Record)
            and self.value == other.value
            and self.version == other.version
        )

    def __repr__(self):
        return "Record(value=%r, version=%r)" % (self.value, self.version)


class VersionedStore(object):
    """按键版本做 last-write-wins 的存储；tombstone 作为普通值参与去重。""" 

    def __init__(self):
        self.data = {}  # type: Dict[str, Record]

    def apply(self, key: str, value: Any, version: int) -> bool:
        """仅当版本不低于现有版本时写入；重复投递幂等。返回是否生效。"""
        existing = self.data.get(key)
        if existing is None or version >= existing.version:
            self.data[key] = Record(value, version)
            return True
        return False

    def get_record(self, key: str) -> Optional[Record]:
        return self.data.get(key)

    def delete_range(self, start: str, end: str) -> None:
        for key in [k for k in self.data if start <= k < end]:
            del self.data[key]


# 路由模式
MODE_SINGLE_PARENT = "single_parent"
MODE_MIRRORING = "mirroring"
MODE_SPLIT = "split"


class Router(object):
    """持有路由表与路由版本；对过期版本请求进行 fencing。""" 

    def __init__(self, version: int, split_point: str):
        self.version = version
        self.split_point = split_point
        self.mode = MODE_SINGLE_PARENT
        self.keep_parent_sync = True  # split 后、shrink 前保持父副本同步（双读状态）

    def fence(self, client_version: int) -> None:
        if client_version < self.version:
            raise StaleRouteError(
                "客户端路由版本 %d 落后于当前版本 %d，请求被拒绝"
                % (client_version, self.version)
            )

    def side_of(self, key: str) -> str:
        return "left" if key < self.split_point else "right"


class PlanExecutor(object):
    """按计划顺序执行各阶段；支持注入失败以验证回滚。""" 

    def __init__(self, plan: SplitPlan, request: SplitRequest):
        self.plan = plan
        self.request = request
        self.parent = VersionedStore()
        self.children = {
            "left": VersionedStore(),
            "right": VersionedStore(),
        }  # type: Dict[str, Optional[VersionedStore]]
        self.router = Router(request.current_version, plan.split_point)
        self.completed = []  # type: List[str]
        self.inflight_old_version_writes = 0
        self.rolled_back_at = None  # type: Optional[str]

    # ---- 客户端入口 ----

    def _serving_store(self, side: str) -> VersionedStore:
        """split 模式下某侧当前的服务存储：收缩后左半由父分片服务。"""
        store = self.children[side]
        if store is None:
            return self.parent
        return store

    def write(self, key: str, value: Any, version: int,
              client_version: Optional[int] = None) -> None:
        if client_version is None:
            client_version = self.router.version
        self.router.fence(client_version)
        mode = self.router.mode
        if mode == MODE_SINGLE_PARENT:
            self.parent.apply(key, value, version)
        elif mode == MODE_MIRRORING:
            self.parent.apply(key, value, version)
            child = self.children[self.router.side_of(key)]
            child.apply(key, value, version)
        elif mode == MODE_SPLIT:
            store = self._serving_store(self.router.side_of(key))
            store.apply(key, value, version)
            if self.router.keep_parent_sync:
                self.parent.apply(key, value, version)
        else:  # pragma: no cover
            raise AssertionError("未知路由模式 %r" % mode)

    def delete(self, key: str, version: int,
               client_version: Optional[int] = None) -> None:
        self.write(key, TOMBSTONE, version, client_version)

    def read_record(self, key: str,
                    client_version: Optional[int] = None) -> Optional[Record]:
        if client_version is None:
            client_version = self.router.version
        self.router.fence(client_version)
        mode = self.router.mode
        if mode in (MODE_SINGLE_PARENT, MODE_MIRRORING):
            return self.parent.get_record(key)
        # split：读子分片；双读状态下与父副本比对
        store = self._serving_store(self.router.side_of(key))
        record = store.get_record(key)
        if self.router.keep_parent_sync:
            parent_record = self.parent.get_record(key)
            if record != parent_record:
                raise ConsistencyError(
                    "双读不一致 key=%r child=%r parent=%r"
                    % (key, record, parent_record)
                )
        return record

    # ---- 阶段执行 ----

    def run_prepare(self) -> None:
        self.completed.append(PHASE_PREPARE)

    def run_mirror(self) -> None:
        self.router.mode = MODE_MIRRORING
        self.completed.append(PHASE_MIRROR)

    def run_backfill(self, fail_on_key: Optional[str] = None) -> None:
        """历史回填：按版本去重（不覆盖更高版本的实时写入），tombstone 一并复制。"""
        try:
            for key in sorted(self.parent.data):
                if fail_on_key is not None and key == fail_on_key:
                    raise BackfillError("回填在 key=%r 处失败" % key)
                record = self.parent.data[key]
                child = self.children[self.router.side_of(key)]
                child.apply(key, record.value, record.version)
        except Exception:
            self.rollback(PHASE_BACKFILL)
            raise
        self.completed.append(PHASE_BACKFILL)

    def run_double_read(self) -> None:
        """按计划中的校验样本比对父分片与对应子分片。"""
        try:
            for sample in self.plan.validation_samples:
                parent_record = self.parent.get_record(sample.key)
                child = self.children[self.router.side_of(sample.key)]
                child_record = child.get_record(sample.key)
                if parent_record != child_record:
                    raise ConsistencyError(
                        "样本 %s key=%r 不一致: parent=%r child=%r"
                        % (sample.sample_id, sample.key, parent_record, child_record)
                    )
        except Exception:
            self.rollback(PHASE_DOUBLE_READ)
            raise
        self.completed.append(PHASE_DOUBLE_READ)

    def run_route_switch(self) -> None:
        self.router.version += 1
        self.router.mode = MODE_SPLIT
        self.router.keep_parent_sync = True  # 保留双读状态直到 shrink
        self.completed.append(PHASE_ROUTE_SWITCH)

    def run_shrink(self) -> None:
        try:
            if self.inflight_old_version_writes > 0:
                raise ShrinkError(
                    "仍有 %d 个旧版本在途写入，禁止收缩父范围"
                    % self.inflight_old_version_writes
                )
            split = self.plan.split_point
            _, parent_end = self.request.parent_range
            self.parent.delete_range(split, parent_end)
            self.children["left"] = None  # 左半由父分片继续服务
            self.router.keep_parent_sync = False
        except Exception:
            self.rollback(PHASE_SHRINK)
            raise
        self.completed.append(PHASE_SHRINK)

    def run_all(self) -> None:
        self.run_prepare()
        self.run_mirror()
        self.run_backfill()
        self.run_double_read()
        self.run_route_switch()
        self.run_shrink()

    # ---- 回滚 ----

    def rollback(self, failed_phase: str) -> None:
        point = self.plan.rollback_for(failed_phase)
        if point is None:
            raise AssertionError("阶段 %r 无回滚点" % failed_phase)
        self.rolled_back_at = failed_phase
        if point.action == ROLLBACK_ABORT_SINGLE_PARENT:
            self.router.mode = MODE_SINGLE_PARENT
            self.children = {"left": None, "right": None}
        elif point.action == ROLLBACK_KEEP_MIRRORING:
            self.router.mode = MODE_MIRRORING
        elif point.action == ROLLBACK_KEEP_DOUBLE_READ:
            self.router.mode = MODE_SPLIT
            self.router.keep_parent_sync = True
        else:  # pragma: no cover
            raise AssertionError("未知回滚动作 %r" % point.action)
