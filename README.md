# storage-shard-split-consistency-planner
规划在线分片分裂、路由切换和一致性校验

## 概述

针对在线分片分裂期间的一致性问题：父分片在分裂时仍接收靠近边界的写入，
如果路由直接切到子分片而没有双读或 fencing，边界键可能在两侧出现不同版本。
本规划器根据父分片范围、目标分裂点、键分布、当前路由版本、写入速率、
副本位置和复制延迟，输出完整的分裂计划。

**规划器不直接迁移数据。** 它只产出阶段顺序、每阶段路由版本、校验样本和
回滚点；实际的数据搬运由存储引擎按计划执行。仓库中的 `PlanExecutor`
是内存模拟执行器，仅用于验证计划的一致性语义。

## 阶段顺序

1. `prepare_children` — 创建左右子范围并分配副本
2. `mirror_writes` — 镜像双写，窗口覆盖复制延迟与全部边界键
3. `backfill` — 历史回填，按版本 last-write-wins 去重，tombstone 一并复制
4. `double_read` — 按校验样本（边界 + 热点 + 等距抽样）比对父子两侧
5. `route_switch` — 路由版本 +1，旧版本请求被 fencing 拒绝而非静默接受
6. `shrink_parent` — 确认无旧版本在途写入后收缩父范围

## 一致性保证

- 切换前镜像窗口覆盖所有边界写入（含热点边界键）
- 携带旧路由版本的晚到请求触发 `StaleRouteError`，不会被新分片静默接受
- 回填与实时写入按键版本去重，删除与 tombstone 不丢失、不复活
- 失败回滚：切换前失败恢复单父范围或保持镜像双写；切换后失败保留双读状态

## 使用

```python
from shard_split_planner import KeyWeight, SplitPlanner, SplitRequest

request = SplitRequest(
    parent_range=("a", "z"),
    split_point="k050",
    key_distribution=[KeyWeight("k049", 500.0), KeyWeight("k050", 800.0)],
    current_version=7,
    write_rate=1000.0,
    replica_locations=["az1", "az2", "az3", "az4"],
    replication_lag_ms=50.0,
)
plan = SplitPlanner().plan(request)
# plan.phases / plan.validation_samples / plan.rollback_for(...) / plan.mirror_window_ms
```

## 测试

```
python -m unittest
```

覆盖热点边界、晚到写入 fencing、重复键去重、删除与 tombstone、
回填失败与收缩失败回滚。兼容 Python 3.8+。
