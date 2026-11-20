from shard_split_planner import KeyWeight, SplitRequest


def make_request(**overrides):
    """构造默认请求：父范围 [a, z)，分裂点 k050，100 个键。"""
    distribution = [
        KeyWeight(key="k%03d" % i, writes_per_sec=float(i % 10))
        for i in range(100)
    ]
    params = dict(
        parent_range=("a", "z"),
        split_point="k050",
        key_distribution=distribution,
        current_version=7,
        write_rate=1000.0,
        replica_locations=["az1", "az2", "az3", "az4"],
        replication_lag_ms=50.0,
    )
    params.update(overrides)
    return SplitRequest(**params)
