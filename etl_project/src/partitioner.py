import hashlib
from typing import Optional


def default_partitioner(key: Optional[bytes], all_partitions: list[int], available_partitions: list[int]) -> int:
    """
    Deterministic partitioner compatible with kafka-python's partitioner interface.

    - If key is provided, route consistently by hashing the key.
    - If key is None, fall back to the first available partition for stability.
    """
    if not available_partitions:
        # Safety: if none reported available, use first partition
        return 0

    if key is None:
        return available_partitions[0]

    key_hash = int.from_bytes(hashlib.sha256(key).digest()[:8], byteorder="big", signed=False)
    idx = key_hash % len(available_partitions)
    return available_partitions[idx]


def partition_by_customer_id(key: Optional[bytes], all_partitions: list[int], available_partitions: list[int]) -> int:
    """
    Example: partition by numeric customer id embedded in key bytes (utf-8 string).
    Expects key like b"customer:123" or just b"123".
    Falls back to default hashing when parsing fails.
    """
    if key is None:
        return default_partitioner(key, all_partitions, available_partitions)

    try:
        text = key.decode("utf-8")
        # Extract trailing integer if present
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            cid = int(digits)
            if not available_partitions:
                return 0
            return available_partitions[cid % len(available_partitions)]
    except Exception:
        pass

    return default_partitioner(key, all_partitions, available_partitions)


