"""Station projections for mixed batches.

A batch concerns a station when at least one of its formal deltas belongs to
that station.  Station reviews keep only their own delta subsequence, in the
original in-batch order.
"""
from warehouse.source.deltas import DeltaEntry


def batch_stations(deltas: tuple[DeltaEntry, ...]) -> tuple[str, ...]:
    return tuple(sorted({entry.after.station for entry in deltas}))


def filter_deltas(deltas: tuple[DeltaEntry, ...], station: str) -> tuple[DeltaEntry, ...]:
    return tuple(entry for entry in deltas if entry.after.station == station)


def concerns(deltas: tuple[DeltaEntry, ...], station: str) -> bool:
    return any(entry.after.station == station for entry in deltas)
