from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.relay.entries import list_entries_through
from warehouse.relay.stream import read_stream_bounds
from warehouse.retention.claims import list_obligations
from warehouse.source.stations import concerns


@dataclass(frozen=True)
class ReclaimPlan:
    previous_boundary: int
    delete_through: int
    protected: tuple[str, ...]


def _needed_by(entry, obligations) -> bool:
    for obligation in obligations:
        if entry.sequence <= obligation.confirmed_through:
            continue
        if obligation.station is None or concerns(entry.deltas, obligation.station):
            return True
    return False


def plan_reclaim(connection: Connection, through: int | None) -> ReclaimPlan:
    if through is not None and (type(through) is not int or through < 0):
        raise ValueError('reclaim limit must be a nonnegative sequence')
    bounds = read_stream_bounds(connection)
    obligations = list_obligations(connection)
    if not obligations:
        raise RuntimeError('relay has no primary obligation')
    target = bounds.published_through if through is None else min(through, bounds.published_through)
    if bounds.reclaimed_through > target:
        target = bounds.reclaimed_through
    for obligation in obligations:
        if obligation.station is None and obligation.confirmed_through < bounds.reclaimed_through:
            raise RuntimeError('a subscription is missing a previously reclaimed prefix')

    entries = list_entries_through(connection, target)
    delete_through = bounds.reclaimed_through
    for entry in entries:
        if entry.sequence <= delete_through:
            continue
        if _needed_by(entry, obligations):
            break
        delete_through = entry.sequence
    protected = tuple(entry.batch_id for entry in entries if entry.sequence > delete_through)
    return ReclaimPlan(bounds.reclaimed_through, delete_through, protected)
