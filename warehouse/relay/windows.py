from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.relay.entries import load_entry
from warehouse.relay.stream import read_stream_bounds
from warehouse.retention.claims import find_obligation
from warehouse.source.batches import find_published_sequence
from warehouse.source.stations import batch_stations, concerns


@dataclass(frozen=True)
class DeliveryWindow:
    confirmed_through: int
    published_through: int
    next_sequence: int | None


def batch_at(connection: Connection, sequence: int):
    """Return ``(batch_id, deltas)`` from the work record or the formal archive."""
    entry = load_entry(connection, sequence)
    if entry is not None:
        return entry.batch_id, entry.deltas
    batch = find_published_sequence(connection, sequence)
    if batch is None:
        return None
    return batch.batch_id, batch.deltas


def related_sequences(connection: Connection, station: str, after: int,
                      through: int) -> tuple[int, ...]:
    found = []
    for sequence in range(after + 1, through + 1):
        batch = batch_at(connection, sequence)
        if batch is None:
            raise RuntimeError(f'missing relay record at sequence {sequence}')
        if concerns(batch[1], station):
            found.append(sequence)
    return tuple(found)


def crossed_batches(connection: Connection,
                    after: int, through: int) -> tuple[tuple[int, str, tuple[str, ...]], ...]:
    crossed = []
    for sequence in range(after + 1, through):
        batch = batch_at(connection, sequence)
        if batch is None:
            raise RuntimeError(f'missing relay record at sequence {sequence}')
        crossed.append((sequence, batch[0], batch_stations(batch[1])))
    return tuple(crossed)


def next_delivery_window(connection: Connection, subscription: str) -> DeliveryWindow:
    obligation = find_obligation(connection, subscription)
    if obligation is None:
        raise KeyError(f'unknown relay subscription {subscription!r}')
    bounds = read_stream_bounds(connection)
    confirmed = obligation.confirmed_through
    if obligation.station is None:
        if not bounds.reclaimed_through <= confirmed <= bounds.published_through:
            raise RuntimeError(f'confirmation is outside the relay window for {subscription!r}')
        target = None if confirmed == bounds.published_through else confirmed + 1
        return DeliveryWindow(confirmed, bounds.published_through, target)
    if not 0 <= confirmed <= bounds.published_through:
        raise RuntimeError(f'confirmation is outside the relay window for {subscription!r}')
    related = related_sequences(connection, obligation.station, confirmed,
                                bounds.published_through)
    target = related[0] if related else None
    return DeliveryWindow(confirmed, bounds.published_through, target)


def admission_window(connection: Connection, sequence: int,
                     station: str | None = None) -> DeliveryWindow | None:
    bounds = read_stream_bounds(connection)
    if type(sequence) is not int or not 0 <= sequence <= bounds.published_through:
        raise ValueError('specified checkpoint is outside the published stream')
    if station is None:
        if sequence < bounds.reclaimed_through:
            return None
        target = None if sequence == bounds.published_through else sequence + 1
        return DeliveryWindow(sequence, bounds.published_through, target)
    # A station review expires only when a work batch with a formal entry for
    # this station was physically deleted after the starting checkpoint.
    # Deleted batches holding only foreign entries stay resolvable through the
    # formal archive and may be crossed.
    if sequence < bounds.reclaimed_through:
        deleted_related = related_sequences(connection, station, sequence,
                                            bounds.reclaimed_through)
        if deleted_related:
            return None
    related = related_sequences(connection, station, sequence, bounds.published_through)
    target = related[0] if related else None
    return DeliveryWindow(sequence, bounds.published_through, target)
