from dataclasses import dataclass

from warehouse.ledger.history import find_posted_batch
from warehouse.ledger.store import ledger_transaction
from warehouse.relay.entries import load_entry
from warehouse.relay.stream import relay_transaction
from warehouse.relay.windows import related_sequences
from warehouse.retention.claims import advance_obligation, find_obligation
from warehouse.source.stations import filter_deltas


@dataclass(frozen=True)
class AckResult:
    subscription: str
    confirmed_through: int
    advanced: bool


def _require_posting(ledger, subscription: str, batch_id: str, sequence: int):
    with ledger_transaction(ledger, write=False) as connection:
        posted = find_posted_batch(connection, subscription, batch_id)
    if posted is None or posted.sequence != sequence:
        raise ValueError('acknowledgment requires the matching committed posting')
    return posted


def _acknowledge_station(connection, obligation, posted, batch_id: str, sequence: int):
    station = obligation.station
    if sequence <= obligation.confirmed_through:
        return AckResult(obligation.subscription, obligation.confirmed_through, False)
    related = related_sequences(connection, station, obligation.confirmed_through, sequence)
    if related != (sequence,):
        raise ValueError('acknowledgment would skip a related batch')
    entry = load_entry(connection, sequence)
    if entry is None or entry.batch_id != batch_id:
        raise ValueError('committed posting does not match the next relay batch')
    if filter_deltas(entry.deltas, station) != posted.deltas:
        raise ValueError('committed posting does not match the next relay batch')
    advance_obligation(connection, obligation, sequence, contiguous=False)
    return AckResult(obligation.subscription, sequence, True)


def acknowledge_posted(relay, ledger, subscription: str, batch_id: str,
                       sequence: int) -> AckResult:
    # A committed history row is immutable and can be checked before taking the relay write lock.
    posted = _require_posting(ledger, subscription, batch_id, sequence)
    with relay_transaction(relay) as connection:
        obligation = find_obligation(connection, subscription)
        if obligation is None:
            raise KeyError(f'unknown relay subscription {subscription!r}')
        if obligation.station is not None:
            return _acknowledge_station(connection, obligation, posted, batch_id, sequence)
        if sequence <= obligation.confirmed_through:
            return AckResult(subscription, obligation.confirmed_through, False)
        if sequence != obligation.confirmed_through + 1:
            raise ValueError('acknowledgment would skip a batch')
        entry = load_entry(connection, sequence)
        if entry is None or (entry.batch_id, entry.deltas) != (batch_id, posted.deltas):
            raise ValueError('committed posting does not match the next relay batch')
        advance_obligation(connection, obligation, sequence)
    return AckResult(subscription, sequence, True)
