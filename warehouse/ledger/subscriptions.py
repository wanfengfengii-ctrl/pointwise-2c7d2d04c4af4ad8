from dataclasses import dataclass
from sqlite3 import Connection

from warehouse.ledger.balances import apply_deltas, save_balances
from warehouse.ledger.checkpoints import (Checkpoint, load_checkpoint, save_checkpoint,
                                         seed_checkpoint_prefix)
from warehouse.ledger.history import (PostedBatch, append_posted_batch, seed_history,
                                      read_history_prefix)
from warehouse.source.stations import filter_deltas


@dataclass(frozen=True)
class LedgerSubscription:
    subscription: str
    kind: str
    sequence: int
    checkpoint_id: str
    station: str | None = None


def initialize_subscriptions(connection: Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('primary', 'review')),
            sequence INTEGER NOT NULL CHECK (sequence >= 0), checkpoint_id TEXT NOT NULL,
            station TEXT
        )
    ''')
    connection.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS one_primary_ledger
        ON subscriptions (kind) WHERE kind = 'primary'
    ''')


def find_subscription(connection: Connection, subscription: str) -> LedgerSubscription | None:
    row = connection.execute('''
        SELECT subscription, kind, sequence, checkpoint_id, station
        FROM subscriptions WHERE subscription = ?
    ''', (subscription,)).fetchone()
    return None if row is None else LedgerSubscription(*row)


def ensure_primary_ledger(connection: Connection, subscription: str) -> LedgerSubscription:
    row = connection.execute('''
        SELECT subscription, kind, sequence, checkpoint_id, station
        FROM subscriptions WHERE kind = 'primary'
    ''').fetchone()
    if row is not None:
        current = LedgerSubscription(*row)
        if current.subscription != subscription:
            raise ValueError(f'ledger already belongs to primary {current.subscription!r}')
        return current
    connection.execute('''
        INSERT INTO subscriptions (subscription, kind, sequence, checkpoint_id, station)
        VALUES (?, 'primary', 0, 'C0', NULL)
    ''', (subscription,))
    save_checkpoint(connection, Checkpoint(subscription, 'C0', 0, None, ()))
    return LedgerSubscription(subscription, 'primary', 0, 'C0', None)


def _seed_station_review(connection: Connection, subscription: str, source: Checkpoint,
                         station: str) -> LedgerSubscription:
    kept_batches = []
    for posted in read_history_prefix(connection, source.subscription, source.sequence):
        kept = filter_deltas(posted.deltas, station)
        if kept:
            kept_batches.append((posted.sequence, posted.batch_id, kept))
    inherited = connection.execute('''
        SELECT checkpoint_id, sequence FROM checkpoints
        WHERE subscription = ? AND sequence <= ? ORDER BY sequence
    ''', (source.subscription, source.sequence)).fetchall()
    if not inherited or inherited[-1][0] != source.checkpoint_id:
        inherited.append((source.checkpoint_id, source.sequence))
    for checkpoint_id, sequence in inherited:
        running: tuple = ()
        last_batch_id = None
        for posted_sequence, batch_id, kept in kept_batches:
            if posted_sequence > sequence:
                break
            running = apply_deltas(running, kept)
            last_batch_id = batch_id
        save_checkpoint(connection, Checkpoint(subscription, checkpoint_id, sequence,
                                               last_batch_id, running))
    for sequence, batch_id, kept in kept_batches:
        append_posted_batch(connection, PostedBatch(subscription, sequence, batch_id,
                                                    f'C{sequence}', kept))
    head = load_checkpoint(connection, subscription, source.checkpoint_id)
    save_balances(connection, subscription, head.balances)
    return LedgerSubscription(subscription, 'review', source.sequence, source.checkpoint_id,
                              station)


def seed_subscription(connection: Connection, subscription: str, source: Checkpoint,
                      station: str | None = None) -> LedgerSubscription:
    connection.execute('''
        INSERT INTO subscriptions (subscription, kind, sequence, checkpoint_id, station)
        VALUES (?, 'review', ?, ?, ?)
    ''', (subscription, source.sequence, source.checkpoint_id, station))
    if station is not None:
        return _seed_station_review(connection, subscription, source, station)
    seed_checkpoint_prefix(connection, source, subscription)
    seed_history(connection, source.subscription, subscription, source.sequence)
    save_balances(connection, subscription, source.balances)
    return LedgerSubscription(subscription, 'review', source.sequence, source.checkpoint_id,
                              None)


def advance_subscription(connection: Connection, subscription: LedgerSubscription,
                         checkpoint: Checkpoint, *, contiguous: bool = True) -> None:
    if checkpoint.subscription != subscription.subscription:
        raise ValueError('checkpoint does not belong to the subscription')
    if contiguous:
        if checkpoint.sequence != subscription.sequence + 1:
            raise ValueError('checkpoint does not follow the subscription head')
    elif checkpoint.sequence <= subscription.sequence:
        raise ValueError('checkpoint does not advance the subscription head')
    changed = connection.execute('''
        UPDATE subscriptions SET sequence = ?, checkpoint_id = ?
        WHERE subscription = ? AND sequence = ?
    ''', (checkpoint.sequence, checkpoint.checkpoint_id, subscription.subscription,
          subscription.sequence))
    if changed.rowcount != 1:
        raise RuntimeError('ledger head changed within the posting transaction')
