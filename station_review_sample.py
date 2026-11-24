"""Inputs and checks for the station-review example.

The original full-subscription fixture is kept verbatim; the station checks
exercise opening a station review from a reclaimed prefix, expiry, interrupted
admission, lost posting acknowledgement and evidence read-back without the
relay database.
"""
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support import revision
from warehouse.fork.handoff import fork, fork_status
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.ledger.post import post
from warehouse.relay.delivery import next_delivery
from warehouse.relay.stream import relay_transaction
from warehouse.retention.reclaim import reclaim
from warehouse.source.batches import find_published_batch
from warehouse.source.publish import publish


def make_batches(*, same_value=False):
    first = [revision('a1', 7), revision('b1', 10, station='B', row=2)]
    second = [revision('b2', 12, station='B', previous='b1')]
    a_previous = 'a1'
    if same_value:
        second.append(revision('a2', 7, previous='a1', row=2))
        a_previous = 'a2'
    third = [revision('a3-first', 13, previous=a_previous),
             revision('b3', 17, station='B', previous='b2', row=2),
             revision('a3-last', 18, previous='a3-first', row=3)]
    return [{'batch_id': f'B{i}', 'revisions': rows}
            for i, rows in enumerate((first, second, third), start=1)]


def _work_ids(relay):
    with relay_transaction(relay, write=False) as connection:
        return tuple(row[0] for row in
                     connection.execute('SELECT batch_id FROM relay_entries ORDER BY sequence'))


def check_fixture(relay, ledger, *, same_value):
    primary = initialize(relay, ledger)
    for batch in make_batches(same_value=same_value):
        publish(relay, batch)
    assert [post(relay, ledger, primary).checkpoint.total_liters for _ in range(2)] == [17, 19]
    removed = reclaim(relay, 2)
    assert removed.deleted == ('B1', 'B2')
    with relay_transaction(relay, write=False) as connection:
        remaining = connection.execute('SELECT batch_id FROM relay_entries ORDER BY sequence').fetchall()
        archived, _ = find_published_batch(connection, 'B2')
    actual = [(item.after.station, item.liters_delta) for item in archived.deltas]
    assert remaining == [('B3',)]
    assert actual == ([('B', 2), ('A', 0)] if same_value else [('B', 2)])
    old = fork(relay, ledger, 'full', primary, 'C1')
    assert (old.status, old.receipt) == ('expired', None)
    offered = next_delivery(relay, primary).entry
    assert [(item.after.station, item.liters_delta) for item in offered.deltas] == [('A', 6), ('B', 5), ('A', 5)]
    final = post(relay, ledger, primary, delivery=offered).checkpoint
    assert (final.checkpoint_id, final.total_liters) == ('C3', 35)
    print(f'B2 正式分录 {actual}，实际回收 {removed.deleted}，全量交班 {old.status}，主账 C3 {final.total_liters} L')


def check_station_fixture(relay, ledger):
    primary = initialize(relay, ledger)
    for batch in make_batches(same_value=False):
        publish(relay, batch)
    assert [post(relay, ledger, primary).checkpoint.total_liters for _ in range(2)] == [17, 19]
    assert reclaim(relay, 2).deleted == ('B1', 'B2')
    assert _work_ids(relay) == ('B3',)

    opened = fork(relay, ledger, 'SA', primary, 'C1', station='A')
    assert opened.status == 'ready' and opened.receipt.station == 'A'
    review = opened.receipt.review_subscription
    start = read(ledger, review, opened.receipt.initial_checkpoint)
    assert (start.total_liters, start.skipped) == (7, ())
    assert [batch.batch_id for batch in start.history] == ['B1']
    assert [(entry.after.station, entry.liters_delta)
            for batch in start.history for entry in batch.deltas] == [('A', 7)]

    assert post(relay, ledger, primary).checkpoint.total_liters == 35
    held = reclaim(relay, 3)
    assert (held.deleted, held.protected) == ((), ('B3',))
    posted = post(relay, ledger, review)
    assert posted.status == 'posted'
    assert (posted.checkpoint.checkpoint_id, posted.checkpoint.total_liters) == ('C3', 18)
    assert posted.checkpoint.skipped == ((2, 'B2', ('B',)),)
    assert [(entry.after.station, entry.liters_delta) for entry in posted.batch.deltas] == [('A', 6), ('A', 5)]
    released = reclaim(relay, 3)
    assert (released.deleted, released.protected) == (('B3',), ())
    assert _work_ids(relay) == ()
    print(f'甲站从 C1 起 {start.total_liters} L，跳过 {posted.checkpoint.skipped}，'
          f'落账 {posted.checkpoint.total_liters} L，确认前保留 {held.protected}，'
          f'确认后实删 {released.deleted}')

    os.remove(relay)
    final = read(ledger, review, 'C3')
    assert (final.total_liters, final.skipped) == (18, ((2, 'B2', ('B',)),))
    assert [(batch.batch_id, [(entry.after.station, entry.after.origin.row)
                              for entry in batch.deltas]) for batch in final.history] == [
        ('B1', [('A', 1)]), ('B3', [('A', 1), ('A', 3)])]
    print('断开中继后仅凭核账库读回本站 18 L 及跨序依据')
    return final


def check_station_expiry(relay, ledger):
    primary = initialize(relay, ledger)
    for batch in make_batches(same_value=True):
        publish(relay, batch)
    assert [post(relay, ledger, primary).checkpoint.total_liters for _ in range(2)] == [17, 19]
    assert reclaim(relay, 2).deleted == ('B1', 'B2')
    expired = fork(relay, ledger, 'SA', primary, 'C1', station='A')
    assert (expired.status, expired.receipt) == ('expired', None)
    initialize(relay, ledger)
    assert fork(relay, ledger, 'SA', primary, 'C1', station='A') == expired
    assert fork_status(ledger, 'SA') == expired
    print('同值换版含甲站分录，B2 已实删，甲站交班 expired')


def check_station_lost_ack(relay, ledger):
    primary = initialize(relay, ledger)
    published = make_batches(same_value=False)
    publish(relay, published[0])
    post(relay, ledger, primary)

    def drop(point):
        if point == 'after_fork_admission':
            raise TimeoutError('admission reply lost')

    early = fork(relay, ledger, 'SA', primary, 'C1', station='A', boundary=drop)
    assert early.status == 'waiting'
    for batch in published[1:]:
        publish(relay, batch)
    assert [post(relay, ledger, primary).checkpoint.total_liters for _ in range(2)] == [19, 35]
    held = reclaim(relay, 3)
    assert (held.deleted, held.protected) == (('B1', 'B2'), ('B3',))
    ready = fork(relay, ledger, 'SA', primary, 'C1', station='A')
    assert ready.status == 'ready'
    review = ready.receipt.review_subscription

    def lose_commit(point):
        if point == 'after_post_commit':
            raise TimeoutError('posting reply lost')

    try:
        post(relay, ledger, review, boundary=lose_commit)
    except TimeoutError:
        pass
    else:
        raise AssertionError('posting acknowledgement should have been lost')
    kept = reclaim(relay, 3)
    assert (kept.deleted, kept.protected) == ((), ('B3',))
    replayed = post(relay, ledger, review)
    assert (replayed.status, replayed.checkpoint.total_liters) == ('replayed', 18)
    assert replayed.checkpoint.skipped == ((2, 'B2', ('B',)),)
    released = reclaim(relay, 3)
    assert (released.deleted, released.protected) == (('B3',), ())
    print(f'受理回包丢失后保留 {held.protected}，落账确认丢失后保留 {kept.protected}，'
          f'复领后实删 {released.deleted}')


def main():
    for same_value in (False, True):
        with TemporaryDirectory(prefix='station-review-fixture-') as directory:
            root = Path(directory)
            check_fixture(root / 'relay.sqlite', root / 'ledger.sqlite', same_value=same_value)
    with TemporaryDirectory(prefix='station-review-open-') as directory:
        root = Path(directory)
        check_station_fixture(root / 'relay.sqlite', root / 'ledger.sqlite')
    with TemporaryDirectory(prefix='station-review-expired-') as directory:
        root = Path(directory)
        check_station_expiry(root / 'relay.sqlite', root / 'ledger.sqlite')
    with TemporaryDirectory(prefix='station-review-lost-') as directory:
        root = Path(directory)
        check_station_lost_ack(root / 'relay.sqlite', root / 'ledger.sqlite')


if __name__ == '__main__':
    main()
