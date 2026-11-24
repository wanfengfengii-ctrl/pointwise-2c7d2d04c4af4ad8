"""Inputs and a V1 check for the station-review example."""
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support import revision
from warehouse.fork.handoff import fork
from warehouse.ledger.bootstrap import initialize
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


def main():
    for same_value in (False, True):
        with TemporaryDirectory(prefix='station-review-fixture-') as directory:
            root = Path(directory)
            check_fixture(root / 'relay.sqlite', root / 'ledger.sqlite', same_value=same_value)


if __name__ == '__main__':
    main()
