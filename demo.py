import json
from pathlib import Path
from tempfile import TemporaryDirectory

from station_review_sample import make_batches
from warehouse.fork.handoff import fork, fork_status
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.ledger.post import post
from warehouse.relay.stream import relay_transaction
from warehouse.retention.reclaim import reclaim
from warehouse.source.publish import publish


def lose_admission_reply(boundary):
    if boundary == 'after_fork_admission':
        raise TimeoutError('值班台未收到中继受理回包')


def run(relay, ledger, batches):
    main = initialize(relay, ledger)
    original = publish(relay, batches[0])
    first = post(relay, ledger, main).checkpoint
    waiting = fork(relay, ledger, 'R', main, first.checkpoint_id, boundary=lose_admission_reply)
    assert waiting.status == 'waiting' and waiting.receipt is None, 'FORK_CONTINUITY: waiting receipt'
    later = [publish(relay, batch) for batch in batches[1:]]
    main_totals = [first.total_liters]
    main_totals.extend(post(relay, ledger, main).checkpoint.total_liters for _ in later)
    before = reclaim(relay, 3)
    assert before.deleted == ('B1',) and before.protected == ('B2', 'B3'), 'FORK_CONTINUITY: retention'

    initialize(relay, ledger)
    ready = fork(relay, ledger, 'R', main, first.checkpoint_id)
    assert ready.status == 'ready', 'FORK_CONTINUITY: recovery'
    review = ready.receipt.review_subscription
    start = read(ledger, review, ready.receipt.initial_checkpoint)
    totals = [start.total_liters]
    evidence = []
    for _ in later:
        result = post(relay, ledger, review)
        totals.append(result.checkpoint.total_liters)
        delta = result.batch.deltas[0]
        evidence.append([result.batch.batch_id, delta.liters_delta, delta.before.origin.document,
                         delta.after.origin.document, delta.after.origin.row])
    after = reclaim(relay, 3)
    initialize(relay, ledger)
    with relay_transaction(relay, write=False) as connection:
        remaining = connection.execute('SELECT batch_id FROM relay_entries ORDER BY sequence').fetchall()
    final = read(ledger, review, 'C3')
    actual = {
        'published_deltas': [batch.deltas[0].liters_delta for batch in [original, *later]],
        'held_during_handoff': list(before.protected),
    }
    downstream = {
        'main_totals': main_totals, 'review_totals': totals, 'review_evidence': evidence,
        'released_after_confirmation': list(after.deleted), 'remaining_work': remaining,
        'historical_batch_ids': [batch.batch_id for batch in final.history],
        'same_receipt_on_reopen': fork_status(ledger, 'R') == ready,
    }
    expected = {'published_deltas': [7, 11, -4], 'held_during_handoff': ['B2', 'B3']}
    downstream_expected = {
        'main_totals': [7, 18, 14], 'review_totals': [7, 18, 14],
        'review_evidence': [['B2', 11, 'meter-B1', 'meter-B2', 2],
                            ['B3', -4, 'meter-B2', 'meter-B3', 3]],
        'released_after_confirmation': ['B2', 'B3'], 'remaining_work': [],
        'historical_batch_ids': ['B1', 'B2', 'B3'], 'same_receipt_on_reopen': True,
    }
    assert actual == expected, f'FORK_CONTINUITY: {actual!r}'
    assert downstream == downstream_expected, f'FORK_CONTINUITY: {downstream!r}'
    print(f'主订阅逐批账量 {main_totals} L，复核订阅逐批账量 {totals} L。')
    print(f'交班时保留 {before.protected}，复核确认后实删 {after.deleted}。')
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'v6',
        'input': {'batches': batches, 'request': 'R', 'checkpoint': 'C1',
                  'reply_lost_at': 'after_fork_admission'},
        'expected': expected, 'actual': actual,
        'downstream_expected': downstream_expected, 'downstream_actual': downstream,
        'code_paths': ['warehouse/source/publish.py', 'warehouse/relay/admit.py',
                       'warehouse/retention/reclaim.py', 'warehouse/fork/recovery.py',
                       'warehouse/ledger/post.py'],
        'oracle': '按输入手算 7−0、18−7、14−18。复核确认前两条原批应保留，确认后查工作表实删。',
    }, ensure_ascii=False, separators=(',', ':')))


def run_station(relay, ledger):
    batches = make_batches(same_value=False)
    main = initialize(relay, ledger)
    for batch in batches:
        publish(relay, batch)
    main_totals = [post(relay, ledger, main).checkpoint.total_liters for _ in range(2)]
    reclaimed = reclaim(relay, 2)
    opened = fork(relay, ledger, 'SA', main, 'C1', station='A')
    assert opened.status == 'ready', 'STATION_REVIEW: opening'
    review = opened.receipt.review_subscription
    start = read(ledger, review, opened.receipt.initial_checkpoint)
    main_totals.append(post(relay, ledger, main).checkpoint.total_liters)
    held = reclaim(relay, 3)
    progressed = post(relay, ledger, review)
    released = reclaim(relay, 3)
    initialize(relay, ledger)
    final = read(ledger, review, 'C3')
    actual = {
        'main_totals': main_totals, 'reclaimed_before_fork': list(reclaimed.deleted),
        'station_start': [start.total_liters, [batch.batch_id for batch in start.history]],
        'held_before_station_ack': list(held.protected), 'station_totals': [start.total_liters,
                                                                          progressed.checkpoint.total_liters],
        'station_batch': [progressed.batch.batch_id, progressed.batch.sequence],
        'station_entries': [[entry.after.station, entry.liters_delta, entry.after.origin.row]
                            for entry in progressed.batch.deltas],
        'skipped': [[sequence, batch_id, list(stations)]
                    for sequence, batch_id, stations in progressed.checkpoint.skipped],
        'released_after_station_ack': list(released.deleted),
        'history': [[batch.batch_id, [entry.after.station for entry in batch.deltas]]
                    for batch in final.history],
    }
    expected = {
        'main_totals': [17, 19, 35], 'reclaimed_before_fork': ['B1', 'B2'],
        'station_start': [7, ['B1']], 'held_before_station_ack': ['B3'],
        'station_totals': [7, 18], 'station_batch': ['B3', 3],
        'station_entries': [['A', 6, 1], ['A', 5, 3]],
        'skipped': [[2, 'B2', ['B']]], 'released_after_station_ack': ['B3'],
        'history': [['B1', ['A']], ['B3', ['A', 'A']]],
    }
    assert actual == expected, f'STATION_REVIEW: {actual!r}'
    print(f'甲站从 C1 的 7 L 起，跨过 {progressed.checkpoint.skipped}，'
          f'B3 落账到 {progressed.checkpoint.total_liters} L。')
    print(f'甲站确认前保留 {held.protected}，确认后实删 {released.deleted}。')
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'station-v1',
        'input': {'batches': batches, 'request': 'SA', 'checkpoint': 'C1',
                  'station': 'A', 'reclaimed_through': 2},
        'expected': expected, 'actual': actual,
        'code_paths': ['warehouse/fork/handoff.py', 'warehouse/relay/windows.py',
                       'warehouse/retention/eligibility.py', 'warehouse/ledger/post.py',
                       'warehouse/ledger/checkpoints.py'],
        'oracle': 'B2 只涉乙站，已回收仍可按正式档案跨过；B3 本站分录为 13−7=6、18−13=5，'
                  '合计 18 L，原第 1、3 行次序与出处保留。',
    }, ensure_ascii=False, separators=(',', ':')))


def main():
    sample = Path(__file__).with_name('sample-batches.json')
    batches = json.loads(sample.read_text(encoding='utf-8'))
    with TemporaryDirectory(prefix='chenghe-metering-') as directory:
        workspace = Path(directory)
        run(workspace / 'relay.sqlite', workspace / 'ledger.sqlite', batches)
    with TemporaryDirectory(prefix='chenghe-station-') as directory:
        workspace = Path(directory)
        run_station(workspace / 'relay.sqlite', workspace / 'ledger.sqlite')


if __name__ == '__main__':
    main()
