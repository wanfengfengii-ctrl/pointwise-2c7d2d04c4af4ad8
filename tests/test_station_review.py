import os
from pathlib import Path

from station_review_sample import make_batches
from tests.support import WorkspaceTestCase, fail_at, revision
from warehouse.fork.handoff import fork, fork_status
from warehouse.ledger.bootstrap import initialize
from warehouse.ledger.checkpoints import read
from warehouse.ledger.post import post
from warehouse.ledger.subscriptions import find_subscription
from warehouse.ledger.store import ledger_transaction
from warehouse.relay.admit import find_admission
from warehouse.relay.delivery import next_delivery
from warehouse.relay.entries import list_entries_through
from warehouse.relay.stream import read_stream_bounds, relay_transaction
from warehouse.retention.claims import find_obligation
from warehouse.retention.reclaim import reclaim
from warehouse.source.publish import publish


class StationReviewTests(WorkspaceTestCase):
    def setup_station(self, *, same_value=False, reclaim_through=None):
        self.batches = make_batches(same_value=same_value)
        for batch in self.batches:
            publish(self.relay, batch)
        self.assertEqual([post(self.relay, self.ledger, self.main).checkpoint.total_liters
                          for _ in range(2)], [17, 19])
        if reclaim_through is not None:
            self.assertEqual(reclaim(self.relay, reclaim_through).deleted, ('B1', 'B2'))

    def open_station(self, station='A', request='SA', checkpoint='C1', **kwargs):
        return fork(self.relay, self.ledger, request, self.main, checkpoint,
                    station=station, **kwargs)

    def station_work(self):
        with relay_transaction(self.relay, write=False) as connection:
            return tuple(entry.batch_id for entry in list_entries_through(connection, 100))

    def test_reclaimed_foreign_archive_is_crossable(self):
        self.setup_station(reclaim_through=2)
        opened = self.open_station()
        self.assertEqual(opened.status, 'ready')
        receipt = opened.receipt
        self.assertEqual((receipt.source_checkpoint, receipt.station), ('C1', 'A'))
        start = read(self.ledger, receipt.review_subscription, receipt.initial_checkpoint)
        self.assertEqual((start.total_liters, start.skipped), (7, ()))
        self.assertEqual([batch.batch_id for batch in start.history], ['B1'])
        self.assertEqual([entry.after.station for batch in start.history
                          for entry in batch.deltas], ['A'])
        progressed = post(self.relay, self.ledger, receipt.review_subscription)
        self.assertEqual(progressed.status, 'posted')
        self.assertEqual(progressed.checkpoint.checkpoint_id, 'C3')
        self.assertEqual(progressed.checkpoint.total_liters, 18)
        self.assertEqual(progressed.checkpoint.skipped, ((2, 'B2', ('B',)),))
        self.assertEqual([(entry.after.station, entry.liters_delta)
                          for entry in progressed.batch.deltas], [('A', 6), ('A', 5)])
        final = read(self.ledger, receipt.review_subscription, 'C3')
        self.assertEqual(final.skipped, ((2, 'B2', ('B',)),))
        self.assertEqual([(batch.batch_id, [entry.after.origin.row for entry in batch.deltas])
                          for batch in final.history], [('B1', [1]), ('B3', [1, 3])])

    def test_station_entry_in_deleted_batch_expires(self):
        self.setup_station(same_value=True, reclaim_through=2)
        expired = self.open_station()
        self.assertEqual((expired.status, expired.receipt), ('expired', None))
        initialize(self.relay, self.ledger)
        self.assertEqual(self.open_station(), expired)
        self.assertEqual(fork_status(self.ledger, 'SA'), expired)
        with ledger_transaction(self.ledger, write=False) as connection:
            self.assertIsNone(find_subscription(connection, 'review:SA'))

    def test_mixed_batch_keeps_original_order(self):
        self.setup_station()
        review = self.open_station().receipt.review_subscription
        posted = post(self.relay, self.ledger, review)
        self.assertEqual((posted.batch.sequence, posted.batch.batch_id), (3, 'B3'))
        self.assertEqual([(entry.after.station, entry.liters_delta)
                          for entry in posted.batch.deltas], [('A', 6), ('A', 5)])
        self.assertEqual(posted.batch.checkpoint_id, 'C3')
        with ledger_transaction(self.ledger, write=False) as connection:
            self.assertEqual(read(self.ledger, review, 'C3').total_liters, 18)
            balances = connection.execute('''
                SELECT station FROM balances WHERE subscription = ?''', (review,)).fetchall()
            self.assertEqual(balances, [('A',)])

    def test_foreign_only_review_waits(self):
        self.setup_station()
        opened = self.open_station(station='C')
        self.assertEqual(opened.status, 'ready')
        review = opened.receipt.review_subscription
        start = read(self.ledger, review, 'C1')
        self.assertEqual((start.total_liters, start.history, start.skipped), (0, (), ()))
        self.assertEqual(post(self.relay, self.ledger, review).status, 'waiting')

    def test_pending_retains_related_batches_until_ack(self):
        self.setup_station()
        review = self.open_station().receipt.review_subscription
        post(self.relay, self.ledger, self.main)
        held = reclaim(self.relay, 3)
        self.assertEqual((held.deleted, held.protected), (('B1', 'B2'), ('B3',)))
        post(self.relay, self.ledger, review)
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B3',))
        self.assertEqual(self.station_work(), ())

    def test_reply_lost_after_post_commit(self):
        self.setup_station()
        review = self.open_station().receipt.review_subscription
        post(self.relay, self.ledger, self.main)
        with self.assertRaisesRegex(TimeoutError, 'after_post_commit'):
            post(self.relay, self.ledger, review, boundary=fail_at('after_post_commit'))
        committed = read(self.ledger, review, 'C3')
        self.assertEqual((committed.total_liters, committed.skipped), (18, ((2, 'B2', ('B',)),)))
        self.assertEqual(reclaim(self.relay, 3).protected, ('B3',))
        initialize(self.relay, self.ledger)
        recovered = post(self.relay, self.ledger, review)
        self.assertEqual((recovered.status, recovered.checkpoint.checkpoint_id),
                         ('replayed', 'C3'))
        self.assertTrue(recovered.acknowledgment.advanced)
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B3',))

    def test_disconnect_after_reclaim_still_reads(self):
        self.setup_station()
        review = self.open_station().receipt.review_subscription
        post(self.relay, self.ledger, self.main)
        post(self.relay, self.ledger, review)
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B1', 'B2', 'B3'))
        os.remove(self.relay)
        final = read(self.ledger, review, 'C3')
        self.assertEqual(final.total_liters, 18)
        self.assertEqual(final.skipped, ((2, 'B2', ('B',)),))
        self.assertEqual([(batch.batch_id, [entry.after.station for entry in batch.deltas])
                          for batch in final.history], [('B1', ['A']), ('B3', ['A', 'A'])])

    def test_admission_reply_lost_keeps_obligation(self):
        self.setup_station()
        result = self.open_station(boundary=fail_at('after_fork_admission'))
        self.assertEqual((result.status, result.receipt), ('waiting', None))
        with relay_transaction(self.relay, write=False) as connection:
            admitted = find_admission(connection, 'SA')
            self.assertEqual((admitted.status, admitted.station), ('admitted', 'A'))
            self.assertEqual(find_obligation(connection, 'review:SA').station, 'A')
        post(self.relay, self.ledger, self.main)
        self.assertEqual(reclaim(self.relay, 3).protected, ('B3',))
        ready = self.open_station()
        self.assertEqual(ready.status, 'ready')
        review = ready.receipt.review_subscription
        post(self.relay, self.ledger, review)
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B3',))

    def test_station_binding_is_fixed(self):
        self.setup_station()
        waiting = self.open_station(boundary=fail_at('after_fork_binding'))
        self.assertEqual(waiting.status, 'waiting')
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.open_station(station='B')
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.open_station(checkpoint='C2')
        ready = self.open_station()
        self.assertEqual(ready.status, 'ready')
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.open_station(station=None)
        self.assertEqual(fork_status(self.ledger, 'SA'), ready)

    def test_blank_station_rejected(self):
        self.setup_station()
        for value in ('', '   '):
            with self.assertRaisesRegex(ValueError, 'station'):
                self.open_station(station=value)

    def test_foreign_detail_not_in_station_balance(self):
        report = {'batch_id': 'mix', 'revisions': [
            revision('m1', 5, station='B'), revision('m2', 3, station='A', row=2),
            revision('m3', 9, station='B', previous='m1', row=3)]}
        publish(self.relay, report)
        post(self.relay, self.ledger, self.main)
        opened = fork(self.relay, self.ledger, 'SB', self.main, 'C0', station='A')
        self.assertEqual(opened.status, 'ready')
        review = opened.receipt.review_subscription
        posted = post(self.relay, self.ledger, review)
        self.assertEqual(posted.checkpoint.total_liters, 3)
        self.assertEqual([(entry.after.station, entry.liters_delta)
                          for entry in posted.batch.deltas], [('A', 3)])
        self.assertEqual(posted.checkpoint.skipped, ())

    def test_missing_and_zero_judged_by_station(self):
        self.publish_sample(1)
        self.run_post()
        report = {'batch_id': 'MIX', 'revisions': [
            revision('x1', 7, station='A', previous='r1', row=1),
            revision('x2', None, station='B', row=2),
            revision('x3', 0, station='A', previous='x1', row=3),
            revision('x4', 4, station='A', previous='x3', row=4)]}
        publish(self.relay, report)
        self.run_post()
        opened = fork(self.relay, self.ledger, 'SM', self.main, 'C1', station='A')
        self.assertEqual(opened.status, 'ready')
        review = opened.receipt.review_subscription
        posted = post(self.relay, self.ledger, review)
        self.assertEqual(posted.checkpoint.total_liters, 4)
        self.assertEqual([(entry.after.station, entry.after.state, entry.liters_delta)
                          for entry in posted.batch.deltas],
                         [('A', 'reading', 0), ('A', 'reading', -7), ('A', 'reading', 4)])
        self.assertEqual(posted.checkpoint.skipped, ())
        self.assertEqual([(item.station, item.liters)
                          for item in posted.checkpoint.balances], [('A', 4)])

        publish(self.relay, {'batch_id': 'O1', 'revisions': [
            revision('o1', 2, station='B', previous='x2', row=5)]})
        publish(self.relay, {'batch_id': 'O2', 'revisions': [
            revision('o2', 3, station='B', previous='o1', row=6)]})
        post(self.relay, self.ledger, self.main)
        post(self.relay, self.ledger, self.main)
        self.assertEqual(post(self.relay, self.ledger, review).status, 'waiting')
        late = {'batch_id': 'LATE', 'revisions': [
            revision('y1', 6, station='A', previous='x4', row=7)]}
        publish(self.relay, late)
        progressed = post(self.relay, self.ledger, review)
        self.assertEqual((progressed.status, progressed.checkpoint.total_liters),
                         ('posted', 6))
        self.assertEqual([skip[1] for skip in progressed.checkpoint.skipped],
                         ['O1', 'O2'])
        self.assertEqual([skip[2] for skip in progressed.checkpoint.skipped],
                         [('B',), ('B',)])

    def test_admission_lost_then_resume_keeps_b3(self):
        for batch in make_batches():
            publish(self.relay, batch)
        self.assertEqual(post(self.relay, self.ledger, self.main).checkpoint.total_liters, 17)
        early = fork(self.relay, self.ledger, 'SA', self.main, 'C1', station='A',
                     boundary=fail_at('after_fork_admission'))
        self.assertEqual(early.status, 'waiting')
        self.assertEqual([self.run_post().checkpoint.total_liters for _ in range(2)], [19, 35])
        held = reclaim(self.relay, 3)
        self.assertEqual((held.deleted, held.protected), (('B1', 'B2'), ('B3',)))
        ready = fork(self.relay, self.ledger, 'SA', self.main, 'C1', station='A')
        self.assertEqual(ready.status, 'ready')
        review = ready.receipt.review_subscription
        start = read(self.ledger, review, 'C1')
        self.assertEqual((start.total_liters, start.skipped), (7, ()))
        with self.assertRaisesRegex(TimeoutError, 'after_post_commit'):
            post(self.relay, self.ledger, review, boundary=fail_at('after_post_commit'))
        kept = reclaim(self.relay, 3)
        self.assertEqual((kept.deleted, kept.protected), ((), ('B3',)))
        replayed = post(self.relay, self.ledger, review)
        self.assertEqual((replayed.status, replayed.checkpoint.total_liters,
                          replayed.checkpoint.skipped), ('replayed', 18, ((2, 'B2', ('B',)),)))
        self.assertEqual(reclaim(self.relay, 3).deleted, ('B3',))
        self.assertEqual(self.station_work(), ())
        os.remove(self.relay)
        final = read(self.ledger, review, 'C3')
        self.assertEqual((final.total_liters, final.skipped), (18, ((2, 'B2', ('B',)),)))
        self.assertEqual([(batch.batch_id, batch.sequence,
                           [(entry.after.station, entry.after.origin.row)
                            for entry in batch.deltas]) for batch in final.history],
                         [('B1', 1, [('A', 1)]), ('B3', 3, [('A', 1), ('A', 3)])])

    def test_foreign_only_deleted_batch_archived_at_admission(self):
        self.setup_station(same_value=False)
        post(self.relay, self.ledger, self.main)
        self.assertEqual(reclaim(self.relay, 2).deleted, ('B1', 'B2'))
        opened = self.open_station()
        self.assertEqual(opened.status, 'ready')
        with relay_transaction(self.relay, write=False) as connection:
            self.assertEqual(tuple(entry.batch_id for entry in
                                   list_entries_through(connection, 100)), ('B3',))
        review = opened.receipt.review_subscription
        posted = post(self.relay, self.ledger, review)
        self.assertEqual((posted.status, posted.batch.batch_id), ('posted', 'B3'))
        self.assertEqual(posted.checkpoint.skipped, ((2, 'B2', ('B',)),))

    def test_review_related_batch_not_acknowledged_by_primary_only(self):
        self.setup_station()
        review = self.open_station().receipt.review_subscription
        post(self.relay, self.ledger, self.main)
        self.assertEqual(reclaim(self.relay, 3).protected, ('B3',))
        with relay_transaction(self.relay, write=False) as connection:
            obligation = find_obligation(connection, review)
        self.assertEqual((obligation.station, obligation.confirmed_through), ('A', 1))

    def test_inherited_old_checkpoints_trace_station_only(self):
        self.setup_station()
        opened = self.open_station()
        review = opened.receipt.review_subscription
        early = read(self.ledger, review, 'C1')
        self.assertEqual((early.total_liters, [batch.batch_id for batch in early.history]),
                         (7, ['B1']))
        self.assertEqual([(entry.after.station, entry.liters_delta)
                          for batch in early.history for entry in batch.deltas], [('A', 7)])
        self.assertEqual(early.skipped, ())
        initial = read(self.ledger, review, 'C0')
        self.assertEqual((initial.total_liters, initial.history), (0, ()))
        with self.assertRaisesRegex(KeyError, 'unknown checkpoint'):
            read(self.ledger, review, 'C2')

    def test_foreign_batches_skipped_by_sequence(self):
        self.setup_station()
        review = self.open_station().receipt.review_subscription
        post(self.relay, self.ledger, self.main)
        posted = post(self.relay, self.ledger, review)
        self.assertEqual([(item[0], item[1], item[2]) for item in posted.checkpoint.skipped],
                         [(2, 'B2', ('B',))])
        final = read(self.ledger, review, 'C3')
        self.assertEqual(final.skipped, posted.checkpoint.skipped)
