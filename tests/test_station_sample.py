import os
from pathlib import Path

import station_review_sample
from tests.support import WorkspaceTestCase


class StationSampleTests(WorkspaceTestCase):
    def _paths(self, name):
        root = self.root / name
        root.mkdir()
        return root / 'relay.sqlite', root / 'ledger.sqlite'

    def test_full_fixtures(self):
        for same_value in (False, True):
            relay, ledger = self._paths(f'full-{same_value}')
            station_review_sample.check_fixture(relay, ledger, same_value=same_value)

    def test_station_open_and_disconnect(self):
        relay, ledger = self._paths('open')
        final = station_review_sample.check_station_fixture(relay, ledger)
        self.assertEqual(final.total_liters, 18)
        self.assertFalse(Path(relay).exists())

    def test_station_expiry_fixture(self):
        relay, ledger = self._paths('expiry')
        station_review_sample.check_station_expiry(relay, ledger)

    def test_station_lost_ack_fixture(self):
        relay, ledger = self._paths('lost')
        station_review_sample.check_station_lost_ack(relay, ledger)
