import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        portal = Portal()
        portal.draft('A1', {'count': 1})
        portal.drop_response = True
        with self.assertRaises(ConnectionError):
            portal.submit('A1')
        self.assertEqual(len(portal.query('A1')), 1)
        self.assertNotEqual(content_digest({'count': 1}), content_digest({'count': 2}))

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
