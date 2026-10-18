"""Offline tests. Run: python -m unittest discover -s tests -v"""
import importlib.util
import json
import unittest
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
SCRIPT = BASE / 'scripts/paper2/collect_disclosure_change_5way.py'
spec = importlib.util.spec_from_file_location('dc5_collector', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class TestFiveToTwo(unittest.TestCase):
    def test_regression_mapping(self):
        self.assertEqual(module.GROUP['unchanged'], 'unchanged')
        self.assertEqual(module.GROUP['revised'], 'changed')
        self.assertEqual(module.GROUP['new_candidate'], 'changed')
        self.assertIsNone(module.GROUP['non_substantive'])
        self.assertIsNone(module.GROUP['uncertain'])

    def test_valid_five_way(self):
        rows = [{'current_index': i, 'label': label, 'confidence': 'high',
                 'prior_index_used': None, 'reason': 'Example.'}
                for i, label in enumerate(sorted(module.LABELS))]
        body = {'output': [{'content': [{'text': json.dumps({'classifications': rows})}]}]}
        self.assertEqual(len(module.parse(body, set(range(5)), 10)), 5)

    def test_missing_sentence_rejected(self):
        body = {'output_text': json.dumps({'classifications': [
            {'current_index': 0, 'label': 'unchanged', 'confidence': 'high',
             'prior_index_used': 0, 'reason': 'Same.'}]})}
        with self.assertRaisesRegex(ValueError, 'Coverage mismatch'):
            module.parse(body, {0, 1}, 10)

    def test_invalid_prior_index_rejected(self):
        body = {'output_text': json.dumps({'classifications': [
            {'current_index': 0, 'label': 'revised', 'confidence': 'mid',
             'prior_index_used': 10, 'reason': 'Updated.'}]})}
        with self.assertRaisesRegex(ValueError, 'prior_index'):
            module.parse(body, {0}, 10)

    def test_invalid_label_rejected(self):
        body = {'output_text': json.dumps({'classifications': [
            {'current_index': 0, 'label': 'persistent', 'confidence': 'high',
             'prior_index_used': None, 'reason': 'Old label.'}]})}
        with self.assertRaisesRegex(ValueError, 'invalid label'):
            module.parse(body, {0}, 10)


if __name__ == '__main__':
    unittest.main()
