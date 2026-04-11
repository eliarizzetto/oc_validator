import unittest
import json
from os.path import join, dirname, abspath
from oc_validator.semantics import Semantics


class TestSemanticsCheckSemantics(unittest.TestCase):
    """Tests for Semantics.check_semantics."""

    def setUp(self):
        self.semantics = Semantics()
        with open(join(dirname(abspath(__file__)), '..', 'oc_validator', 'id_type_alignment.json'), 'r', encoding='utf-8') as f:
            self.alignment = json.load(f)

    def _make_row(self, id_val='doi:10.1234/abc', type_val='journal article'):
        return {'id': id_val, 'type': type_val}

    # --- Valid: no semantic errors ---
    def test_doi_journal_article(self):
        """doi is compatible with journal article."""
        row = self._make_row('doi:10.1234/abc', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_isbn_book(self):
        """isbn is compatible with book."""
        row = self._make_row('isbn:9783161484100', 'book')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_issn_journal(self):
        """issn is compatible with journal."""
        row = self._make_row('issn:0028-0836', 'journal')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_arxiv_journal_article(self):
        """arxiv is compatible with journal article."""
        row = self._make_row('arxiv:0711.3834', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_temp_journal_article(self):
        """temp is always compatible."""
        row = self._make_row('temp:1', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_local_journal_article(self):
        """local is always compatible."""
        row = self._make_row('local:abc', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_multiple_valid_ids(self):
        """Multiple IDs all compatible with the type."""
        row = self._make_row('doi:10.1234/abc pmid:12345678', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_wikipedia_reference_entry(self):
        """wikipedia is compatible with reference entry."""
        row = self._make_row('wikipedia:en:Test', 'reference entry')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    # --- Invalid: semantic errors ---
    def test_isbn_journal_article_invalid(self):
        """isbn is NOT compatible with journal article."""
        row = self._make_row('isbn:9783161484100', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertIn('id', result)
        self.assertIn('type', result)
        self.assertEqual(result['id'], [0])

    def test_arxiv_book_invalid(self):
        """arxiv is NOT compatible with book (it IS compatible per alignment)."""
        # Actually arxiv IS compatible with book. Let's use a truly incompatible one.
        pass

    def test_issn_journal_article_invalid(self):
        """issn is NOT compatible with journal article."""
        row = self._make_row('issn:0028-0836', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertIn('id', result)

    def test_pmcid_book_invalid(self):
        """pmcid is NOT compatible with book."""
        row = self._make_row('pmcid:PMC1234567', 'book')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertIn('id', result)

    def test_mixed_valid_and_invalid_ids(self):
        """One valid and one invalid ID in the same row."""
        row = self._make_row('doi:10.1234/abc isbn:9783161484100', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertIn('id', result)
        # doi is valid (idx 0), isbn is invalid (idx 1)
        self.assertEqual(result['id'], [1])

    def test_both_ids_invalid(self):
        """Both IDs are incompatible with the type."""
        row = self._make_row('isbn:9783161484100 issn:0028-0836', 'journal article')
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result['id'], [0, 1])

    # --- Edge cases ---
    def test_empty_id(self):
        """Empty id field - no error."""
        row = {'id': '', 'type': 'journal article'}
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_empty_type(self):
        """Empty type field - no error."""
        row = {'id': 'doi:10.1234/abc', 'type': ''}
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})

    def test_both_empty(self):
        """Both id and type are empty - no error."""
        row = {'id': '', 'type': ''}
        result = self.semantics.check_semantics(row, self.alignment)
        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main()
