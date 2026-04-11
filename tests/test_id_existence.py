import unittest
from unittest.mock import patch, MagicMock
from oc_validator.id_existence import IdExistence


class TestIdExistenceCheckIdExistence(unittest.TestCase):
    """Tests for IdExistence.check_id_existence."""

    def setUp(self):
        """Create IdExistence with external services mocked (use_meta_endpoint=False)."""
        self.existence = IdExistence(use_meta_endpoint=False)

    def test_temp_always_exists(self):
        self.assertTrue(self.existence.check_id_existence('temp:1'))

    def test_local_always_exists(self):
        self.assertTrue(self.existence.check_id_existence('local:abc'))

    def test_omid_delegates_to_query_omid_in_meta(self):
        with patch.object(self.existence, 'query_omid_in_meta', return_value=True) as mock_omid:
            result = self.existence.check_id_existence('omid:br/0610116033')
            mock_omid.assert_called_once_with('omid:br/0610116033')
            self.assertTrue(result)

    def test_with_meta_endpoint_found_in_meta(self):
        """If meta endpoint returns True, skip external service."""
        existence = IdExistence(use_meta_endpoint=True)
        with patch.object(existence, 'query_meta_triplestore', return_value=True) as mock_meta, \
             patch.object(existence, 'query_external_service') as mock_ext:
            result = existence.check_id_existence('doi:10.1234/abc')
            mock_meta.assert_called_once_with('doi:10.1234/abc')
            mock_ext.assert_not_called()
            self.assertTrue(result)

    def test_with_meta_endpoint_not_found_falls_back(self):
        """If meta endpoint returns False, fall back to external service."""
        existence = IdExistence(use_meta_endpoint=True)
        with patch.object(existence, 'query_meta_triplestore', return_value=False) as mock_meta, \
             patch.object(existence, 'query_external_service', return_value=True) as mock_ext:
            result = existence.check_id_existence('doi:10.1234/abc')
            mock_meta.assert_called_once()
            mock_ext.assert_called_once_with('doi:10.1234/abc')
            self.assertTrue(result)

    def test_without_meta_endpoint_uses_external(self):
        with patch.object(self.existence, 'query_external_service', return_value=True) as mock_ext:
            result = self.existence.check_id_existence('doi:10.1234/abc')
            mock_ext.assert_called_once_with('doi:10.1234/abc')
            self.assertTrue(result)


class TestIdExistenceQueryExternalService(unittest.TestCase):
    """Tests for IdExistence.query_external_service."""

    def setUp(self):
        self.existence = IdExistence(use_meta_endpoint=False)

    def test_doi_dispatches_to_doi_manager(self):
        with patch.object(self.existence.doi_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('doi:10.1234/abc')
            mock_exists.assert_called_once_with('10.1234/abc')
            self.assertTrue(result)

    def test_isbn_dispatches_to_isbn_manager(self):
        with patch.object(self.existence.isbn_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('isbn:9783161484100')
            mock_exists.assert_called_once_with('9783161484100')
            self.assertTrue(result)

    def test_issn_dispatches_to_issn_manager(self):
        with patch.object(self.existence.issn_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('issn:0028-0836')
            mock_exists.assert_called_once_with('0028-0836')
            self.assertTrue(result)

    def test_orcid_dispatches_to_orcid_manager(self):
        with patch.object(self.existence.orcid_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('orcid:0000-0001-2345-6789')
            mock_exists.assert_called_once_with('0000-0001-2345-6789')
            self.assertTrue(result)

    def test_pmcid_dispatches_to_pmcid_manager(self):
        with patch.object(self.existence.pmcid_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('pmcid:PMC1234567')
            mock_exists.assert_called_once_with('PMC1234567')
            self.assertTrue(result)

    def test_pmid_dispatches_to_pmid_manager(self):
        with patch.object(self.existence.pmid_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('pmid:12345678')
            mock_exists.assert_called_once_with('12345678')
            self.assertTrue(result)

    def test_ror_dispatches_to_ror_manager(self):
        with patch.object(self.existence.ror_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('ror:012345678')
            mock_exists.assert_called_once_with('012345678')
            self.assertTrue(result)

    def test_url_dispatches_to_url_manager(self):
        with patch.object(self.existence.url_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('url:https://example.com')
            mock_exists.assert_called_once_with('https://example.com')
            self.assertTrue(result)

    def test_viaf_dispatches_to_viaf_manager(self):
        with patch.object(self.existence.viaf_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('viaf:12345678')
            mock_exists.assert_called_once_with('12345678')
            self.assertTrue(result)

    def test_wikidata_dispatches_to_wikidata_manager(self):
        with patch.object(self.existence.wikidata_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('wikidata:Q12345')
            mock_exists.assert_called_once_with('Q12345')
            self.assertTrue(result)

    def test_wikipedia_dispatches_to_wikipedia_manager(self):
        with patch.object(self.existence.wikipedia_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('wikipedia:en:Test')
            mock_exists.assert_called_once_with('en:Test')
            self.assertTrue(result)

    def test_openalex_dispatches_to_openalex_manager(self):
        with patch.object(self.existence.openalex_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('openalex:W1234567890')
            mock_exists.assert_called_once_with('W1234567890')
            self.assertTrue(result)

    def test_crossref_dispatches_to_crossref_manager(self):
        with patch.object(self.existence.crossref_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('crossref:78')
            mock_exists.assert_called_once_with('78')
            self.assertTrue(result)

    def test_jid_dispatches_to_jid_manager(self):
        with patch.object(self.existence.jid_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('jid:jssog1981')
            mock_exists.assert_called_once_with('jssog1981')
            self.assertTrue(result)

    def test_arxiv_dispatches_to_arxiv_manager(self):
        with patch.object(self.existence.arxiv_mngr, 'exists', return_value=True) as mock_exists:
            result = self.existence.query_external_service('arxiv:0711.3834')
            mock_exists.assert_called_once_with('0711.3834')
            self.assertTrue(result)

    def test_unknown_prefix_returns_false(self):
        result = self.existence.query_external_service('unknown:value')
        self.assertFalse(result)

    def test_doi_not_found_returns_false(self):
        with patch.object(self.existence.doi_mngr, 'exists', return_value=False):
            result = self.existence.query_external_service('doi:10.1234/nonexistent')
            self.assertFalse(result)


class TestIdExistenceQueryMetaTriplestore(unittest.TestCase):
    """Tests for IdExistence.query_meta_triplestore."""

    def setUp(self):
        self.existence = IdExistence(use_meta_endpoint=True)

    @patch('oc_validator.id_existence.time.sleep')
    def test_successful_query_returns_boolean(self, mock_sleep):
        """Test that a successful SPARQL query returns the boolean result."""
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': True}
        with patch.object(self.existence.sparql, 'query', return_value=mock_result):
            result = self.existence.query_meta_triplestore('doi:10.1234/abc')
            self.assertTrue(result)

    @patch('oc_validator.id_existence.time.sleep')
    def test_not_found_returns_false(self, mock_sleep):
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': False}
        with patch.object(self.existence.sparql, 'query', return_value=mock_result):
            result = self.existence.query_meta_triplestore('doi:10.1234/abc')
            self.assertFalse(result)

    @patch('oc_validator.id_existence.time.sleep')
    def test_retry_on_failure(self, mock_sleep):
        """Test that the query retries on transient failures."""
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': True}
        # First call raises, second succeeds
        with patch.object(self.existence.sparql, 'query', side_effect=[Exception('Network error'), mock_result]):
            result = self.existence.query_meta_triplestore('doi:10.1234/abc', retries=3, delay=0.01)
            self.assertTrue(result)
            mock_sleep.assert_called_once_with(0.01)

    @patch('oc_validator.id_existence.time.sleep')
    def test_max_retries_returns_false(self, mock_sleep):
        """Test that after max retries, returns False."""
        with patch.object(self.existence.sparql, 'query', side_effect=Exception('Network error')):
            result = self.existence.query_meta_triplestore('doi:10.1234/abc', retries=2, delay=0.01)
            self.assertFalse(result)
            self.assertEqual(mock_sleep.call_count, 1)  # one sleep between retries


class TestIdExistenceQueryOmidInMeta(unittest.TestCase):
    """Tests for IdExistence.query_omid_in_meta."""

    def setUp(self):
        self.existence = IdExistence(use_meta_endpoint=True)

    @patch('oc_validator.id_existence.time.sleep')
    def test_omid_found(self, mock_sleep):
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': True}
        with patch.object(self.existence.sparql, 'query', return_value=mock_result):
            result = self.existence.query_omid_in_meta('omid:br/0610116033')
            self.assertTrue(result)

    @patch('oc_validator.id_existence.time.sleep')
    def test_omid_not_found(self, mock_sleep):
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': False}
        with patch.object(self.existence.sparql, 'query', return_value=mock_result):
            result = self.existence.query_omid_in_meta('omid:br/0610116033')
            self.assertFalse(result)

    @patch('oc_validator.id_existence.time.sleep')
    def test_omid_retry_on_failure(self, mock_sleep):
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': True}
        with patch.object(self.existence.sparql, 'query', side_effect=[Exception('fail'), mock_result]):
            result = self.existence.query_omid_in_meta('omid:br/0610116033', retries=3, delay=0.01)
            self.assertTrue(result)

    @patch('oc_validator.id_existence.time.sleep')
    def test_omid_max_retries_returns_false(self, mock_sleep):
        with patch.object(self.existence.sparql, 'query', side_effect=Exception('fail')):
            result = self.existence.query_omid_in_meta('omid:br/0610116033', retries=2, delay=0.01)
            self.assertFalse(result)

    def test_omid_strips_prefix_correctly(self):
        """Verify the SPARQL query uses the ID without the omid: prefix."""
        mock_result = MagicMock()
        mock_result.convert.return_value = {'boolean': True}
        with patch.object(self.existence.sparql, 'setQuery') as mock_set, \
             patch.object(self.existence.sparql, 'setReturnFormat'), \
             patch.object(self.existence.sparql, 'query', return_value=mock_result):
            self.existence.query_omid_in_meta('omid:br/0610116033')
            query_arg = mock_set.call_args[0][0]
            self.assertIn('br/0610116033', query_arg)
            self.assertNotIn('omid:br/0610116033', query_arg)


if __name__ == '__main__':
    unittest.main()
