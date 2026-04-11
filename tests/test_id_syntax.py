import unittest
from oc_validator.id_syntax import IdSyntax


class TestIdSyntaxCheck(unittest.TestCase):
    """Tests for IdSyntax.check_id_syntax."""

    def setUp(self):
        self.syntax = IdSyntax()

    # ---- DOI ----
    def test_valid_doi(self):
        self.assertTrue(self.syntax.check_id_syntax('doi:10.1234/abc'))

    def test_valid_doi_complex(self):
        self.assertTrue(self.syntax.check_id_syntax('doi:10.1007/978-3-662-07918-8_3'))

    def test_invalid_doi_no_prefix(self):
        self.assertFalse(self.syntax.check_id_syntax('doi:'))

    def test_invalid_doi_bad_format(self):
        self.assertFalse(self.syntax.check_id_syntax('doi:not-a-doi'))

    # ---- ISBN ----
    def test_valid_isbn_13(self):
        self.assertTrue(self.syntax.check_id_syntax('isbn:9783161484100'))

    def test_invalid_isbn(self):
        self.assertFalse(self.syntax.check_id_syntax('isbn:not-an-isbn!@#'))

    # ---- ORCID ----
    def test_valid_orcid(self):
        self.assertTrue(self.syntax.check_id_syntax('orcid:0000-0001-2345-6789'))

    def test_invalid_orcid(self):
        self.assertFalse(self.syntax.check_id_syntax('orcid:not-an-orcid!@#'))

    # ---- PMCID ----
    def test_valid_pmcid(self):
        self.assertTrue(self.syntax.check_id_syntax('pmcid:PMC1234567'))

    def test_invalid_pmcid_no_prefix(self):
        self.assertFalse(self.syntax.check_id_syntax('pmcid:1234567'))

    # ---- PMID ----
    def test_valid_pmid(self):
        self.assertTrue(self.syntax.check_id_syntax('pmid:12345678'))

    def test_invalid_pmid_non_numeric(self):
        self.assertFalse(self.syntax.check_id_syntax('pmid:abc'))

    # ---- ROR ----
    def test_valid_ror(self):
        self.assertTrue(self.syntax.check_id_syntax('ror:012345678'))

    # ---- URL ----
    def test_valid_url(self):
        self.assertTrue(self.syntax.check_id_syntax('url:example.com'))

    def test_invalid_url(self):
        self.assertFalse(self.syntax.check_id_syntax('url:not-a-url'))

    # ---- VIAF ----
    def test_valid_viaf(self):
        self.assertTrue(self.syntax.check_id_syntax('viaf:12345678'))

    # ---- Wikidata ----
    def test_valid_wikidata(self):
        self.assertTrue(self.syntax.check_id_syntax('wikidata:Q12345'))

    # ---- Wikipedia ----
    def test_valid_wikipedia(self):
        self.assertTrue(self.syntax.check_id_syntax('wikipedia:12345'))

    # ---- OpenAlex ----
    def test_valid_openalex_work(self):
        self.assertTrue(self.syntax.check_id_syntax('openalex:W1234567890'))

    def test_valid_openalex_author_not_accepted(self):
        """OpenAlex manager only accepts W and S prefixes, not A."""
        self.assertFalse(self.syntax.check_id_syntax('openalex:A1234567890'))

    # ---- Crossref ----
    def test_valid_crossref(self):
        self.assertTrue(self.syntax.check_id_syntax('crossref:78'))

    # ---- JID ----
    def test_valid_jid(self):
        self.assertTrue(self.syntax.check_id_syntax('jid:jssog1981'))

    # ---- arXiv ----
    def test_valid_arxiv(self):
        self.assertTrue(self.syntax.check_id_syntax('arxiv:0711.3834'))

    # ---- OMID ----
    def test_valid_omid_br(self):
        self.assertTrue(self.syntax.check_id_syntax('omid:br/0610116033'))

    def test_valid_omid_ra(self):
        self.assertTrue(self.syntax.check_id_syntax('omid:ra/0610116033'))

    def test_invalid_omid_wrong_entity(self):
        self.assertFalse(self.syntax.check_id_syntax('omid:xx/0610116033'))

    # ---- temp and local (always pass) ----
    def test_temp_always_valid(self):
        self.assertTrue(self.syntax.check_id_syntax('temp:1'))

    def test_temp_any_value(self):
        self.assertTrue(self.syntax.check_id_syntax('temp:anything'))

    def test_local_always_valid(self):
        self.assertTrue(self.syntax.check_id_syntax('local:abc-123'))

    # ---- Unknown scheme ----
    def test_unknown_scheme_returns_false(self):
        self.assertFalse(self.syntax.check_id_syntax('unknown:value'))


if __name__ == '__main__':
    unittest.main()
