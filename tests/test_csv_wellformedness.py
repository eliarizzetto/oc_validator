import unittest
from oc_validator.csv_wellformedness import Wellformedness
from oc_validator.helper import Helper
from oc_validator.lmdb_cache import InMemoryCache, InMemoryUnionFind


class TestWellformednessBrId(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_br_id."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_doi(self):
        self.assertTrue(self.wf.wellformedness_br_id('doi:10.1234/abc'))

    def test_valid_issn(self):
        self.assertTrue(self.wf.wellformedness_br_id('issn:1234-5679'))

    def test_valid_isbn(self):
        self.assertTrue(self.wf.wellformedness_br_id('isbn:9783161484100'))

    def test_valid_pmid(self):
        self.assertTrue(self.wf.wellformedness_br_id('pmid:12345678'))

    def test_valid_pmcid(self):
        self.assertTrue(self.wf.wellformedness_br_id('pmcid:PMC1234567'))

    def test_valid_url(self):
        self.assertTrue(self.wf.wellformedness_br_id('url:https://example.com'))

    def test_valid_wikidata(self):
        self.assertTrue(self.wf.wellformedness_br_id('wikidata:Q12345'))

    def test_valid_wikipedia(self):
        self.assertTrue(self.wf.wellformedness_br_id('wikipedia:en:Test'))

    def test_valid_openalex(self):
        self.assertTrue(self.wf.wellformedness_br_id('openalex:W1234567890'))

    def test_valid_temp(self):
        self.assertTrue(self.wf.wellformedness_br_id('temp:1'))

    def test_valid_local(self):
        self.assertTrue(self.wf.wellformedness_br_id('local:abc-123'))

    def test_valid_omid(self):
        self.assertTrue(self.wf.wellformedness_br_id('omid:br/0610116033'))

    def test_valid_jid(self):
        self.assertTrue(self.wf.wellformedness_br_id('jid:jssog1981'))

    def test_valid_arxiv(self):
        self.assertTrue(self.wf.wellformedness_br_id('arxiv:0711.3834'))

    def test_valid_doi_complex(self):
        self.assertTrue(self.wf.wellformedness_br_id('doi:10.1007/978-3-662-07918-8_3'))

    # --- Invalid cases ---
    def test_invalid_no_colon(self):
        self.assertFalse(self.wf.wellformedness_br_id('doi10.1234'))

    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_br_id(''))

    def test_invalid_unknown_scheme(self):
        self.assertFalse(self.wf.wellformedness_br_id('fake:value'))

    def test_invalid_space_in_value(self):
        self.assertFalse(self.wf.wellformedness_br_id('doi:10.1234/abc def'))

    def test_invalid_orcid_scheme(self):
        """ORCID is an RA scheme, not a BR scheme."""
        self.assertFalse(self.wf.wellformedness_br_id('orcid:0000-0001-2345-6789'))

    def test_invalid_crossref_scheme(self):
        """Crossref is an RA scheme, not a BR scheme."""
        self.assertFalse(self.wf.wellformedness_br_id('crossref:78'))

    def test_invalid_ror_scheme(self):
        """ROR is an RA scheme, not a BR scheme."""
        self.assertFalse(self.wf.wellformedness_br_id('ror:012345678'))

    def test_invalid_viaf_scheme(self):
        """VIAF is an RA scheme, not a BR scheme."""
        self.assertFalse(self.wf.wellformedness_br_id('viaf:12345678'))

    def test_invalid_only_prefix(self):
        self.assertFalse(self.wf.wellformedness_br_id('doi:'))

    def test_invalid_trailing_space(self):
        self.assertFalse(self.wf.wellformedness_br_id('doi:10.1234 '))


class TestWellformednessPeopleItem(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_people_item."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_name_only(self):
        self.assertTrue(self.wf.wellformedness_people_item('Smith, John'))

    def test_valid_name_with_single_id(self):
        self.assertTrue(self.wf.wellformedness_people_item('Smith, John [orcid:0000-0001-2345-6789]'))

    def test_valid_name_with_multiple_ids(self):
        self.assertTrue(self.wf.wellformedness_people_item('Smith, John [orcid:0000-0001-2345-6789 wikidata:Q12345]'))

    def test_valid_ids_only(self):
        self.assertTrue(self.wf.wellformedness_people_item('[orcid:0000-0001-2345-6789]'))

    def test_valid_multiple_ids_only(self):
        self.assertTrue(self.wf.wellformedness_people_item('[orcid:0000-0001-2345-6789 wikidata:Q12345]'))

    def test_valid_single_name(self):
        self.assertTrue(self.wf.wellformedness_people_item('Aristotle'))

    def test_valid_name_without_comma(self):
        self.assertTrue(self.wf.wellformedness_people_item('John'))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_people_item(''))

    def test_invalid_br_id_scheme(self):
        """BR schemes (e.g. doi) are not valid for people items."""
        self.assertFalse(self.wf.wellformedness_people_item('Smith [doi:10.1234/abc]'))

    def test_id_outside_brackets_is_treated_as_name(self):
        """An RA ID string without brackets is treated as a name (valid format)."""
        self.assertTrue(self.wf.wellformedness_people_item('orcid:0000-0001-2345-6789'))

    def test_invalid_unclosed_bracket(self):
        self.assertFalse(self.wf.wellformedness_people_item('Smith [orcid:0000-0001-2345-6789'))

    def test_invalid_empty_brackets(self):
        self.assertFalse(self.wf.wellformedness_people_item('Smith []'))


class TestWellformednessPublisherItem(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_publisher_item."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_name_only(self):
        self.assertTrue(self.wf.wellformedness_publisher_item('Springer Science and Business Media LLC'))

    def test_valid_name_with_id(self):
        self.assertTrue(self.wf.wellformedness_publisher_item('Springer Science and Business Media LLC [crossref:297]'))

    def test_valid_name_with_multiple_ids(self):
        self.assertTrue(self.wf.wellformedness_publisher_item(
            'Ovid Technologies (Wolters Kluwer Health) [omid:ra/0610116033 crossref:276]'
        ))

    def test_valid_ids_only(self):
        self.assertTrue(self.wf.wellformedness_publisher_item('[crossref:297]'))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_publisher_item(''))

    def test_invalid_br_id_scheme(self):
        """BR schemes like doi are not valid for publisher items."""
        self.assertFalse(self.wf.wellformedness_publisher_item('Publisher [doi:10.1234]'))

    def test_invalid_unclosed_bracket(self):
        self.assertFalse(self.wf.wellformedness_publisher_item('Publisher [crossref:297'))


class TestOrphanRaId(unittest.TestCase):
    """Tests for Wellformedness.orphan_ra_id."""

    def setUp(self):
        self.wf = Wellformedness()

    def test_orphan_id_present(self):
        self.assertTrue(self.wf.orphan_ra_id('Smith orcid:0000-0001-2345-6789'))

    def test_no_orphan_id_in_brackets(self):
        self.assertFalse(self.wf.orphan_ra_id('Smith [orcid:0000-0001-2345-6789]'))

    def test_no_id_at_all(self):
        self.assertFalse(self.wf.orphan_ra_id('Smith, John'))

    def test_orphan_crossref_id(self):
        self.assertTrue(self.wf.orphan_ra_id('Publisher crossref:297'))

    def test_orphan_mixed(self):
        """ID outside brackets even when brackets also present."""
        self.assertTrue(self.wf.orphan_ra_id('Smith orcid:0000-0001-2345-6789 [wikidata:Q12345]'))


class TestWellformednessDate(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_date."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_year_only(self):
        self.assertTrue(self.wf.wellformedness_date('2004'))

    def test_valid_year_month(self):
        self.assertTrue(self.wf.wellformedness_date('2004-01'))

    def test_valid_year_month_day(self):
        self.assertTrue(self.wf.wellformedness_date('2004-01-14'))

    def test_valid_year_december(self):
        self.assertTrue(self.wf.wellformedness_date('2004-12'))

    def test_valid_year_month_31(self):
        self.assertTrue(self.wf.wellformedness_date('2004-01-31'))

    def test_valid_year_month_day_29(self):
        self.assertTrue(self.wf.wellformedness_date('2004-12-29'))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_date(''))

    def test_invalid_month_13(self):
        self.assertFalse(self.wf.wellformedness_date('2004-13'))

    def test_invalid_day_32(self):
        self.assertFalse(self.wf.wellformedness_date('2004-01-32'))

    def test_invalid_day_00(self):
        self.assertFalse(self.wf.wellformedness_date('2004-01-00'))

    def test_invalid_month_00(self):
        self.assertFalse(self.wf.wellformedness_date('2004-00'))

    def test_invalid_slash_separator(self):
        self.assertFalse(self.wf.wellformedness_date('2004/01/14'))

    def test_invalid_two_digit_year(self):
        self.assertFalse(self.wf.wellformedness_date('04'))

    def test_invalid_month_only(self):
        self.assertFalse(self.wf.wellformedness_date('01'))

    def test_invalid_day_without_month(self):
        self.assertFalse(self.wf.wellformedness_date('2004--14'))

    def test_invalid_text(self):
        self.assertFalse(self.wf.wellformedness_date('not-a-date'))

    def test_invalid_year_month_day_extra(self):
        self.assertFalse(self.wf.wellformedness_date('2004-01-14-00'))

    def test_invalid_month_single_digit(self):
        self.assertFalse(self.wf.wellformedness_date('2004-1'))

    def test_invalid_day_single_digit(self):
        self.assertFalse(self.wf.wellformedness_date('2004-01-1'))


class TestWellformednessVenue(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_venue."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_name_only(self):
        self.assertTrue(self.wf.wellformedness_venue('Nature'))

    def test_valid_name_with_id(self):
        self.assertTrue(self.wf.wellformedness_venue('Nature [issn:0028-0836]'))

    def test_valid_name_with_multiple_ids(self):
        self.assertTrue(self.wf.wellformedness_venue(
            'IEEE Transactions on Magnetics [issn:0018-9464]'
        ))

    def test_valid_ids_only(self):
        self.assertTrue(self.wf.wellformedness_venue('[issn:0028-0836]'))

    def test_valid_multiple_ids_only(self):
        self.assertTrue(self.wf.wellformedness_venue('[issn:0028-0836 issn:1476-4687]'))

    def test_valid_from_real_data(self):
        self.assertTrue(self.wf.wellformedness_venue(
            'Insulation of High-Voltage Equipment [isbn:9783642058530 isbn:9783662079188]'
        ))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_venue(''))

    def test_pmcid_is_valid_in_venue(self):
        """PMCID IS in br_id_schemes_for_venues despite the code comment."""
        self.assertTrue(self.wf.wellformedness_venue('Journal [pmcid:PMC1234567]'))

    def test_invalid_ra_id_in_venue(self):
        """RA schemes (orcid, crossref) should not be in venues."""
        self.assertFalse(self.wf.wellformedness_venue('Journal [orcid:0000-0001-2345-6789]'))

    def test_invalid_unclosed_bracket(self):
        self.assertFalse(self.wf.wellformedness_venue('Nature [issn:0028-0836'))


class TestOrphanVenueId(unittest.TestCase):
    """Tests for Wellformedness.orphan_venue_id."""

    def setUp(self):
        self.wf = Wellformedness()

    def test_orphan_id_present(self):
        self.assertTrue(self.wf.orphan_venue_id('Nature issn:0028-0836'))

    def test_no_orphan_id_in_brackets(self):
        self.assertFalse(self.wf.orphan_venue_id('Nature [issn:0028-0836]'))

    def test_no_id_at_all(self):
        self.assertFalse(self.wf.orphan_venue_id('Nature'))

    def test_orphan_mixed(self):
        self.assertTrue(self.wf.orphan_venue_id('Nature issn:0028-0836 [doi:10.1234]'))


class TestWellformednessVolumeIssue(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_volume_issue."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_single_number(self):
        self.assertTrue(self.wf.wellformedness_volume_issue('36'))

    def test_valid_single_word(self):
        self.assertTrue(self.wf.wellformedness_volume_issue('vol-1'))

    def test_valid_multi_token(self):
        self.assertTrue(self.wf.wellformedness_volume_issue('Volume 1'))

    def test_valid_alphanumeric(self):
        self.assertTrue(self.wf.wellformedness_volume_issue('A1'))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_volume_issue(''))

    def test_invalid_leading_space(self):
        self.assertFalse(self.wf.wellformedness_volume_issue(' 36'))

    def test_invalid_trailing_space(self):
        self.assertFalse(self.wf.wellformedness_volume_issue('36 '))

    def test_invalid_double_space(self):
        self.assertFalse(self.wf.wellformedness_volume_issue('36  4'))


class TestWellformednessPage(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_page."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases: numeric ---
    def test_valid_numeric_range(self):
        self.assertTrue(self.wf.wellformedness_page('1-10'))

    def test_valid_numeric_range_large(self):
        self.assertTrue(self.wf.wellformedness_page('1280-1284'))

    def test_valid_same_page(self):
        self.assertTrue(self.wf.wellformedness_page('5-5'))

    def test_valid_single_digit_pages(self):
        self.assertTrue(self.wf.wellformedness_page('3-9'))

    # --- Valid cases: roman ---
    def test_valid_roman_range(self):
        self.assertTrue(self.wf.wellformedness_page('i-x'))

    def test_valid_roman_range_uppercase(self):
        self.assertTrue(self.wf.wellformedness_page('I-X'))

    def test_valid_roman_mixed_case(self):
        self.assertTrue(self.wf.wellformedness_page('i-X'))

    # --- Valid cases: alphanumeric ---
    def test_valid_alphanumeric_range(self):
        self.assertTrue(self.wf.wellformedness_page('a1-b2'))

    def test_valid_alphanumeric_prefix(self):
        self.assertTrue(self.wf.wellformedness_page('A1-A10'))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_page(''))

    def test_invalid_no_hyphen(self):
        self.assertFalse(self.wf.wellformedness_page('10'))

    def test_invalid_only_hyphen(self):
        self.assertFalse(self.wf.wellformedness_page('-'))

    def test_invalid_triple_hyphen(self):
        self.assertFalse(self.wf.wellformedness_page('1-2-3'))

    def test_invalid_page_zero(self):
        self.assertFalse(self.wf.wellformedness_page('0-10'))

    def test_invalid_spaces(self):
        self.assertFalse(self.wf.wellformedness_page('1 - 10'))

    def test_invalid_leading_zero(self):
        self.assertFalse(self.wf.wellformedness_page('01-10'))

    def test_invalid_just_letters(self):
        self.assertFalse(self.wf.wellformedness_page('abc'))

    def test_valid_greek_alphanum(self):
        self.assertTrue(self.wf.wellformedness_page('\u03b11-\u03b12'))  # α1-α2


class TestCheckPageInterval(unittest.TestCase):
    """Tests for Wellformedness.check_page_interval."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_ascending(self):
        self.assertTrue(self.wf.check_page_interval('1-10'))

    def test_valid_equal(self):
        self.assertTrue(self.wf.check_page_interval('5-5'))

    def test_valid_roman_ascending(self):
        self.assertTrue(self.wf.check_page_interval('i-x'))

    def test_valid_roman_equal(self):
        self.assertTrue(self.wf.check_page_interval('v-v'))

    def test_valid_same_alphanumeric(self):
        self.assertTrue(self.wf.check_page_interval('a12-a12'))

    def test_valid_large_range(self):
        self.assertTrue(self.wf.check_page_interval('1-999'))

    # --- Invalid cases ---
    def test_invalid_descending(self):
        self.assertFalse(self.wf.check_page_interval('10-1'))

    def test_invalid_roman_descending(self):
        self.assertFalse(self.wf.check_page_interval('x-i'))

    def test_invalid_different_alphanumeric(self):
        self.assertFalse(self.wf.check_page_interval('a12-b24'))

    def test_invalid_roman_garbage(self):
        self.assertFalse(self.wf.check_page_interval('abc-def'))


class TestWellformednessType(unittest.TestCase):
    """Tests for Wellformedness.wellformedness_type."""

    def setUp(self):
        self.wf = Wellformedness()

    # --- Valid cases ---
    def test_valid_journal_article(self):
        self.assertTrue(self.wf.wellformedness_type('journal article'))

    def test_valid_book(self):
        self.assertTrue(self.wf.wellformedness_type('book'))

    def test_valid_book_chapter(self):
        self.assertTrue(self.wf.wellformedness_type('book chapter'))

    def test_valid_journal(self):
        self.assertTrue(self.wf.wellformedness_type('journal'))

    def test_valid_proceedings_article(self):
        self.assertTrue(self.wf.wellformedness_type('proceedings article'))

    def test_valid_dataset(self):
        self.assertTrue(self.wf.wellformedness_type('dataset'))

    def test_valid_dissertation(self):
        self.assertTrue(self.wf.wellformedness_type('dissertation'))

    def test_valid_preprint(self):
        self.assertTrue(self.wf.wellformedness_type('preprint'))

    def test_valid_retraction_notice(self):
        self.assertTrue(self.wf.wellformedness_type('retraction notice'))

    # --- Invalid cases ---
    def test_invalid_empty(self):
        self.assertFalse(self.wf.wellformedness_type(''))

    def test_invalid_article(self):
        """'article' alone is not a valid type."""
        self.assertFalse(self.wf.wellformedness_type('article'))

    def test_invalid_journal_article_capitalized(self):
        self.assertFalse(self.wf.wellformedness_type('Journal Article'))

    def test_invalid_with_extra_space(self):
        self.assertFalse(self.wf.wellformedness_type(' journal article'))

    def test_invalid_unknown_type(self):
        self.assertFalse(self.wf.wellformedness_type('magazine article'))


class TestGetMissingValues(unittest.TestCase):
    """Tests for Wellformedness.get_missing_values."""

    def setUp(self):
        self.wf = Wellformedness()

    def _make_row(self, **overrides):
        """Create a row dict with all META-CSV fields, overriding specific ones."""
        base = {
            'id': 'doi:10.1234/abc',
            'title': 'A Title',
            'author': 'Smith, John',
            'pub_date': '2020',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': 'journal article',
            'publisher': '',
            'editor': ''
        }
        base.update(overrides)
        return base

    # --- Valid rows (no missing values) ---
    def test_complete_row_journal_article(self):
        row = self._make_row()
        self.assertEqual(self.wf.get_missing_values(row), {})

    def test_complete_row_with_volume_and_venue(self):
        row = self._make_row(volume='36', venue='Nature [issn:0028-0836]')
        self.assertEqual(self.wf.get_missing_values(row), {})

    def test_complete_row_book_chapter(self):
        row = self._make_row(
            id='doi:10.1234/abc',
            type='book chapter',
            title='A Chapter',
            venue='A Book [isbn:9783161484100]'
        )
        self.assertEqual(self.wf.get_missing_values(row), {})

    # --- Missing fields when id is empty ---
    def test_missing_id_and_type_title_pubdate(self):
        row = self._make_row(id='', type='journal article', title='', pub_date='', author='', editor='')
        result = self.wf.get_missing_values(row)
        self.assertIn('title', result)
        self.assertIn('pub_date', result)
        self.assertIn('author', result)
        self.assertIn('editor', result)
        self.assertIn('type', result)

    def test_missing_id_no_type(self):
        """When id and type are both empty, title, pub_date, and author/editor are missing."""
        row = self._make_row(id='', type='', title='', pub_date='', author='', editor='')
        result = self.wf.get_missing_values(row)
        self.assertIn('title', result)
        self.assertIn('pub_date', result)
        self.assertIn('author', result)
        self.assertIn('type', result)

    def test_temp_id_only_missing_title(self):
        """temp: ID counts as 'no real ID'; title becomes required."""
        row = self._make_row(id='temp:1', type='journal article', title='', pub_date='2020', author='Smith')
        result = self.wf.get_missing_values(row)
        self.assertIn('title', result)

    def test_local_id_only_missing_title(self):
        """local: ID counts as 'no real ID'; title becomes required."""
        row = self._make_row(id='local:abc', type='journal article', title='', pub_date='2020', author='Smith')
        result = self.wf.get_missing_values(row)
        self.assertIn('title', result)

    def test_temp_id_with_all_required_fields(self):
        """temp: ID but all required fields are present - should be OK for required fields."""
        row = self._make_row(id='temp:1', type='journal article', title='A Title', pub_date='2020', author='Smith')
        result = self.wf.get_missing_values(row)
        self.assertNotIn('title', result)
        self.assertNotIn('pub_date', result)

    # --- book chapter requires venue when id is missing ---
    def test_book_chapter_missing_venue(self):
        row = self._make_row(id='', type='book chapter', title='A Chapter', venue='')
        result = self.wf.get_missing_values(row)
        self.assertIn('venue', result)

    def test_book_chapter_has_venue(self):
        row = self._make_row(id='', type='book chapter', title='A Chapter', venue='A Book [isbn:978123]')
        result = self.wf.get_missing_values(row)
        self.assertNotIn('venue', result)

    # --- volume without venue ---
    def test_volume_without_venue(self):
        row = self._make_row(volume='36', venue='')
        result = self.wf.get_missing_values(row)
        self.assertIn('venue', result)

    def test_issue_without_venue(self):
        row = self._make_row(issue='4', venue='')
        result = self.wf.get_missing_values(row)
        self.assertIn('venue', result)

    def test_volume_with_venue(self):
        row = self._make_row(volume='36', venue='Nature')
        result = self.wf.get_missing_values(row)
        self.assertNotIn('venue', result)

    # --- journal issue requires venue ---
    def test_journal_issue_missing_venue(self):
        row = self._make_row(id='', type='journal issue', venue='', title='', issue='')
        result = self.wf.get_missing_values(row)
        self.assertIn('venue', result)
        self.assertIn('title', result)
        self.assertIn('issue', result)

    def test_journal_issue_has_venue_and_either_title_or_issue(self):
        row = self._make_row(id='', type='journal issue', venue='Nature', title='', issue='4')
        result = self.wf.get_missing_values(row)
        self.assertNotIn('venue', result)

    # --- journal volume requires venue ---
    def test_journal_volume_missing_venue(self):
        row = self._make_row(id='', type='journal volume', venue='', title='', volume='')
        result = self.wf.get_missing_values(row)
        self.assertIn('venue', result)

    # --- book series / journal / proceedings series: title required when id missing ---
    def test_journal_missing_title(self):
        row = self._make_row(id='', type='journal', title='')
        result = self.wf.get_missing_values(row)
        self.assertIn('title', result)


class TestGetDuplicatesMeta(unittest.TestCase):
    """Tests for Wellformedness.get_duplicates_meta."""

    def setUp(self):
        self.wf = Wellformedness()
        self.messages = {
            'm11': 'The same bibliographic resource is being represented in more than one row.'
        }

    def test_no_duplicates(self):
        uf = InMemoryUnionFind()
        uf.union('doi:10.1234/a', 'doi:10.1234/a')  # just register
        data = InMemoryCache('test')
        data.open()
        data['0'] = 'doi:10.1234/a'
        data['1'] = 'doi:10.5678/b'

        result = list(self.wf.get_duplicates_meta(uf, data, self.messages))
        self.assertEqual(result, [])
        data.close()

    def test_with_duplicates(self):
        uf = InMemoryUnionFind()
        # Two rows with same entity
        uf.find('doi:10.1234/a')
        uf.find('doi:10.1234/a')
        data = InMemoryCache('test')
        data.open()
        data['0'] = 'doi:10.1234/a'
        data['1'] = 'doi:10.1234/a'

        result = list(self.wf.get_duplicates_meta(uf, data, self.messages))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['error_label'], 'duplicate_br')
        data.close()

    def test_empty_data(self):
        uf = InMemoryUnionFind()
        data = InMemoryCache('test')
        data.open()

        result = list(self.wf.get_duplicates_meta(uf, data, self.messages))
        self.assertEqual(result, [])
        data.close()


class TestGetDuplicatesCits(unittest.TestCase):
    """Tests for Wellformedness.get_duplicates_cits."""

    def setUp(self):
        self.wf = Wellformedness()
        self.messages = {
            'm4': 'Self-citation warning.',
            'm5': 'Duplicate citation error.'
        }

    def test_no_duplicates(self):
        uf = InMemoryUnionFind()
        uf.find('doi:10.1234/a')
        uf.find('doi:10.5678/b')
        uf.find('doi:10.9012/c')
        data = InMemoryCache('test')
        data.open()
        data['0'] = ('doi:10.1234/a', 'doi:10.5678/b')
        data['1'] = ('doi:10.1234/a', 'doi:10.9012/c')

        result = list(self.wf.get_duplicates_cits(uf, data, self.messages))
        self.assertEqual(result, [])
        data.close()

    def test_self_citation(self):
        uf = InMemoryUnionFind()
        uf.union('doi:10.1234/a', 'doi:10.1234/a')
        uf.union('doi:10.1234/a', 'doi:10.5678/b')
        data = InMemoryCache('test')
        data.open()
        data['0'] = ('doi:10.1234/a', 'doi:10.5678/b')

        result = list(self.wf.get_duplicates_cits(uf, data, self.messages))
        self.assertTrue(any(r['error_label'] == 'self-citation' for r in result))
        data.close()

    def test_duplicate_citation(self):
        uf = InMemoryUnionFind()
        uf.find('doi:10.1234/a')
        uf.find('doi:10.5678/b')
        data = InMemoryCache('test')
        data.open()
        data['0'] = ('doi:10.1234/a', 'doi:10.5678/b')
        data['1'] = ('doi:10.1234/a', 'doi:10.5678/b')

        result = list(self.wf.get_duplicates_cits(uf, data, self.messages))
        self.assertTrue(any(r['error_label'] == 'duplicate_citation' for r in result))
        data.close()

    def test_empty_data(self):
        uf = InMemoryUnionFind()
        data = InMemoryCache('test')
        data.open()

        result = list(self.wf.get_duplicates_cits(uf, data, self.messages))
        self.assertEqual(result, [])
        data.close()


if __name__ == '__main__':
    unittest.main()
