# ISC License
#
# Copyright (c) 2023-2026, Elia Rizzetto, Silvio Peroni
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

import unittest
from oc_validator.table_reader import AgentItem, VenueInfo, MetadataRow, CitationsRow, read_metadata_row, read_citations_row


class TestAgentItem(unittest.TestCase):
    """Tests for the AgentItem class."""

    def test_parse_name_only(self):
        item = AgentItem('Smith, John')
        self.assertEqual(item.name, 'Smith, John')
        self.assertEqual(item.ids, [])

    def test_parse_name_with_single_id(self):
        item = AgentItem('Smith, John [orcid:0000-0001-2345-6789]')
        self.assertEqual(item.name, 'Smith, John')
        self.assertEqual(item.ids, ['orcid:0000-0001-2345-6789'])

    def test_parse_name_with_multiple_ids(self):
        item = AgentItem('Smith, John [orcid:0000-0001-2345-6789 wikidata:Q12345]')
        self.assertEqual(item.name, 'Smith, John')
        self.assertEqual(item.ids, ['orcid:0000-0001-2345-6789', 'wikidata:Q12345'])

    def test_parse_ids_only(self):
        item = AgentItem('[orcid:0000-0001-2345-6789]')
        self.assertEqual(item.ids, ['orcid:0000-0001-2345-6789'])
        self.assertEqual(item.name, '')

    def test_parse_crossref_id(self):
        item = AgentItem('Springer Science and Business Media LLC [crossref:297]')
        self.assertEqual(item.name, 'Springer Science and Business Media LLC')
        self.assertEqual(item.ids, ['crossref:297'])

    def test_parse_omid_id(self):
        item = AgentItem('Ovid Technologies (Wolters Kluwer Health) [omid:ra/0610116033 crossref:276]')
        self.assertIn('omid:ra/0610116033', item.ids)
        self.assertIn('crossref:276', item.ids)

    def test_to_dict(self):
        item = AgentItem('Smith [orcid:0000-0001-2345-6789]')
        d = item.to_dict()
        self.assertEqual(d['name'], 'Smith')
        self.assertEqual(d['ids'], ['orcid:0000-0001-2345-6789'])

    def test_repr(self):
        item = AgentItem('Smith [orcid:0000-0001-2345-6789]')
        r = repr(item)
        self.assertIn('AgentItem', r)
        self.assertIn('Smith', r)

    def test_str_returns_raw(self):
        raw = 'Smith, John [orcid:0000-0001-2345-6789]'
        item = AgentItem(raw)
        self.assertEqual(str(item), raw)

    def test_parse_empty_string(self):
        item = AgentItem('')
        self.assertEqual(item.name, '')
        self.assertEqual(item.ids, [])

    def test_parse_name_without_comma(self):
        item = AgentItem('Aristotle')
        self.assertEqual(item.name, 'Aristotle')
        self.assertEqual(item.ids, [])

    def test_parse_ror_id(self):
        item = AgentItem('An Org [ror:012345678]')
        self.assertEqual(item.ids, ['ror:012345678'])

    def test_parse_viaf_id(self):
        item = AgentItem('An Author [viaf:12345678]')
        self.assertEqual(item.ids, ['viaf:12345678'])


class TestVenueInfo(unittest.TestCase):
    """Tests for the VenueInfo class."""

    def test_parse_name_only(self):
        venue = VenueInfo('Nature')
        self.assertEqual(venue.name, 'Nature')
        self.assertEqual(venue.ids, [])

    def test_parse_name_with_single_id(self):
        venue = VenueInfo('Nature [issn:0028-0836]')
        self.assertEqual(venue.name, 'Nature')
        self.assertEqual(venue.ids, ['issn:0028-0836'])

    def test_parse_name_with_multiple_ids(self):
        venue = VenueInfo('IEEE Transactions on Magnetics [issn:0018-9464]')
        self.assertEqual(venue.name, 'IEEE Transactions on Magnetics')
        self.assertEqual(venue.ids, ['issn:0018-9464'])

    def test_parse_ids_only(self):
        venue = VenueInfo('[issn:0028-0836]')
        self.assertEqual(venue.ids, ['issn:0028-0836'])
        self.assertEqual(venue.name, '')

    def test_parse_isbn_venue(self):
        venue = VenueInfo('Insulation of High-Voltage Equipment [isbn:9783642058530 isbn:9783662079188]')
        self.assertIn('isbn:9783642058530', venue.ids)
        self.assertIn('isbn:9783662079188', venue.ids)

    def test_to_dict(self):
        venue = VenueInfo('Nature [issn:0028-0836]')
        d = venue.to_dict()
        self.assertEqual(d['name'], 'Nature')
        self.assertEqual(d['ids'], ['issn:0028-0836'])

    def test_repr(self):
        venue = VenueInfo('Nature [issn:0028-0836]')
        r = repr(venue)
        self.assertIn('VenueInfo', r)

    def test_str_returns_raw(self):
        raw = 'Nature [issn:0028-0836]'
        venue = VenueInfo(raw)
        self.assertEqual(str(venue), raw)

    def test_parse_empty(self):
        venue = VenueInfo('')
        self.assertEqual(venue.name, '')
        self.assertEqual(venue.ids, [])

    def test_parse_doi_venue(self):
        venue = VenueInfo('A Venue [doi:10.1234/abc]')
        self.assertEqual(venue.ids, ['doi:10.1234/abc'])

    def test_parse_url_venue(self):
        venue = VenueInfo('A Venue [url:https://example.com]')
        self.assertEqual(venue.ids, ['url:https://example.com'])

    def test_parse_wikidata_venue(self):
        venue = VenueInfo('A Venue [wikidata:Q12345]')
        self.assertEqual(venue.ids, ['wikidata:Q12345'])

    def test_parse_openalex_venue(self):
        venue = VenueInfo('A Venue [openalex:W1234567890]')
        self.assertEqual(venue.ids, ['openalex:W1234567890'])

    def test_parse_omid_venue(self):
        venue = VenueInfo('A Venue [omid:br/0610116033]')
        self.assertEqual(venue.ids, ['omid:br/0610116033'])

    def test_parse_jid_venue(self):
        venue = VenueInfo('A Venue [jid:jssog1981]')
        self.assertEqual(venue.ids, ['jid:jssog1981'])

    def test_pmcid_not_venue_id(self):
        """PMCID should not be recognized as a venue ID (per the venue ID schemes)."""
        venue = VenueInfo('A Venue [pmcid:PMC1234567]')
        self.assertEqual(venue.ids, [])

    def test_orcid_not_venue_id(self):
        """ORCID is an RA scheme, not a BR/venue scheme."""
        venue = VenueInfo('A Venue [orcid:0000-0001-2345-6789]')
        self.assertEqual(venue.ids, [])


class TestMetadataRow(unittest.TestCase):
    """Tests for the MetadataRow class."""

    def _make_row(self, **overrides):
        base = {
            'id': 'doi:10.1234/abc',
            'title': 'A Title',
            'author': 'Smith, John',
            'pub_date': '2020',
            'venue': 'Nature [issn:0028-0836]',
            'volume': '36',
            'issue': '4',
            'page': '1-10',
            'type': 'journal article',
            'publisher': 'Springer [crossref:297]',
            'editor': ''
        }
        base.update(overrides)
        return MetadataRow(base)

    def test_parse_basic_row(self):
        row = self._make_row()
        self.assertEqual(row.id, ['doi:10.1234/abc'])
        self.assertEqual(row.title, 'A Title')
        self.assertEqual(row.pub_date, '2020')
        self.assertEqual(row.volume, '36')
        self.assertEqual(row.issue, '4')
        self.assertEqual(row.page, '1-10')
        self.assertEqual(row.type, 'journal article')

    def test_parse_multiple_ids(self):
        row = self._make_row(id='doi:10.1234/abc pmid:12345')
        self.assertEqual(row.id, ['doi:10.1234/abc', 'pmid:12345'])

    def test_parse_author(self):
        row = self._make_row(author='Smith, John [orcid:0000-0001-2345-6789]; Doe, Jane')
        self.assertIsNotNone(row.author)
        self.assertEqual(len(row.author), 2)
        self.assertEqual(row.author[0].name, 'Smith, John')
        self.assertEqual(row.author[0].ids, ['orcid:0000-0001-2345-6789'])
        self.assertEqual(row.author[1].name, 'Doe, Jane')

    def test_parse_empty_author(self):
        row = self._make_row(author='')
        self.assertIsNone(row.author)

    def test_parse_venue(self):
        row = self._make_row(venue='Nature [issn:0028-0836]')
        self.assertIsNotNone(row.venue)
        self.assertEqual(row.venue.name, 'Nature')
        self.assertEqual(row.venue.ids, ['issn:0028-0836'])

    def test_parse_empty_venue(self):
        row = self._make_row(venue='')
        self.assertIsNone(row.venue)

    def test_parse_publisher(self):
        row = self._make_row(publisher='Springer [crossref:297]')
        self.assertIsNotNone(row.publisher)
        self.assertEqual(row.publisher[0].name, 'Springer')
        self.assertEqual(row.publisher[0].ids, ['crossref:297'])

    def test_parse_editor(self):
        row = self._make_row(editor='Doe, Jane [orcid:0000-0002-0000-0000]')
        self.assertIsNotNone(row.editor)
        self.assertEqual(len(row.editor), 1)

    def test_parse_empty_fields(self):
        row = self._make_row(id='', title='', author='', pub_date='', venue='',
                           volume='', issue='', page='', type='', publisher='', editor='')
        self.assertEqual(row.id, [])
        self.assertEqual(row.title, '')
        self.assertIsNone(row.author)
        self.assertIsNone(row.venue)
        self.assertIsNone(row.publisher)
        self.assertIsNone(row.editor)

    def test_flat_serialise(self):
        row = self._make_row()
        flat = row.flat_serialise()
        self.assertIn('id', flat)
        self.assertIn('title', flat)
        self.assertIn('author', flat)
        self.assertIn('venue', flat)
        self.assertIn('type', flat)
        self.assertIsInstance(flat['id'], list)
        self.assertIsInstance(flat['title'], list)

    def test_flat_serialise_empty_fields(self):
        row = self._make_row(author='', venue='', publisher='', editor='')
        flat = row.flat_serialise()
        self.assertEqual(flat['author'], [])
        self.assertEqual(flat['venue'], [])
        self.assertEqual(flat['publisher'], [])
        self.assertEqual(flat['editor'], [])

    def test_repr(self):
        row = self._make_row()
        r = repr(row)
        self.assertIn('MetadataRow', r)


class TestCitationsRow(unittest.TestCase):
    """Tests for the CitationsRow class."""

    def _make_row(self, **overrides):
        base = {
            'citing_id': 'doi:10.1234/a',
            'citing_publication_date': '2020',
            'cited_id': 'doi:10.5678/b',
            'cited_publication_date': '2019'
        }
        base.update(overrides)
        return CitationsRow(base)

    def test_parse_basic_row(self):
        row = self._make_row()
        self.assertEqual(row.citing_id, ['doi:10.1234/a'])
        self.assertEqual(row.cited_id, ['doi:10.5678/b'])
        self.assertEqual(row.citing_publication_date, '2020')
        self.assertEqual(row.cited_publication_date, '2019')

    def test_parse_multiple_citing_ids(self):
        row = self._make_row(citing_id='doi:10.1234/a pmid:12345')
        self.assertEqual(row.citing_id, ['doi:10.1234/a', 'pmid:12345'])

    def test_parse_empty_citing_id(self):
        row = self._make_row(citing_id='')
        self.assertEqual(row.citing_id, [])

    def test_parse_no_dates(self):
        row = CitationsRow({'citing_id': 'doi:10.1234/a', 'cited_id': 'doi:10.5678/b'})
        self.assertIsNone(row.citing_publication_date)
        self.assertIsNone(row.cited_publication_date)

    def test_parse_two_column_format(self):
        row = CitationsRow({'citing_id': 'doi:10.1234/a', 'cited_id': 'doi:10.5678/b'})
        self.assertEqual(row.citing_id, ['doi:10.1234/a'])
        self.assertEqual(row.cited_id, ['doi:10.5678/b'])

    def test_flat_serialise(self):
        row = self._make_row()
        flat = row.flat_serialise()
        self.assertIn('citing_id', flat)
        self.assertIn('cited_id', flat)
        self.assertIn('citing_publication_date', flat)
        self.assertIn('cited_publication_date', flat)
        self.assertIsInstance(flat['citing_id'], list)

    def test_flat_serialise_no_dates(self):
        row = CitationsRow({'citing_id': 'doi:10.1234/a', 'cited_id': 'doi:10.5678/b'})
        flat = row.flat_serialise()
        self.assertEqual(flat['citing_publication_date'], [])
        self.assertEqual(flat['cited_publication_date'], [])

    def test_repr(self):
        row = self._make_row()
        r = repr(row)
        self.assertIn('CitationsRow', r)


class TestReadMetadataRow(unittest.TestCase):
    """Tests for the read_metadata_row factory function."""

    def test_basic(self):
        row_dict = {
            'id': 'doi:10.1234/abc',
            'title': 'Test',
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
        result = read_metadata_row(row_dict)
        self.assertIsInstance(result, MetadataRow)
        self.assertEqual(result.id, ['doi:10.1234/abc'])

    def test_does_not_modify_input(self):
        row_dict = {
            'id': 'doi:10.1234/abc',
            'title': 'Test',
            'author': '',
            'pub_date': '',
            'venue': '',
            'volume': '',
            'issue': '',
            'page': '',
            'type': '',
            'publisher': '',
            'editor': ''
        }
        original = row_dict.copy()
        read_metadata_row(row_dict)
        self.assertEqual(row_dict, original)


class TestReadCitationsRow(unittest.TestCase):
    """Tests for the read_citations_row factory function."""

    def test_basic(self):
        row_dict = {
            'citing_id': 'doi:10.1234/a',
            'citing_publication_date': '2020',
            'cited_id': 'doi:10.5678/b',
            'cited_publication_date': '2019'
        }
        result = read_citations_row(row_dict)
        self.assertIsInstance(result, CitationsRow)
        self.assertEqual(result.citing_id, ['doi:10.1234/a'])

    def test_does_not_modify_input(self):
        row_dict = {
            'citing_id': 'doi:10.1234/a',
            'cited_id': 'doi:10.5678/b'
        }
        original = row_dict.copy()
        read_citations_row(row_dict)
        self.assertEqual(row_dict, original)


if __name__ == '__main__':
    unittest.main()
