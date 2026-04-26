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

from re import match, search, sub, findall
from roman import fromRoman, InvalidRomanNumeralError
from oc_validator.helper import Helper
from oc_validator.lmdb_cache import LmdbCache, LmdbUnionFind, InMemoryCache, InMemoryUnionFind
from json import load
from os.path import join, dirname, abspath
from typing import Generator, List, Union

class Wellformedness:
    """
    Provides well-formedness checks for every field of META-CSV and CITS-CSV rows.

    Each method validates the format of a single field type (IDs, dates, venues,
    pages, etc.) against the OpenCitations CSV specification.
    """

    def __init__(self) -> None:
        """
        Initialise the Wellformedness checker and load the ID-type alignment configuration.

        :rtype: None
        """
        self.helper = Helper()
        self.br_id_schemes = ['doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia', 'openalex', 'temp', 'local', 'omid', 'jid', 'arxiv']
        self.br_id_schemes_for_venues = ['doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia', 'openalex', 'omid', 'jid', 'arxiv']
        self.ra_id_schemes = ['crossref', 'orcid', 'viaf', 'wikidata', 'ror', 'omid']
        with open(join(dirname(abspath(__file__)), 'id_type_alignment.json'), 'r', encoding='utf-8') as fa:
            self.id_type_dict = load(fa)


    def wellformedness_br_id(self, id_element: str) -> bool:
        """
        Validate the well-formedness of a single bibliographic-resource ID element.

        Checks that the element matches ``<scheme>:<value>`` where *scheme* is one
        of the recognised bibliographic-resource ID schemes.

        :param id_element: A single ID string (e.g. ``"doi:10.1234/abc"``).
        :type id_element: str
        :return: ``True`` if the element is well-formed, ``False`` otherwise.
        :rtype: bool
        """
        id_pattern = fr'^({"|".join(self.br_id_schemes)}):\S+$'
        if match(id_pattern, id_element):
            return True
        else:
            return False

    def wellformedness_people_item(self, ra_item: str) -> bool:
        """
        Validate the well-formedness of an item inside the ``author`` or ``editor`` field.

        An item may be a name, a name followed by bracketed IDs, or just bracketed IDs,
        conforming to the META-CSV syntax.

        :param ra_item: The raw string of a single author/editor item.
        :type ra_item: str
        :return: ``True`` if well-formed, ``False`` otherwise.
        :rtype: bool
        """
        #  todo: create stricter regex for not allowing characters that are likely to be illegal in a person's name/surname
        #   (e.g. digits, apostrophe, underscore, full-stop, etc.)
        outside_brackets = r'(?:[^\s,;\[\]]+(?:\s[^\s,;\[\]]+)*),?(?:\s[^\s,;\[\]]+)*'
        inside_brackets = fr'\[({"|".join(self.ra_id_schemes)}):\S+(?:\s({"|".join(self.ra_id_schemes)}):\S+)*\]'
        ra_item_pattern = fr'^(?:({outside_brackets}\s{inside_brackets})|({outside_brackets}\s?)|({inside_brackets}))$'

        if match(ra_item_pattern, ra_item):
            return True
        else:
            return False

    def wellformedness_publisher_item(self, ra_item: str) -> bool:
        """
        Validate the well-formedness of an item inside the ``publisher`` field.

        Unlike :meth:`wellformedness_people_item`, this allows commas in the
        name portion because publisher names may contain them.

        :param ra_item: The raw string of a single publisher item.
        :type ra_item: str
        :return: ``True`` if well-formed, ``False`` otherwise.
        :rtype: bool
        """
        outside_brackets_pub = r'(?:[^\s\[\]]+(?:\s[^\s\[\]]+)*)'
        inside_brackets = fr'\[({"|".join(self.ra_id_schemes)}):\S+(?:\s({"|".join(self.ra_id_schemes)}):\S+)*\]'
        ra_item_pattern = fr'^(?:({outside_brackets_pub}\s{inside_brackets})|({outside_brackets_pub}\s?)|({inside_brackets}))$'

        if match(ra_item_pattern, ra_item):
            return True
        else:
            return False

    def orphan_ra_id(self, ra_item: str) -> bool:
        """
        Detect responsible-agent IDs that appear outside square brackets.

        Returns ``True`` if the input string is likely to contain one or more
        RA IDs not enclosed in ``[]``, which would indicate a formatting issue.

        :param ra_item: The item inside an RA field, as split by the ``'; '`` separator.
        :type ra_item: str
        :return: ``True`` if an orphan ID is found (likely not well-formed),
            ``False`` if no match is found.
        :rtype: bool
        """
        if search(fr'({"|".join(self.ra_id_schemes)}):', sub(r'\[.*\]', '', ra_item)):
            return True
        else:
            return False

    def wellformedness_date(self, date_field: str) -> bool:
        """
        Validate the well-formedness of a date string.

        Accepted formats are ``YYYY`` or ``YYYY-MM`` or ``YYYY-MM-DD``.

        :param date_field: The raw date string from a date field.
        :type date_field: str
        :return: ``True`` if the date is well-formed, ``False`` otherwise.
        :rtype: bool
        """
        date_pattern = r'^((?:\d{4}\-(?:0[1-9]|1[012])(?:\-(?:0[1-9]|[12][0-9]|3[01]))?)|(?:\d{4}))$'
        if match(date_pattern, date_field):
            return True
        else:
            return False

    def wellformedness_venue(self, venue_value: str) -> bool:
        """
        Validate the well-formedness of the ``venue`` field value.

        The venue may be a name, a name followed by bracketed IDs, or just
        bracketed IDs, using bibliographic-resource ID schemes.

        :param venue_value: The raw venue string.
        :type venue_value: str
        :return: ``True`` if well-formed, ``False`` otherwise.
        :rtype: bool
        """
        outside_brackets_venue = r'(?:[^\s\[\]]+(?:\s[^\s\[\]]+)*)'
        # pmcids are not valid identifiers for 'venues'!
        inside_brackets_venue = fr'\[({"|".join(self.br_id_schemes_for_venues)}):\S+(?:\s({"|".join(self.br_id_schemes_for_venues)}):\S+)*\]'
        venue_pattern = fr'^(?:({outside_brackets_venue}\s{inside_brackets_venue})|({outside_brackets_venue}\s?)|({inside_brackets_venue}))$'

        if match(venue_pattern, venue_value):
            return True
        else:
            return False

    def orphan_venue_id(self, venue_value: str) -> bool:
        """
        Detect venue IDs that appear outside square brackets.

        Returns ``True`` if the input string likely contains one or more
        bibliographic-resource IDs not enclosed in ``[]``.

        :param venue_value: The raw value of the ``venue`` field.
        :type venue_value: str
        :return: ``True`` if an orphan ID is found, ``False`` otherwise.
        :rtype: bool
        """
        if search(fr'({"|".join(self.br_id_schemes_for_venues)}):', sub(r'\[.*\]', '', venue_value)):
            return True
        else:
            return False

    def wellformedness_volume_issue(self, vi_value: str) -> bool:
        """
        Validate the well-formedness of a ``volume`` or ``issue`` field value.

        The value must be one or more non-whitespace tokens separated by single spaces.

        :param vi_value: The raw volume or issue string.
        :type vi_value: str
        :return: ``True`` if well-formed, ``False`` otherwise.
        :rtype: bool
        """
        vi_pattern = r'^\S+(?:\s\S+)*$'

        if match(vi_pattern, vi_value):
            return True
        else:
            return False

    def wellformedness_page(self, page_value: str) -> bool:
        """
        Validate the well-formedness of the ``page`` field value.

        Accepts numeric ranges (``1-10``), Roman numeral ranges (``i-x``),
        and alphanumeric page ranges (``a1-b2``).

        :param page_value: The raw page string.
        :type page_value: str
        :return: ``True`` if well-formed, ``False`` otherwise.
        :rtype: bool
        """
        # todo: create stricter regex for roman numerals and valid intervals
        # NB: incorrect roman numerals and impossible ranges (e.g. 200-20) still validate!
        natural_number = r'([1-9][0-9]*)'
        roman_numeral = r'([IiVvXxLlCcDdMm]+)'
        single_alphanum = r'((?:(?:[A-Za-z]|[α-ωΑ-Ω])?[1-9]\d*)|(?:[1-9]\d*(?:[A-Za-z]|[α-ωΑ-Ω])?))'
        normal_page_pattern = f'^(?:{natural_number}|{roman_numeral})-(?:{natural_number}|{roman_numeral})$'
        alphanum_page_pattern = f'^{single_alphanum}-{single_alphanum}$'

        if match(normal_page_pattern, page_value):
            return True
        elif match(alphanum_page_pattern, page_value):
            return True
        else:
            return False

    def check_page_interval(self, page_interval: str) -> bool:
        """
        Validate that the page interval is logically consistent.

        Verifies that the start page is less than or equal to the end page.
        Handles Arabic numerals, Roman numerals, and alphanumeric strings.

        :param page_interval: The value of the ``page`` field (e.g. ``"1-10"``).
        :type page_interval: str
        :return: ``True`` if the interval is valid or cannot be converted to
            integers, ``False`` if the interval is definitively invalid.
        :rtype: bool
        """
        
        def extract_segments(text):
            letters = findall(r'[a-zA-Z]+', text)
            numbers = findall(r'\d+', text)
            return letters, numbers
        
        both_num = page_interval.split('-')
        converted = []
        for num_str in both_num:
            if num_str.isnumeric():
                converted.append(int(num_str))
            else:
                try:
                    converted.append(fromRoman(num_str.upper()))
                except InvalidRomanNumeralError:
                    if both_num[0] == both_num[1]:
                        return True  # ignore cases with identical alphanumeric strings (e.g. "a12-a12")
                    
                    elif both_num[0].isalnum() and both_num[1].isalnum():
                        alph1, num1 = extract_segments(both_num[0])
                        alph2, num2 = extract_segments(both_num[1])
                        if [l for l in (alph1, num1, alph2, num2) if len(l)>1]:
                            return False # exclude strs with non-contiguous alphabetic segments (e.g. 'a123b-c456')
                        char1 = alph1[0].lower() if alph1 else ''
                        char2 = alph2[0].lower() if alph2 else ''
                        dig1 = int(num1[0]) if num1 else 0
                        dig2 = int(num2[0]) if num2 else 0
                        if ((char1 == char2) or (char1 and not char2)) and (dig1 <= dig2):
                            return True
                        return False
                    else:
                        return False

        if converted[0] <= converted[1]:
            return True
        else:
            return False

    def check_duplicate_ra_by_id(self, items: List) -> List[List[int]]:
        """
        Find in-field duplicates among author/editor items based on shared RA IDs.

        Two items are considered duplicates when they share at least one
        responsible-agent identifier (e.g. ``orcid:0000-0001``).

        :param items: List of :class:`~oc_validator.table_reader.AgentItem` objects.
        :type items: List
        :return: A list of duplicate groups. Each group is a sorted list of item
            indices that share at least one RA ID. An empty list means no duplicates.
        :rtype: List[List[int]]
        """
        pid_to_indices: dict = {}
        for idx, item in enumerate(items):
            for pid in item.ids:
                pid_to_indices.setdefault(pid, []).append(idx)

        seen_groups: set = set()
        result: List[List[int]] = []
        for indices in pid_to_indices.values():
            if len(indices) >= 2:
                group = tuple(sorted(set(indices)))
                if group not in seen_groups:
                    seen_groups.add(group)
                    result.append(list(group))

        return result

    def check_duplicate_publisher_by_raw(self, items: List) -> List[List[int]]:
        """
        Find in-field duplicates among publisher items based on raw string exact match.

        Two publisher items are considered duplicates when their raw string
        representations are identical.

        :param items: List of :class:`~oc_validator.table_reader.AgentItem` objects.
        :type items: List
        :return: A list of duplicate groups. Each group is a list of item indices
            whose raw strings are identical. An empty list means no duplicates.
        :rtype: List[List[int]]
        """
        raw_to_indices: dict = {}
        for idx, item in enumerate(items):
            raw_to_indices.setdefault(item._raw, []).append(idx)

        result: List[List[int]] = []
        for indices in raw_to_indices.values():
            if len(indices) >= 2:
                result.append(indices)

        return result

    def wellformedness_type(self, type_value: str) -> bool:
        """
        Validate the well-formedness of the ``type`` field value.

        The type must be one of the keys in the ID-type alignment dictionary.

        :param type_value: The raw type string.
        :type type_value: str
        :return: ``True`` if the type is recognised, ``False`` otherwise.
        :rtype: bool
        """

        if type_value in self.id_type_dict.keys():
            return True
        else:
            return False

    def get_missing_values(self, row: dict) -> dict:
        """
        Check whether a row has all required fields for its resource type.

        When the ``id`` field is empty or contains only ``temp:``/``local:`` IDs,
        certain other fields become mandatory depending on the ``type`` value.
        The returned dictionary maps field names to ``[0]`` (for fields that
        condition the requirement) or ``None`` (for missing fields).

        :param row: A dictionary representing a single CSV row.
        :type row: dict
        :return: Dictionary locating missing required fields. Empty if the row
            satisfies all requirements.
        :rtype: dict
        """

        # TODO: Consider using an external config file, as you do for checking id-type semantic alignment, since the list
        #  of accepted types might change/be extended frequently!

        missing = {}
        ids = row['id'].split(' ')
        internal_only_id = all(id.startswith('temp:') or id.startswith('local:') for id in ids)
        if not row['id'] or internal_only_id:  # ID value is missing or only temp/local IDs are specified

            if row['type']:  # ID is missing and 'type' is specified

                if row['type'] in ['book', 'dataset', 'data file', 'dissertation', 'edited book',
                                   'journal article', 'monograph', 'other', 'peer review', 'posted content',
                                   'web content', 'proceedings article', 'reference book', 'report']:
                    if not row['title']:
                        missing['type'] = [0]
                        missing['title'] = None
                    if not row['pub_date']:
                        missing['type'] = [0]
                        missing['pub_date'] = None
                    if not row['author'] and not row['editor']:
                        missing['type'] = [0]
                        if not row['author']:
                            missing['author'] = None
                        if not row['editor']:
                            missing['editor'] = None

                elif row['type'] in ['book chapter', 'book part', 'book section', 'book track', 'component',
                                     'reference entry']:
                    if not row['title']:
                        missing['type'] = [0]
                        missing['title'] = None
                    if not row['venue']:
                        missing['type'] = [0]
                        missing['venue'] = None

                elif row['type'] in ['book series', 'book set', 'journal', 'proceedings', 'proceedings series',
                                     'report series', 'standard', 'standard series']:
                    if not row['title']:
                        missing['type'] = [0]
                        missing['title'] = None

                elif row['type'] == 'journal issue':
                    if not row['venue']:
                        missing['type'] = [0]
                        missing['venue'] = None
                    if not row['title'] and not row['issue']:
                        missing['type'] = [0]
                        if not row['title']:
                            missing['title'] = None
                        if not row['issue']:
                            missing['issue'] = None

                elif row['type'] == 'journal volume':
                    if not row['venue']:
                        missing['type'] = [0]
                        missing['venue'] = None
                    if not row['title'] and not row['volume']:
                        missing['type'] = [0]
                        if not row['title']:
                            missing['title'] = None
                        if not row['volume']:
                            missing['volume'] = None

            else:

                if not row['title']:
                    missing['type'] = None
                    missing['title'] = None
                if not row['pub_date']:
                    missing['type'] = None
                    missing['pub_date'] = None
                if not row['author'] and not row['editor']:
                    missing['type'] = None
                    if not row['author']:
                        missing['author'] = None
                    if not row['editor']:
                        missing['editor'] = None

        # the 2 conditions below apply to any type of BR and regardless of an ID being specified
        # cfr. also docs/mandatory_fields.csv

        if row['volume'] and not row['venue']:
            missing['volume'] = [0]
            missing['venue'] = None

        if row['issue'] and not row['venue']:
            missing['issue'] = [0]
            missing['venue'] = None


        return missing

    # # THIS FUNCTION IS THE OLD FUNCTION TO GET DUPLICATES, KEPT HERE FOR REFERENCE.
    # def get_duplicates_cits(self, entities: list, data_dict: list, messages) -> list:
    #     """
    #     Creates a list of dictionaries containing the duplication error in the whole document, either within a row
    #     (self-citation) or between two or more rows (duplicate citations).
    #     :param entities: list containing sets of strings (the IDs), where each set corresponds to a bibliographic entity
    #     :param data_dict: the list of the document's rows, read as dictionaries
    #     :param messages: the dictionary containing the messages as they're read from the .yaml config file
    #     :return: list of dictionaries, each carrying full info about each duplication error within the document.
    #     """
    #     visited_dicts = []
    #     report = []
    #     for row_idx, row in enumerate(data_dict):
    #         citation = {'citing_id': '', 'cited_id': ''}

    #         citing_items = row['citing_id'].split(' ')
    #         for item in citing_items:
    #             if citation['citing_id'] == '':
    #                 for set_idx, set in enumerate(entities):
    #                     if item in set:  # mapping the single ID to its corresponding set representing the bibl. entity
    #                         citation['citing_id'] = set_idx
    #                         break

    #         cited_items = row['cited_id'].split(' ')
    #         for item in cited_items:
    #             if citation['cited_id'] == '':
    #                 for set_idx, set in enumerate(entities):
    #                     if item in set:  # mapping the single ID to its corresponding set representing the bibl. entity
    #                         citation['cited_id'] = set_idx
    #                         break

    #         # If a field contains only invalid items, it is not possible to map it to an entity set: process the row
    #         # only if both citing and cited are associated to an entity set, i.e. their value in the 'citation'
    #         # dictionary is not still an empty string (as it had been initialized).
    #         if citation['citing_id'] != '' and citation['cited_id'] != '':

    #             if citation['citing_id'] == citation['cited_id']:  # SELF-CITATION warning (an entity cites itself)
    #                 table = {
    #                     row_idx: {
    #                         'citing_id': [idx for idx in range(len(citing_items))],
    #                         'cited_id': [idx for idx in range(len(cited_items))]
    #                     }
    #                 }
    #                 message = messages['m4']
    #                 report.append(
    #                     self.helper.create_error_dict(validation_level='csv_wellformedness', error_type='warning',
    #                                                   message=message, error_label='self-citation', located_in='field',
    #                                                   table=table, valid=True))

    #             # SAVE CITATIONS BETWEEN ENTITIES IN A LIST.
    #             # Each citation is represented as a nested dictionary in which the key-values representing the entity-to-entity
    #             # citation are unique within the list, but the table representing the location of an INSTANCE of an
    #             # entity-to-entity citation is updated each time a new instance of such citation is found in the csv document.

    #             citation_table = {
    #                 row_idx: {
    #                     'citing_id': [idx for idx in range(len(citing_items))],
    #                     'cited_id': [idx for idx in range(len(cited_items))]
    #                 }
    #             }

    #             cit_info = {'citation': citation, 'table': citation_table}

    #             if not visited_dicts:  # just for the first round of the iteration (when visited_dicts is empty)
    #                 visited_dicts.append(cit_info)
    #             else:
    #                 for dict_idx, cit_dict in enumerate(visited_dicts):
    #                     if citation == cit_dict['citation']:
    #                         visited_dicts[dict_idx]['table'].update(cit_info['table'])
    #                         break
    #                     elif dict_idx == (len(visited_dicts) - 1):
    #                         visited_dicts.append(cit_info)

    #     for d in visited_dicts:
    #         if len(d['table']) > 1:  # if there's more than 1 row in table for a citation (duplicate rows error)
    #             table = d['table']
    #             message = messages['m5']

    #             report.append(
    #                 self.helper.create_error_dict(validation_level='csv_wellformedness', error_type='error',
    #                                               message=message, error_label='duplicate_citation', located_in='row',
    #                                               table=table))
    #     return report

    def get_duplicates_cits(self, uf: Union[LmdbUnionFind, InMemoryUnionFind], data_cache: Union[LmdbCache, InMemoryCache], messages: dict) -> Generator:
        """
        Find duplicate citations and self-citations in a CITS-CSV document.

        No new large structures are held in RAM: the citation-occurrence map is
        persisted in a temporary cache and iterated at the end to detect duplicates.

        :param uf: Union-Find populated with all IDs encountered during validation.
        :type uf: Union[LmdbUnionFind, InMemoryUnionFind]
        :param data_cache: Cache mapping ``str(row_idx)`` to a
            ``(citing_id_str, cited_id_str)`` tuple for every row.
        :type data_cache: Union[LmdbCache, InMemoryCache]
        :param messages: Error-message template dictionary (from ``messages.yaml``).
        :type messages: dict
        :return: Generator of error-dict objects.
        :rtype: Generator
        """
        # citation_map_cache: key = "citing_root\x00cited_root",
        #                     value = {row_idx: {'citing_id': [...], 'cited_id': [...]}}

        # Infer  from the type of data_cache whether to use an in-memory 
        # object or an LMDB cache to collect duplicate issues positions
        if isinstance(data_cache, InMemoryCache):
            res_caching = InMemoryCache
        else:
            res_caching = LmdbCache

        with res_caching('dup_cits_citation_map') as citation_map_cache:
            for str_idx, (citing_id, cited_id) in data_cache.items():
                row_idx = int(str_idx)
                citing_items = citing_id.split(' ')
                cited_items = cited_id.split(' ')

                # Find first registered citing / cited entity root (O(1) LMDB lookup each)
                citing_root = next(
                    (uf.find(item) for item in citing_items if item in uf), None
                )
                cited_root = next(
                    (uf.find(item) for item in cited_items if item in uf), None
                )

                if citing_root is None or cited_root is None:
                    continue  # row has no mappable entities — skip

                # SELF-CITATION: citing and cited entity are the same
                if citing_root == cited_root:
                    table = {
                        row_idx: {
                            'citing_id': list(range(len(citing_items))),
                            'cited_id': list(range(len(cited_items))),
                        }
                    }
                    yield self.helper.create_error_dict(
                        validation_level='csv_wellformedness',
                        error_type='warning',
                        message=messages['m4'],
                        error_label='self-citation',
                        located_in='field',
                        table=table,
                        valid=True,
                    )

                # Accumulate citation occurrences in LMDB (read-modify-write)
                cit_key = f'{citing_root}\x00{cited_root}'
                row_entry = {
                    row_idx: {
                        'citing_id': list(range(len(citing_items))),
                        'cited_id': list(range(len(cited_items))),
                    }
                }
                existing = citation_map_cache.get(cit_key)
                if existing is None:
                    citation_map_cache[cit_key] = row_entry
                else:
                    existing.update(row_entry)
                    citation_map_cache[cit_key] = existing

            # Second scan: yield errors for citations that appear more than once
            for _cit_key, table in citation_map_cache.items():
                if len(table) > 1:
                    yield self.helper.create_error_dict(
                        validation_level='csv_wellformedness',
                        error_type='error',
                        message=messages['m5'],
                        error_label='duplicate_citation',
                        located_in='row',
                        table=table,
                    )

    # # THIS FUNCTION IS THE OLD FUNCTION TO GET DUPLICATES, KEPT HERE FOR REFERENCE.
    # def get_duplicates_meta(self, entities: list, data_dict: list, messages) -> list:
    #     """
    #     Creates a list of dictionaries containing the duplication error in the whole document between two or more rows.
    #     :param entities: list containing sets of strings (the IDs), where each set corresponds to a bibliographic entity.
    #     :param data_dict: the list of the document's rows, read as dictionaries
    #     :param messages: the dictionary containing the messages as they're read from the .yaml config file
    #     :return: list of dictionaries, each carrying full info about each duplication error within the document.
    #     """
    #     visited_dicts = []
    #     report = []
    #     for row_idx, row in enumerate(data_dict):
    #         br = {'meta_id': None, 'table': {}}
    #         items = row['id'].split(' ')

    #         for item in items:
    #             if not br['meta_id']:
    #                 for set_idx, set in enumerate(entities):
    #                     if item in set:  # mapping the single ID to its corresponding set representing the bibl. entity
    #                         br['meta_id'] = str(set_idx)
    #                         br['table'] = {row_idx: {'id': list(range(len(items)))}}
    #                         break

    #         # process row only if a meta_id has been associated to it (i.e. id field contains at least one valid identifier)
    #         if br['meta_id']:
    #             if not visited_dicts:  # just for the first round of the iteration (when visited_dicts is empty)
    #                 visited_dicts.append(br)
    #             else:
    #                 for visited_br_idx, visited_br in enumerate(visited_dicts):
    #                     if br['meta_id'] == visited_br['meta_id']:
    #                         visited_dicts[visited_br_idx]['table'].update(br['table'])
    #                         break
    #                     elif visited_br_idx == (len(visited_dicts) - 1):
    #                         visited_dicts.append(br)

    #     for d in visited_dicts:
    #         if len(d['table']) > 1:  # if there's more than 1 row in table for a br (duplicate rows error)
    #             table = d['table']
    #             message = messages['m11']

    #             report.append(
    #                 self.helper.create_error_dict(validation_level='csv_wellformedness', error_type='error',
    #                                               message=message, error_label='duplicate_br', located_in='row',
    #                                               table=table))

    #     return report

    def get_duplicates_meta(self, uf: Union[LmdbUnionFind, InMemoryUnionFind], data_cache: Union[LmdbCache, InMemoryCache], messages: dict) -> Generator:
        """
        Find duplicate bibliographic entities in a META-CSV document.

        No new large structures are held in RAM: the entity-occurrence map is
        persisted in a temporary cache and iterated at the end to detect duplicates.

        :param uf: Union-Find populated with all IDs encountered during validation.
        :type uf: Union[LmdbUnionFind, InMemoryUnionFind]
        :param data_cache: Cache mapping ``str(row_idx)`` to the raw ``'id'``
            field string for every row.
        :type data_cache: Union[LmdbCache, InMemoryCache]
        :param messages: Error-message template dictionary (from ``messages.yaml``).
        :type messages: dict
        :return: Generator of error-dict objects.
        :rtype: Generator
        """
        # meta_map_cache: key = entity root string,
        #                 value = {row_idx: {'id': [0, 1, ...]}}

        # Infer  from the type of data_cache whether to use an in-memory 
        # object or an LMDB cache to collect duplicate issues positions
        if isinstance(data_cache, InMemoryCache):
            res_caching = InMemoryCache
        else:
            res_caching = LmdbCache

        with res_caching('dup_meta_entity_map') as meta_map_cache:
            for str_idx, id_value in data_cache.items():
                row_idx = int(str_idx)
                items = id_value.split(' ')

                # Find the first registered ID and resolve its entity root
                root = next(
                    (uf.find(item) for item in items if item in uf), None
                )
                if root is None:
                    continue  # row has no valid mapped IDs — skip

                row_table = {row_idx: {'id': list(range(len(items)))}}

                # Read-modify-write: accumulate occurrences per entity root
                existing:dict = meta_map_cache.get(root)
                if existing is None:
                    meta_map_cache[root] = row_table
                else:
                    existing.update(row_table)
                    meta_map_cache[root] = existing

            # Second scan: yield errors for entities that appear more than once
            for _root, table in meta_map_cache.items():
                if len(table) > 1:
                    yield self.helper.create_error_dict(
                        validation_level='csv_wellformedness',
                        error_type='error',
                        message=messages['m11'],
                        error_label='duplicate_br',
                        located_in='row',
                        table=table,
                    )
