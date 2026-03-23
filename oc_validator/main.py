# Copyright (c) 2023, OpenCitations <contact@opencitations.net>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from csv import DictReader, field_size_limit
from yaml import full_load
from json import load, dumps
from os.path import exists, join, dirname, abspath
from os import makedirs, getcwd
from re import finditer
import tempfile
import shutil
import lmdb
from oc_validator.helper import Helper, read_csv, CSVStreamReader, JSONLStreamIO
from oc_validator.csv_wellformedness import Wellformedness
from oc_validator.id_syntax import IdSyntax
from oc_validator.id_existence import IdExistence
from oc_validator.semantics import Semantics
from oc_validator.table_reader import read_metadata_row, read_citations_row
from oc_validator.lmdb_cache import LmdbCache, InMemoryCache, LmdbUnionFind
from tqdm import tqdm
from argparse import ArgumentParser
import logging


# --- Custom Exception classes. ---
class ValidationError(Exception):
    """Base class for errors related to the validation process."""
    pass

class InvalidTableError(ValidationError):
    """Raised when the submitted table cannot be identified as META-CSV or CITS-CSV, therefore cannot be processed."""
    def __init__(self, input_fp):
        super().__init__('The submitted table does not meet the required basic formatting standards. '
                         'Please ensure that both the metadata and citations tables are valid CSV files following the correct structure: '
                         'the metadata table must have the following columns: "id", "title", "author", "pub_date", "venue", "volume", "issue", "page", "type", "publisher", "editor"; '
                         'the citations table must have either 4 columns ("citing_id", "citing_publication_date", "cited_id", "cited_publication_date") or two columns ("citing_id","cited_id")'
                         'Refer to the documentation at https://github.com/opencitations/crowdsourcing/blob/main/README.md for the expected format and structure before resubmitting your deposit.')
        self.input_fp = input_fp

class TableNotMatchingInstance(ValidationError):
    """Raised when the table submitted for a specific Validator instance in ClosureValidator does not match the process validation type,
        e.g. a CITS-CSV table is submitted for an instance of Validator that is intended to process a META-CSV table.
    """
    def __init__(self, input_fp, detected_table_type, correct_table_type):
        super().__init__(f'The submitted table in file "{input_fp}" is of type {detected_table_type}, but should be of type {correct_table_type} instead.')
        self.input_fp = input_fp
        self.detected_table_type = detected_table_type
        self.correct_table_type = correct_table_type

# --- Class for the main process; validates one document at a time via the Validator.validate() method. ---
class Validator:
    def __init__(self, csv_doc: str, output_dir: str, use_meta_endpoint=False, verify_id_existence=True, 
                 memory_efficient=True, cache_path=None):
        """
        Initialize the Validator.
        
        :param csv_doc: Path to the CSV file to validate
        :param output_dir: Directory to store validation output
        :param use_meta_endpoint: Whether to use OC Meta endpoint for ID existence checks
        :param verify_id_existence: Whether to verify ID existence
        :param memory_efficient: If True, use LMDB for caching (recommended for large files)
        :param cache_path: Optional custom path for LMDB database
        """
        self.csv_doc = csv_doc
        self.csv_stream = CSVStreamReader(csv_doc)  # Use streaming instead of loading all data
        self.table_to_process = self.process_selector()
        self.helper = Helper()
        self.wellformed = Wellformedness()
        self.syntax = IdSyntax()
        self.existence = IdExistence(use_meta_endpoint=use_meta_endpoint)
        self.semantics = Semantics()
        script_dir = dirname(abspath(__file__))  # Directory where the script is located
        self.messages = full_load(open(join(script_dir, 'messages.yaml'), 'r', encoding='utf-8'))
        self.id_type_dict = load(open(join(script_dir, 'id_type_alignment.json'), 'r', encoding='utf-8'))
        self.output_dir = output_dir
        if not exists(self.output_dir):
            makedirs(self.output_dir)
        if self.table_to_process == 'meta_csv':
            self.output_fp_json = self._make_output_filepath('out_validate_meta', 'jsonl')
            self.output_fp_txt = self._make_output_filepath('meta_validation_summary', 'txt')
        elif self.table_to_process == 'cits_csv':
            self.output_fp_json = self._make_output_filepath('out_validate_cits', 'jsonl')
            self.output_fp_txt = self._make_output_filepath('cits_validation_summary', 'txt')
        
        # Initialize cache based on memory_efficient flag
        self.memory_efficient = memory_efficient
        cache_name = f'validator_{hash(csv_doc)}'
        if memory_efficient:
            self.id_cache = LmdbCache(cache_name, path=cache_path)
        else:
            self.id_cache = InMemoryCache(cache_name, path=cache_path)
        
        # Open the cache
        self.id_cache.open()
        
        self.verify_id_existence = verify_id_existence
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cache is properly closed."""
        self.close()
        return False
    
    def close(self):
        """Close the cache and clean up resources."""
        if hasattr(self, 'id_cache') and self.id_cache is not None:
            self.id_cache.close()

    def process_selector(self):
        """
        Detect the table type by streaming the first few rows.
        This is memory-efficient as it doesn't load the entire file.
        """
        # Read first few rows to determine table type
        sample_rows = []
        for i, row in enumerate(self.csv_stream):
            if i >= 10:  # Only need first 10 rows to determine type
                break
            sample_rows.append(row)
        
        if not sample_rows:
            raise InvalidTableError(self.csv_doc)
        
        process_type = None
        try:
            if all(set(row.keys()) == {"id", "title", "author", "pub_date", "venue", "volume", "issue", "page", "type",
                                        "publisher", "editor"} for row in sample_rows):
                process_type = 'meta_csv'
                return process_type
            elif all(set(row.keys()) == {'citing_id', 'citing_publication_date', 'cited_id', 'cited_publication_date'} for row in sample_rows):
                process_type = 'cits_csv'
                return process_type
            elif all(set(row.keys()) == {'citing_id', 'cited_id'} for row in sample_rows): # support also Index tables with no publication dates
                process_type = 'cits_csv'
                return process_type
            else:
                raise InvalidTableError(self.csv_doc)
        except KeyError:
            raise InvalidTableError(self.csv_doc)
        
    def _make_output_filepath(self, base_filename, extension):
        """
        Generates a unique output filepath, checks if a file with the same name exists, and if so appends an incrementing number.
        """
        
        full_path = join(self.output_dir, f"{base_filename}.{extension}")
        counter = 1

        # If filepath already exists, increment the counter and check for existing files
        while exists(full_path):
            full_path = join(self.output_dir, f"{base_filename}_{counter}.{extension}")
            counter += 1
        
        return full_path

    def validate(self) -> bool:
        if self.table_to_process == 'meta_csv':
            return self.validate_meta()
        elif self.table_to_process == 'cits_csv':
            return self.validate_cits()

    def _collect_meta_duplicate_data(self):
        """
        First pass: Collect minimal data needed for duplicate detection.

        Returns a tuple ``(LmdbCache, int)`` where the cache maps
        ``str(row_idx)`` to the raw ``'id'`` field string and the integer is
        the total row count.  The caller is responsible for closing the cache
        when it is no longer needed.
        """
        dup_cache = LmdbCache(f'dup_meta_{abs(hash(self.csv_doc))}')
        dup_cache.open()
        row_count = 0
        for row_idx, row in enumerate(tqdm(self.csv_stream.stream(), desc="First pass: Collecting IDs")):
            dup_cache[str(row_idx)] = row.get('id', '')
            row_count += 1
        return (dup_cache, row_count)
    
    def validate_meta(self) -> bool:
        """
        Validate an instance of META-CSV using JSON-Lines streaming output
        :return: the list of errors, i.e. the report of the validation process
        """
        messages = self.messages
        id_type_dict = self.id_type_dict

        # Set up LMDB Union-Find for entity grouping (replaces in-memory br_id_groups list)
        uf_tmp_dir = tempfile.mkdtemp(prefix='uf_meta_')
        uf_env = lmdb.open(uf_tmp_dir, map_size=2 * 1024 ** 3, sync=False, metasync=False)
        uf = LmdbUnionFind(uf_env)
        duplicate_data_cache = None

        # Open JSON-L file for streaming output
        with JSONLStreamIO(self.output_fp_json, 'a') as jsonl_file:
            # First pass: Collect data for duplicate detection (LMDB-backed)
            duplicate_data_cache, rows_count = self._collect_meta_duplicate_data()
            
            # Second pass: Stream validation
            for row_idx, row in enumerate(tqdm(self.csv_stream.stream(), desc="Second pass: Validating", total=rows_count)):
                row_ok = True  # switch for row well-formedness
                id_ok = True  # switch for id field well-formedness
                type_ok = True  # switch for type field well-formedness

                missing_required_fields = self.wellformed.get_missing_values(
                    row)  # dict w/ positions of error in row; empty if row is fine
                if missing_required_fields:
                    message = messages['m17']
                    table = {row_idx: missing_required_fields}
                    error = self.helper.create_error_dict(
                                validation_level='csv_wellformedness',
                                error_type='error',
                                message=message,
                                error_label='required_fields',
                                located_in='field',
                                table=table)
                    jsonl_file.write(error)
                    row_ok = False

                # Parse row into structured object
                row_obj = read_metadata_row(row)

                for field, value in row.items():

                    if field == 'id':
                        # Use structured object's parsed id field
                        items = row_obj.id
                        if items:
                            br_ids_set = set()  # set where to put well-formed br IDs only

                            for item_idx, item in enumerate(items):

                                if item == '':
                                    message = messages['m1']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='error',
                                                                          message=message,
                                                                          error_label='extra_space',
                                                                          located_in='item',
                                                                          table=table)
                                    jsonl_file.write(error)

                                elif not self.wellformed.wellformedness_br_id(item):
                                    message = messages['m2']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='error',
                                                                          message=message,
                                                                          error_label='br_id_format',
                                                                          located_in='item',
                                                                          table=table)
                                    jsonl_file.write(error)

                                else:
                                    if item not in br_ids_set:
                                        br_ids_set.add(item)
                                    else:  # in-field duplication of the same ID
                                        table = {row_idx: {field: [i for i, v in enumerate(items) if v == item]}}
                                        message = messages['m6']

                                        error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='duplicate_id',
                                                                              located_in='item',
                                                                              table=table)  # valid=False
                                        jsonl_file.write(error)

                                    #  2nd validation level: EXTERNAL SYNTAX OF ID (BIBLIOGRAPHIC RESOURCE)
                                    if not self.syntax.check_id_syntax(item):
                                        message = messages['m19']
                                        table = {row_idx: {field: [item_idx]}}
                                        error = self.helper.create_error_dict(validation_level='external_syntax',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='br_id_syntax',
                                                                              located_in='item',
                                                                              table=table)
                                        jsonl_file.write(error)
                                    #  3rd validation level: EXISTENCE OF ID (BIBLIOGRAPHIC RESOURCE)
                                    else:
                                        if self.verify_id_existence: # if verify_id_existence is False just skip these operations
                                            message = messages['m20']
                                            table = {row_idx: {field: [item_idx]}}
                                            if item not in self.id_cache:
                                                if not self.existence.check_id_existence(item):
                                                    error = self.helper.create_error_dict(validation_level='existence',
                                                                                    error_type='warning',
                                                                                    message=message,
                                                                                    error_label='br_id_existence',
                                                                                    located_in='item',
                                                                                    table=table, valid=True)
                                                    jsonl_file.write(error)
                                                    self.id_cache[item] = False
                                                else:
                                                    self.id_cache[item] = True
                                            elif self.id_cache[item] is False:
                                                error = self.helper.create_error_dict(validation_level='existence',
                                                                                error_type='warning',
                                                                                message=message,
                                                                                error_label='br_id_existence',
                                                                                located_in='item',
                                                                                table=table, valid=True)
                                                jsonl_file.write(error)

                            if len(br_ids_set) >= 1:
                                # Register IDs in LMDB Union-Find (replaces in-memory br_id_groups list)
                                ids_list = list(br_ids_set)
                                uf.find(ids_list[0])  # ensure single-member entities are registered
                                for _ui in range(1, len(ids_list)):
                                    uf.union(ids_list[0], ids_list[_ui])

                            if len(br_ids_set) != len(items):  # --> some well-formedness error occurred in the id field
                                id_ok = False

                    if field == 'title':
                        if value:
                            if value.isupper():
                                message = messages['m8']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='warning',
                                                                      message=message,
                                                                      error_label='uppercase_title',
                                                                      located_in='item',
                                                                      table=table,
                                                                      valid=True)
                                jsonl_file.write(error)

                    if field == 'author' or field == 'editor':
                        # Use structured object's parsed field
                        if field == 'author':
                            agents = row_obj.author
                        else:  # field == 'editor'
                            agents = row_obj.editor
                        
                        if agents:
                            resp_agents = set()
                            items = agents  # Already parsed list of AgentItem objects

                            for item_idx, item in enumerate(items):
                                # Check orphan RA ID using the raw string
                                if self.wellformed.orphan_ra_id(item._raw):
                                    message = messages['m10']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='warning',
                                                                          message=message,
                                                                          error_label='orphan_ra_id',
                                                                          located_in='item',
                                                                          table=table,
                                                                          valid=True)
                                    jsonl_file.write(error)

                                # Validate using the raw string
                                if not self.wellformed.wellformedness_people_item(item._raw):
                                    message = messages['m9']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='error',
                                                                          message=message,
                                                                          error_label='people_item_format',
                                                                          located_in='item',
                                                                          table=table)
                                    jsonl_file.write(error)

                                else:
                                    if item._raw not in resp_agents:
                                        resp_agents.add(item._raw)
                                    else:  # in-field duplication of the same author/editor
                                        table = {row_idx: {field: [i for i, v in enumerate(items) if v._raw == item._raw]}}
                                        message = messages['m26']

                                        error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='duplicate_ra',
                                                                              located_in='item',
                                                                              table=table)  # valid=False
                                        jsonl_file.write(error)

                                    # Use structured object's ids attribute
                                    ids = item.ids

                                    for id in ids:
                                        #  2nd validation level: EXTERNAL SYNTAX OF ID (RESPONSIBLE AGENT)
                                        if not self.syntax.check_id_syntax(id):
                                            message = messages['m21']
                                            table = {row_idx: {field: [item_idx]}}
                                            error = self.helper.create_error_dict(validation_level='external_syntax',
                                                                                  error_type='error',
                                                                                  message=message,
                                                                                  error_label='ra_id_syntax',
                                                                                  located_in='item',
                                                                                  table=table)
                                            jsonl_file.write(error)
                                        #  3rd validation level: EXISTENCE OF ID (RESPONSIBLE AGENT)
                                        else:
                                            if self.verify_id_existence: # if verify_id_existence is False just skip these operations
                                                message = messages['m22']
                                                table = {row_idx: {field: [item_idx]}}
                                                if id not in self.id_cache:
                                                    if not self.existence.check_id_existence(id):
                                                        error = self.helper.create_error_dict(validation_level='existence',
                                                                                        error_type='warning',
                                                                                        message=message,
                                                                                        error_label='ra_id_existence',
                                                                                        located_in='item',
                                                                                        table=table,
                                                                                        valid=True)
                                                        jsonl_file.write(error)
                                                        self.id_cache[id] = False
                                                    else:
                                                        self.id_cache[id] = True
                                                elif self.id_cache[id] is False:
                                                    error = self.helper.create_error_dict(validation_level='existence',
                                                                                error_type='warning',
                                                                                message=message,
                                                                                error_label='ra_id_existence',
                                                                                located_in='item',
                                                                                table=table,
                                                                                valid=True)
                                                    jsonl_file.write(error)
                    if field == 'pub_date':
                        if value:
                            if not self.wellformed.wellformedness_date(value):
                                message = messages['m3']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='date_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)

                    if field == 'venue':
                        # Use structured object's parsed field
                        venue = row_obj.venue
                        if venue:

                            # Check orphan venue ID using the raw string
                            if self.wellformed.orphan_venue_id(venue._raw):
                                message = messages['m15']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='warning',
                                                                      message=message,
                                                                      error_label='orphan_venue_id',
                                                                      located_in='item',
                                                                      table=table,
                                                                      valid=True)
                                jsonl_file.write(error)

                            # Validate using the raw string
                            if not self.wellformed.wellformedness_venue(venue._raw):
                                message = messages['m12']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='venue_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)

                            else:
                                # Use structured object's ids attribute
                                ids = venue.ids

                                for id in ids:

                                    #  2nd validation level: EXTERNAL SYNTAX OF ID (BIBLIOGRAPHIC RESOURCE)
                                    if not self.syntax.check_id_syntax(id):
                                        message = messages['m19']
                                        table = {row_idx: {field: [0]}}
                                        error = self.helper.create_error_dict(validation_level='external_syntax',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='br_id_syntax',
                                                                              located_in='item',
                                                                              table=table)
                                        jsonl_file.write(error)
                                    #  3rd validation level: EXISTENCE OF ID (BIBLIOGRAPHIC RESOURCE)
                                    else:
                                        if self.verify_id_existence: # if verify_id_existence is False just skip these operations
                                            message = messages['m20']
                                            table = {row_idx: {field: [0]}}
                                            if id not in self.id_cache:
                                                if not self.existence.check_id_existence(id):
                                                    error = self.helper.create_error_dict(validation_level='existence',
                                                                                        error_type='warning',
                                                                                        message=message,
                                                                                        error_label='br_id_existence',
                                                                                        located_in='item',
                                                                                        table=table,
                                                                                        valid=True)
                                                    jsonl_file.write(error)
                                                    self.id_cache[id] = False
                                                else:
                                                    self.id_cache[id] = True
                                            elif self.id_cache[id] is False:
                                                error = self.helper.create_error_dict(validation_level='existence',
                                                                                error_type='warning',
                                                                                message=message,
                                                                                error_label='br_id_existence',
                                                                                located_in='item',
                                                                                table=table,
                                                                                valid=True)
                                                jsonl_file.write(error)

                    if field == 'volume':
                        if value:
                            if not self.wellformed.wellformedness_volume_issue(value):
                                message = messages['m13']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='volume_issue_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)

                    if field == 'issue':
                        if value:
                            if not self.wellformed.wellformedness_volume_issue(value):
                                message = messages['m13']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='volume_issue_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)

                    if field == 'page':
                        if value:
                            if not self.wellformed.wellformedness_page(value):
                                message = messages['m14']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='page_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)
                            else:
                                if not self.wellformed.check_page_interval(value):
                                    message = messages['m18']
                                    table = {row_idx: {field: [0]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='warning',
                                                                      message=message,
                                                                      error_label='page_interval',
                                                                      located_in='item',
                                                                      table=table,
                                                                      valid=True)
                                    jsonl_file.write(error)

                    if field == 'type':
                        if value:
                            if not self.wellformed.wellformedness_type(value):
                                message = messages['m16']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='type_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)

                                type_ok = False

                    if field == 'publisher':
                        # Use structured object's parsed field
                        publishers = row_obj.publisher
                        if publishers:
                            resp_agents = set()
                            items = publishers  # Already parsed list of AgentItem objects
                            for item_idx, item in enumerate(items):
                                # Check orphan RA ID using the raw string
                                if self.wellformed.orphan_ra_id(item._raw):
                                    message = messages['m10']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='warning',
                                                                          message=message,
                                                                          error_label='orphan_ra_id',
                                                                          located_in='item',
                                                                          table=table,
                                                                          valid=True)
                                    jsonl_file.write(error)

                                # Validate using the raw string
                                if not self.wellformed.wellformedness_publisher_item(item._raw):
                                    message = messages['m9']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='error',
                                                                          message=message,
                                                                          error_label='publisher_format',
                                                                          located_in='item',
                                                                          table=table)
                                    jsonl_file.write(error)

                                else:
                                    if item._raw not in resp_agents:
                                        resp_agents.add(item._raw)
                                    else:  # in-field duplication of the same publisher
                                        table = {row_idx: {field: [i for i, v in enumerate(items) if v._raw == item._raw]}}
                                        message = messages['m26']

                                        error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='duplicate_ra',
                                                                              located_in='item',
                                                                              table=table)  # valid=False
                                        jsonl_file.write(error)

                                    # Use structured object's ids attribute
                                    ids = item.ids

                                    for id in ids:

                                        #  2nd validation level: EXTERNAL SYNTAX OF ID (RESPONSIBLE AGENT)
                                        if not self.syntax.check_id_syntax(id):
                                            message = messages['m21']
                                            table = {row_idx: {field: [item_idx]}}
                                            error = self.helper.create_error_dict(validation_level='external_syntax',
                                                                                  error_type='error',
                                                                                  message=message,
                                                                                  error_label='ra_id_syntax',
                                                                                  located_in='item',
                                                                                  table=table)
                                            jsonl_file.write(error)
                                        #  3rd validation level: EXISTENCE OF ID (RESPONSIBLE AGENT)
                                        else:
                                            if self.verify_id_existence: # if verify_id_existence is False just skip these operations
                                                message = messages['m22']
                                                table = {row_idx: {field: [item_idx]}}
                                                if id not in self.id_cache:
                                                    if not self.existence.check_id_existence(id):
                                                        error = self.helper.create_error_dict(validation_level='existence',
                                                                                        error_type='warning',
                                                                                        message=message,
                                                                                        error_label='ra_id_existence',
                                                                                        located_in='item',
                                                                                        table=table,
                                                                                        valid=True)
                                                        jsonl_file.write(error)
                                                        self.id_cache[id] = False
                                                    else:
                                                        self.id_cache[id] = True
                                                elif self.id_cache[id] is False:
                                                    error = self.helper.create_error_dict(validation_level='existence',
                                                                                error_type='warning',
                                                                                message=message,
                                                                                error_label='ra_id_existence',
                                                                                located_in='item',
                                                                                table=table,
                                                                                valid=True)
                                                    jsonl_file.write(error)

                if row_ok and id_ok and type_ok:  # row semantics is checked only when the involved parts are well-formed

                    invalid_semantics = self.semantics.check_semantics(row, id_type_dict)
                    if invalid_semantics:
                        message = messages['m23']
                        table = {row_idx: invalid_semantics}
                        error = self.helper.create_error_dict(validation_level='semantics',
                                                      error_type='error',
                                                      message=message,
                                                      error_label='row_semantics',
                                                      located_in='field',
                                                      table=table)
                        jsonl_file.write(error)

            # GET DUPLICATE BIBLIOGRAPHIC ENTITIES (LMDB-backed, no in-memory entity list needed)
            duplicate_report = self.wellformed.get_duplicates_meta(
                uf=uf, data_cache=duplicate_data_cache, messages=messages)
            for error in duplicate_report:
                jsonl_file.write(error)

        # Cleanup LMDB resources used for duplicate detection
        uf_env.close()
        shutil.rmtree(uf_tmp_dir, ignore_errors=True)
        if duplicate_data_cache is not None:
            duplicate_data_cache.close()

        # write human-readable validation summary to txt file
        textual_report_stream= self.helper.create_validation_summary_stream(self.output_fp_json)
        with open(self.output_fp_txt, 'w', encoding='utf-8') as f:
            for l in textual_report_stream:
                f.write(l)

        is_valid = JSONLStreamIO(self.output_fp_json).is_empty()
        return is_valid

    def _collect_cits_duplicate_data(self):
        """
        First pass: Collect minimal data needed for duplicate detection in CITS-CSV.

        Returns a tuple ``(LmdbCache, int)`` where the cache maps
        ``str(row_idx)`` to a ``(citing_id_str, cited_id_str)`` tuple and the
        integer is the total row count.  The caller is responsible for closing
        the cache when it is no longer needed.
        """
        dup_cache = LmdbCache(f'dup_cits_{abs(hash(self.csv_doc))}')
        dup_cache.open()
        row_count = 0
        for row_idx, row in enumerate(tqdm(self.csv_stream.stream(), desc="First pass: Collecting IDs")):
            dup_cache[str(row_idx)] = (row.get('citing_id', ''), row.get('cited_id', ''))
            row_count += 1
        return (dup_cache, row_count)

    def validate_cits(self) -> bool:
        """
        Validates an instance of CITS-CSV using JSON-Lines streaming output
        :return: the list of errors, i.e. the report of the validation process
        """
        messages = self.messages

        # Set up LMDB Union-Find for entity grouping (replaces in-memory id_fields_instances list)
        uf_tmp_dir = tempfile.mkdtemp(prefix='uf_cits_')
        uf_env = lmdb.open(uf_tmp_dir, map_size=100 * 1024 * 1024, sync=False, metasync=False)
        uf = LmdbUnionFind(uf_env)
        duplicate_data_cache = None

        # Open JSON-L file for streaming output
        with JSONLStreamIO(self.output_fp_json, 'a') as jsonl_file:
            # First pass: Collect data for duplicate detection (LMDB-backed)
            duplicate_data_cache, rows_count = self._collect_cits_duplicate_data()
            
            # Second pass: Stream validation
            for row_idx, row in enumerate(tqdm(self.csv_stream.stream(), desc="Second pass: Validating", total=rows_count)):
                # Parse row into structured object
                row_obj = read_citations_row(row)
                
                for field, value in row.items():
                    if field == 'citing_id' or field == 'cited_id':
                        # Use structured object's parsed field
                        if field == 'citing_id':
                            items = row_obj.citing_id
                        else:  # field == 'cited_id'
                            items = row_obj.cited_id
                        
                        if not items:  # Check required fields
                            message = messages['m7']
                            table = {row_idx: {field: None}}
                            error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                  error_type='error',
                                                                  message=message,
                                                                  error_label='required_value_cits',
                                                                  located_in='field',
                                                                  table=table)
                            jsonl_file.write(error)
                        else:  # i.e. if string is not empty...
                            ids_set = set()  # set where to put valid IDs only

                            for item_idx, item in enumerate(items):

                                if item == '':
                                    message = messages['m1']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='error',
                                                                          message=message,
                                                                          error_label='extra_space',
                                                                          located_in='item',
                                                                          table=table)
                                    jsonl_file.write(error)

                                elif not self.wellformed.wellformedness_br_id(item):
                                    message = messages['m2']
                                    table = {row_idx: {field: [item_idx]}}
                                    error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                          error_type='error',
                                                                          message=message,
                                                                          error_label='br_id_format',
                                                                          located_in='item',
                                                                          table=table)
                                    jsonl_file.write(error)

                                else:
                                    if item not in ids_set:
                                        ids_set.add(item)
                                    else:  # in-field duplication of the same ID

                                        table = {row_idx: {field: [i for i, v in enumerate(items) if v == item]}}
                                        message = messages['m6']

                                        error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='duplicate_id',
                                                                              located_in='item',
                                                                              table=table)  # 'valid'=False
                                        jsonl_file.write(error)
                                    #  2nd validation level: EXTERNAL SYNTAX OF ID (BIBLIOGRAPHIC RESOURCE)
                                    if not self.syntax.check_id_syntax(item):
                                        message = messages['m19']
                                        table = {row_idx: {field: [item_idx]}}
                                        error = self.helper.create_error_dict(validation_level='external_syntax',
                                                                              error_type='error',
                                                                              message=message,
                                                                              error_label='br_id_syntax',
                                                                              located_in='item',
                                                                              table=table)
                                        jsonl_file.write(error)
                                    #  3rd validation level: EXISTENCE OF ID (BIBLIOGRAPHIC RESOURCE)
                                    else:
                                        if self.verify_id_existence: # if verify_id_existence is False just skip these operations
                                            message = messages['m20']
                                            table = {row_idx: {field: [item_idx]}}
                                            if item not in self.id_cache:
                                                if not self.existence.check_id_existence(item):
                                                    error = self.helper.create_error_dict(validation_level='existence',
                                                                                        error_type='warning',
                                                                                        message=message,
                                                                                        error_label='br_id_existence',
                                                                                        located_in='item',
                                                                                        table=table, valid=True)
                                                    jsonl_file.write(error)
                                                    self.id_cache[item] = False
                                                else:
                                                    self.id_cache[item] = True
                                            elif self.id_cache[item] is False:
                                                error = self.helper.create_error_dict(validation_level='existence',
                                                                                error_type='warning',
                                                                                message=message,
                                                                                error_label='br_id_existence',
                                                                                located_in='item',
                                                                                table=table, valid=True)
                                                jsonl_file.write(error)

                            if len(ids_set) >= 1:
                                # Register IDs in LMDB Union-Find (replaces in-memory id_fields_instances list)
                                ids_list = list(ids_set)
                                uf.find(ids_list[0])  # ensure single-member entities are registered
                                for _ui in range(1, len(ids_list)):
                                    uf.union(ids_list[0], ids_list[_ui])

                    if field == 'citing_publication_date' or field == 'cited_publication_date':
                        if value:
                            if not self.wellformed.wellformedness_date(value):
                                message = messages['m3']
                                table = {row_idx: {field: [0]}}
                                error = self.helper.create_error_dict(validation_level='csv_wellformedness',
                                                                      error_type='error',
                                                                      message=message,
                                                                      error_label='date_format',
                                                                      located_in='item',
                                                                      table=table)
                                jsonl_file.write(error)

            # GET SELF-CITATIONS AND DUPLICATE CITATIONS (LMDB-backed, no in-memory entity list needed)
            duplicate_report = self.wellformed.get_duplicates_cits(
                uf=uf, data_cache=duplicate_data_cache, messages=messages)
            for error in duplicate_report:
                jsonl_file.write(error)

        # Cleanup LMDB resources used for duplicate detection
        uf_env.close()
        shutil.rmtree(uf_tmp_dir, ignore_errors=True)
        if duplicate_data_cache is not None:
            duplicate_data_cache.close()

        # write human-readable validation summary to txt file
        textual_report_stream= self.helper.create_validation_summary_stream(self.output_fp_json)
        with open(self.output_fp_txt, "w", encoding='utf-8') as f:
            for l in textual_report_stream:
                f.write(l)

        is_valid = JSONLStreamIO(self.output_fp_json).is_empty()
        return is_valid


class ClosureValidator:

    def __init__(self, meta_csv_doc, meta_output_dir, cits_csv_doc, cits_output_dir, strict_sequenciality=False, meta_kwargs=None, cits_kwargs=None) -> None:
        self.meta_csv_doc = meta_csv_doc
        self.meta_output_dir = meta_output_dir
        self.cits_csv_doc = cits_csv_doc
        self.cits_output_dir = cits_output_dir
        self.strict_sequentiality = strict_sequenciality  # if True, runs the check on transitive closure if and only if the other checks passed without errors

        script_dir = dirname(abspath(__file__))  # Directory where the script is located
        self.messages = full_load(open(join(script_dir, 'messages.yaml'), 'r', encoding='utf-8'))

        # Define default kwargs for optional configuration of the two instances of Validator
        default_kwargs = {'use_meta_endpoint': False, 'verify_id_existence': True}

        # Merge user-provided kwargs with defaults
        meta_kwargs = {**default_kwargs, **(meta_kwargs or {})}
        cits_kwargs = {**default_kwargs, **(cits_kwargs or {})}

        # Create Validator instances with merged kwargs
        self.meta_validator = Validator(self.meta_csv_doc, self.meta_output_dir, **meta_kwargs)
        self.cits_validator = Validator(self.cits_csv_doc, self.cits_output_dir, **cits_kwargs)

        self.helper = Helper()

        # Check if each of the two Validator instances is passed the expected table type
        if self.meta_validator.table_to_process != 'meta_csv':
            raise TableNotMatchingInstance(self.meta_csv_doc, self.meta_validator.table_to_process, 'meta_csv')
        if self.cits_validator.table_to_process != 'cits_csv':
            raise TableNotMatchingInstance(self.cits_csv_doc, self.cits_validator.table_to_process, 'cits_csv')
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures caches are properly closed."""
        self.close()
        return False
    
    def close(self):
        """Close caches and clean up resources."""
        if hasattr(self, 'meta_validator'):
            self.meta_validator.close()
        if hasattr(self, 'cits_validator'):
            self.cits_validator.close()


    def check_closure(self) -> tuple[bool, bool]:
        """
        Check transitive closure between META-CSV and CITS-CSV using LMDB-backed
        structures throughout.  No large dicts or lists of sets are held in RAM.

        Four LMDB resources are used:
        - ``meta_uf`` / ``cits_uf``: Union-Find for grouping IDs into entities.
        - ``meta_positions_cache`` / ``cits_positions_cache``: mapping each ID to
          the list of position dicts where it appears in its respective table.

        The closure check avoids building large Python sets by querying the LMDB
        caches directly with O(1) ``__contains__`` lookups.
        """
        meta_is_valid_closure = True
        cits_is_valid_closure = True

        # --- Set up LMDB Union-Finds (entity grouping) ---
        meta_uf_dir = tempfile.mkdtemp(prefix='uf_closure_meta_')
        meta_uf_env = lmdb.open(meta_uf_dir, map_size=100 * 1024 * 1024, sync=False, metasync=False)
        meta_uf = LmdbUnionFind(meta_uf_env)

        cits_uf_dir = tempfile.mkdtemp(prefix='uf_closure_cits_')
        cits_uf_env = lmdb.open(cits_uf_dir, map_size=100 * 1024 * 1024, sync=False, metasync=False)
        cits_uf = LmdbUnionFind(cits_uf_env)

        # --- Set up LMDB position caches (id → list of position dicts) ---
        meta_positions_cache = LmdbCache('closure_meta_positions')
        meta_positions_cache.open()
        cits_positions_cache = LmdbCache('closure_cits_positions')
        cits_positions_cache.open()

        try:
            # --- Collect entities and positions from META ---
            for row_idx, row in enumerate(self.meta_validator.csv_stream.stream()):
                row_obj = read_metadata_row(row)
                if row_obj.id:
                    ids = row_obj.id
                    ids_unique = list(set(ids))
                    # Register / union all IDs in LMDB Union-Find
                    meta_uf.find(ids_unique[0])
                    for _i in range(1, len(ids_unique)):
                        meta_uf.union(ids_unique[0], ids_unique[_i])
                    # Record position for each unique ID (read-modify-write)
                    pos_entry = {row_idx: {'id': list(range(len(ids)))}}
                    for item in ids_unique:
                        existing = meta_positions_cache.get(item)
                        if existing is None:
                            meta_positions_cache[item] = [pos_entry]
                        else:
                            existing.append(pos_entry)
                            meta_positions_cache[item] = existing

            # --- Collect entities and positions from CITS-CSV ---
            for row_idx, row in enumerate(self.cits_validator.csv_stream.stream()):
                row_obj = read_citations_row(row)
                for id_field, field_name in (
                    (row_obj.citing_id, 'citing_id'),
                    (row_obj.cited_id, 'cited_id'),
                ):
                    if id_field:
                        ids = id_field
                        ids_unique = list(set(ids))
                        cits_uf.find(ids_unique[0])
                        for _i in range(1, len(ids_unique)):
                            cits_uf.union(ids_unique[0], ids_unique[_i])
                        pos_entry = {row_idx: {field_name: list(range(len(ids)))}}
                        for item in ids_unique:
                            existing = cits_positions_cache.get(item)
                            if existing is None:
                                cits_positions_cache[item] = [pos_entry]
                            else:
                                existing.append(pos_entry)
                                cits_positions_cache[item] = existing

            # --- Check META entities that have no citations ---
            # An entity is "missing citations" when ALL of its IDs are absent from cits_positions_cache.
            # We check membership directly in LMDB (O(1) per lookup) — no large Python sets needed.
            with JSONLStreamIO(self.meta_validator.output_fp_json, 'a') as meta_json_file:
                for _root, br_ids_set in meta_uf.iter_components():
                    if all(id_ not in cits_positions_cache for id_ in br_ids_set):
                        table: dict = {}
                        for id_ in br_ids_set:
                            for pos_dict in (meta_positions_cache.get(id_) or []):
                                table.update(pos_dict)
                        if table:
                            meta_json_file.write(
                                self.helper.create_error_dict(
                                    validation_level='csv_wellformedness',
                                    error_type='error',
                                    message=self.messages['m24'],
                                    error_label='missing_citations',
                                    located_in='row',
                                    table=table,
                                )
                            )
                            meta_is_valid_closure = False

            # --- Check CITS entities that have no metadata ---
            with JSONLStreamIO(self.cits_validator.output_fp_json, 'a') as cits_json_file:
                for _root, br_ids_set in cits_uf.iter_components():
                    if all(id_ not in meta_positions_cache for id_ in br_ids_set):
                        table = {}
                        for id_ in br_ids_set:
                            for pos_dict in (cits_positions_cache.get(id_) or []):
                                table.update(pos_dict)
                        if table:
                            cits_json_file.write(
                                self.helper.create_error_dict(
                                    validation_level='csv_wellformedness',
                                    error_type='error',
                                    message=self.messages['m25'],
                                    error_label='missing_metadata',
                                    located_in='row',
                                    table=table,
                                )
                            )
                            cits_is_valid_closure = False

        finally:
            # Cleanup all LMDB resources
            meta_uf_env.close()
            shutil.rmtree(meta_uf_dir, ignore_errors=True)
            cits_uf_env.close()
            shutil.rmtree(cits_uf_dir, ignore_errors=True)
            meta_positions_cache.close()
            cits_positions_cache.close()

        # Write human-readable validation summaries for both tables
        textual_report_stream_meta = self.helper.create_validation_summary_stream(self.meta_validator.output_fp_json)
        textual_report_stream_cits = self.helper.create_validation_summary_stream(self.cits_validator.output_fp_json)

        with open(self.meta_validator.output_fp_txt, "w", encoding='utf-8') as fm:
            for lm in textual_report_stream_meta:
                fm.write(lm)
        with open(self.cits_validator.output_fp_txt, "w", encoding='utf-8') as fc:
            for lc in textual_report_stream_cits:
                fc.write(lc)

        return (meta_is_valid_closure, cits_is_valid_closure)
        

    def validate(self):

        # TODO: add informative print messages to say which process is running and when it terminates
        
        # Run single validation for META-CSV and CITS-CSV
        meta_is_valid = self.meta_validator.validate()
        cits_is_valid = self.cits_validator.validate()

        # in case some errors have already been found and strict_sequentiality is True, don't run the check on closure
        if self.strict_sequentiality and (not meta_is_valid or not cits_is_valid):
            print('The separate validation of the metadata (META-CSV) and citations (CITS-CSV) tables already detected some error (in one or both documents).',
                  'Skipping the check of transitive closure as strict_sequentiality==True.')
            return (meta_is_valid, cits_is_valid) 
        
        # Run validation for transitive closure
        meta_is_valid_closure, cits_is_valid_closure = self.check_closure()

        return (bool(meta_is_valid_closure and meta_is_valid), bool(cits_is_valid_closure and cits_is_valid))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-i', '--input', dest='input_csv', required=True,
                        help='The path to the CSV document to validate.', type=str)
    parser.add_argument('-o', '--output', dest='output_dir', required=True,
                        help='The path to the directory where to store the output JSON-L file.', type=str)
    parser.add_argument('-m', '--use-meta', dest='use_meta_endpoint', action='store_true',
                        help='Use the OC Meta endpoint to check if an ID exists.', required=False)
    parser.add_argument('-s', '--no-id-existence', dest='verify_id_existence', action='store_false',
                        help='Skip checking if IDs are registered somewhere, i.e. do not use Meta endpoint nor external APIs.',
                        required=False)
    parser.add_argument('--memory-efficient', dest='memory_efficient', action='store_true', default=True,
                        help='Use LMDB for efficient memory usage with large files (default: True). '
                        'Set to False to use in-memory caching (faster but uses more RAM).',
                        required=False)
    parser.add_argument('--cache-path', dest='cache_path', type=str, default=None,
                        help='Optional custom path for LMDB database storage (default: ./storage/cache).',
                        required=False)
    args = parser.parse_args()
    v = Validator(
        args.input_csv, 
        args.output_dir, 
        args.use_meta_endpoint,
        args.verify_id_existence,
        args.memory_efficient,
        args.cache_path,
    )
    v.validate()

# to instantiate the class, write:
# v = Validator('path/to/csv/file', 'output/dir/path') # optionally set use_meta_endpoint to True and/or verify_id_existence to False
# v.validate() --> validates, returns the output, and saves files


# FROM THE COMMAND LINE:
# python -m oc_validator.main -i <input csv file path> -o <output dir path> [-m] [-s]