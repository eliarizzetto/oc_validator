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
from os import makedirs, listdir
from os.path import exists, join, dirname, abspath
from oc_validator.main import Validator, TableNotMatchingInstance, InvalidTableError, ClosureValidator, ValidationError
from oc_validator.helper import JSONLStreamIO
import shutil
import os
import json
import tempfile


def _cleanup_lmdb_orphans():
    """Remove leftover LMDB temp directories (uf_dup_*, lmdb_*) from the tests/ working dir."""
    cwd = os.getcwd()
    for entry in listdir(cwd):
        if entry.startswith(('uf_dup_', 'lmdb_')):
            shutil.rmtree(join(cwd, entry), ignore_errors=True)


class TestValidator(unittest.TestCase):

    def setUp(self):
        # set current working directory to "tests"
        os.chdir(os.path.dirname(os.path.abspath(__file__)))

        # Create a temporary directory for test outputs
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        # Clean up the temporary directory after tests
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()
        _cleanup_lmdb_orphans()

    def test_InvalidTableError(self):
        invalid_format_table = 'test_data/invalid_format_table.csv'
        with self.assertRaises(InvalidTableError):
            Validator(invalid_format_table, self.test_output_dir)

    def test_TableNotMatchingInstance(self):
        meta_csv = 'test_data/valid_meta_mock.csv'
        cits_csv = 'test_data/valid_cits_mock.csv'
        with self.assertRaises(TableNotMatchingInstance):
            ClosureValidator(
                meta_in=cits_csv,
                meta_out_dir='test_output',
                cits_in=meta_csv,
                cits_out_dir='test_output')

    def test_validate_meta(self):
        """
        Test the validation of a metadata table.
        """
        # valid table
        outdir1 = join(self.test_output_dir, 'valid_meta_single')
        valid_meta_csv = 'test_data/valid_meta_mock.csv'
        vldtr_valid = Validator(valid_meta_csv, outdir1)
        outcome = vldtr_valid.validate()
        self.assertIsInstance(outcome, bool)
        self.assertTrue(exists(vldtr_valid.output_fp_json))
        self.assertTrue(exists(vldtr_valid.output_fp_txt))
        self.assertTrue(outcome)

        # other valid table
        outdir2 = join(self.test_output_dir, 'valid_meta_single_2')
        valid_meta_csv_2 = 'test_data/other_valid_meta_mock.csv'
        vldtr_valid_2 = Validator(valid_meta_csv_2, outdir1, verify_id_existence=False)
        outcome2 = vldtr_valid_2.validate()
        self.assertIsInstance(outcome2, bool)
        self.assertTrue(exists(vldtr_valid_2.output_fp_json))
        self.assertTrue(exists(vldtr_valid_2.output_fp_txt))
        self.assertTrue(outcome2)

        # invalid table
        outdir3 = join(self.test_output_dir, 'invalid_meta_single')
        invalid_meta_csv = 'test_data/invalid_meta_mock.csv'
        vldtr_invalid = Validator(invalid_meta_csv, outdir3)
        outcome3 = vldtr_invalid.validate()
        self.assertIsInstance(outcome3, bool)
        self.assertTrue(exists(vldtr_invalid.output_fp_json))
        self.assertTrue(exists(vldtr_invalid.output_fp_txt))
        self.assertFalse(outcome3)
        with JSONLStreamIO(vldtr_invalid.output_fp_json, 'r') as reader:
            effective_detected_issues = sorted(reader, key=lambda x: json.dumps(x, sort_keys=True))
        with JSONLStreamIO('test_data/invalid_meta_mock_report.jsonl', 'r') as expected_reader:
            expected_issues = sorted(expected_reader, key=lambda x: json.dumps(x, sort_keys=True))
        self.assertEqual(effective_detected_issues, expected_issues)
        
        

        # With use_meta_endpoint=True option
        outdir4 = join(self.test_output_dir, 'valid_meta_use_meta_endpoint')
        vldtr_valid_meta_endpoint = Validator(valid_meta_csv, outdir4, use_meta_endpoint=True)
        outcome4 = vldtr_valid_meta_endpoint.validate()
        self.assertTrue(outcome4)


    def test_validate_cits(self):
        """
        Test the validation of a citations table.
        """

        # VALID TABLE
        outdir1 = join(self.test_output_dir, 'valid_cits_single')
        valid_cits_csv = 'test_data/valid_cits_mock.csv'
        vldtr_valid = Validator(valid_cits_csv, outdir1)
        outcome = vldtr_valid.validate()
        self.assertIsInstance(outcome, bool)
        self.assertTrue(exists(vldtr_valid.output_fp_json))
        self.assertTrue(exists(vldtr_valid.output_fp_txt))
        self.assertTrue(outcome)

        # INVALID TABLE
        outdir2 = join(self.test_output_dir, 'invalid_cits_single')
        invalid_cits_csv = 'test_data/invalid_cits_mock.csv'
        vldtr_invalid = Validator(invalid_cits_csv, outdir2)
        outcome = vldtr_invalid.validate()
        self.assertIsInstance(outcome, bool)
        self.assertTrue(exists(vldtr_invalid.output_fp_json))
        self.assertTrue(exists(vldtr_invalid.output_fp_txt))
        self.assertFalse(outcome)
        with JSONLStreamIO(vldtr_invalid.output_fp_json, 'r') as reader:
            effective_detected_issues = sorted(reader, key=lambda x: json.dumps(x, sort_keys=True))
        with JSONLStreamIO('test_data/invalid_cits_mock_report.jsonl', 'r') as expected_reader:
            expected_issues = sorted(expected_reader, key=lambda x: json.dumps(x, sort_keys=True))
        self.assertEqual(effective_detected_issues, expected_issues)

    def test_closure_validator(self):
        """
        Test the validation of a metadata table and a citations table together.
        """
        # BOTH TABLES ARE VALID and MUTUALLY CLOSED: the resources in 'test_data/valid_cits_mock.csv' are described in 'test_data/valid_meta_mock.csv'!
        outdir1 = join(self.test_output_dir, 'closed_valid')
        valid_meta_csv = 'test_data/valid_meta_mock.csv'
        valid_cits_csv = 'test_data/valid_cits_mock.csv'
        closure_validator1 = ClosureValidator(
            meta_in=valid_meta_csv,
            meta_out_dir=outdir1,
            cits_in=valid_cits_csv,
            cits_out_dir=outdir1
        )
        outcome1 = closure_validator1.validate()
        self.assertIsInstance(outcome1, tuple)
        self.assertTrue(exists(closure_validator1.meta_validator.output_fp_json))
        self.assertTrue(exists(closure_validator1.meta_validator.output_fp_txt))
        self.assertTrue(exists(closure_validator1.cits_validator.output_fp_json))
        self.assertTrue(exists(closure_validator1.cits_validator.output_fp_txt))
        self.assertEqual(outcome1, (True, True))

        # NON-CLOSED TABLES (where resources lack citations and citations lack metadata in both tables) and BOTH TABLES ARE VALID PER SE
        outdir2 = join(self.test_output_dir, 'non_closed_valid')
        other_valid_meta_csv = 'test_data/other_valid_meta_mock.csv'
        other_valid_cits_csv = 'test_data/other_valid_cits_mock.csv'
        closure_validator2 = ClosureValidator(
            meta_in=other_valid_meta_csv,
            meta_out_dir=outdir2,
            cits_in=other_valid_cits_csv,
            cits_out_dir=outdir2,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        outcome2 = closure_validator2.validate()
        self.assertIsInstance(outcome2, tuple)
        self.assertIsInstance(outcome2[0], bool)
        self.assertIsInstance(outcome2[1], bool)
        self.assertTrue(exists(closure_validator2.meta_validator.output_fp_json))
        self.assertTrue(exists(closure_validator2.cits_validator.output_fp_json))
        self.assertTrue(exists(closure_validator2.meta_validator.output_fp_txt))
        self.assertTrue(exists(closure_validator2.cits_validator.output_fp_txt))
        self.assertEqual(outcome2, (False, False))

        # NON-CLOSED TABLES (where resources lack citations and citations lack metadata in both tables) and ONE TABLE IS INVALID
        outdir3 = join(self.test_output_dir, 'non_closed_invalid_cits')
        invalid_cits = 'test_data/invalid_cits_mock.csv'
        closure_validator3 = ClosureValidator(
            meta_in=other_valid_meta_csv,
            meta_out_dir=outdir3,
            cits_in=invalid_cits,
            cits_out_dir=outdir3,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        outcome3 = closure_validator3.validate()
        self.assertIsInstance(outcome3, tuple)
        self.assertEqual(outcome3[0], False) # because even if metadata table is valid per se, the closure is not satisfied
        self.assertEqual(outcome3[1], False) # because citations table is invalid

        # NON-CLOSED TABLES (ONE INVALID) with strict_sequentiality=True option
        outdir4 = join(self.test_output_dir, 'non_closed_invalid_cits_strict')
        closure_validator4 = ClosureValidator(
            meta_in=other_valid_meta_csv,
            meta_out_dir=outdir4,
            cits_in=invalid_cits,
            cits_out_dir=outdir4,
            strict_sequentiality=True,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        outcome4 = closure_validator4.validate()
        self.assertIsInstance(outcome4, tuple)
        self.assertEqual(outcome4[0], True) # because metadata table is valid per se, and the closure is not checked because the other table is invalid
        self.assertEqual(outcome4[1], False) # because citations table is invalid
        self.assertTrue(exists(closure_validator4.meta_validator.output_fp_json))
        self.assertTrue(exists(closure_validator4.cits_validator.output_fp_json))


class TestErrorHierarchy(unittest.TestCase):
    """Test that custom exceptions have the correct inheritance."""

    def test_InvalidTableError_is_ValidationError(self):
        self.assertTrue(issubclass(InvalidTableError, ValidationError))

    def test_TableNotMatchingInstance_is_ValidationError(self):
        self.assertTrue(issubclass(TableNotMatchingInstance, ValidationError))

    def test_ValidationError_is_Exception(self):
        self.assertTrue(issubclass(ValidationError, Exception))

    def test_InvalidTableError_stores_input_fp(self):
        try:
            raise InvalidTableError('some/file.csv')
        except InvalidTableError as e:
            self.assertEqual(e.input_fp, 'some/file.csv')

    def test_TableNotMatchingInstance_stores_fields(self):
        try:
            raise TableNotMatchingInstance('file.csv', 'cits_csv', 'meta_csv')
        except TableNotMatchingInstance as e:
            self.assertEqual(e.input_fp, 'file.csv')
            self.assertEqual(e.detected_table_type, 'cits_csv')
            self.assertEqual(e.correct_table_type, 'meta_csv')


class TestProcessSelector(unittest.TestCase):
    """Test Validator.process_selector table type detection."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_detects_meta_csv(self):
        v = Validator('test_data/valid_meta_mock.csv', join(self.test_output_dir, 'ps_meta'))
        self.assertEqual(v.table_to_process, 'meta_csv')

    def test_detects_cits_csv_4col(self):
        v = Validator('test_data/valid_cits_mock.csv', join(self.test_output_dir, 'ps_cits4'))
        self.assertEqual(v.table_to_process, 'cits_csv')

    def test_detects_cits_csv_2col(self):
        v = Validator('test_data/valid_cits_2col_mock.csv', join(self.test_output_dir, 'ps_cits2'))
        self.assertEqual(v.table_to_process, 'cits_csv')

    def test_invalid_table_raises(self):
        with self.assertRaises(InvalidTableError):
            Validator('test_data/invalid_format_table.csv', join(self.test_output_dir, 'ps_inv'))


class TestValidatorOutputFilePaths(unittest.TestCase):
    """Test output file path generation and counter increment."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_meta_output_files_created(self):
        outdir = join(self.test_output_dir, 'outpaths_meta')
        v = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        self.assertTrue(v.output_fp_json.endswith('.jsonl'))
        self.assertTrue(v.output_fp_txt.endswith('.txt'))
        self.assertTrue('meta' in v.output_fp_json or 'meta' in v.output_fp_txt)
        self.assertTrue(exists(v.output_fp_json))
        self.assertTrue(exists(v.output_fp_txt))

    def test_cits_output_files_created(self):
        outdir = join(self.test_output_dir, 'outpaths_cits')
        v = Validator('test_data/valid_cits_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        self.assertTrue(v.output_fp_json.endswith('.jsonl'))
        self.assertTrue(v.output_fp_txt.endswith('.txt'))
        self.assertTrue('cits' in v.output_fp_json or 'cits' in v.output_fp_txt)
        self.assertTrue(exists(v.output_fp_json))
        self.assertTrue(exists(v.output_fp_txt))

    def test_output_filepath_counter_increment(self):
        """Running the same validator twice should produce files with incrementing counters."""
        outdir = join(self.test_output_dir, 'outpaths_counter')
        v1 = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=False)
        v1.validate()
        fp1 = v1.output_fp_json

        v2 = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=False)
        fp2 = v2.output_fp_json
        v2.validate()

        # The two file paths should differ because the first already exists
        self.assertNotEqual(fp1, fp2)

    def test_output_dir_created_if_missing(self):
        outdir = join(self.test_output_dir, 'new_subdir_for_output')
        self.assertFalse(exists(outdir))
        v = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=False)
        self.assertTrue(exists(outdir))


class TestValidatorWithLMDB(unittest.TestCase):
    """Test Validator with use_lmdb=True option."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_validate_meta_with_lmdb(self):
        outdir = join(self.test_output_dir, 'lmdb_meta')
        v = Validator('test_data/valid_meta_mock.csv', outdir, use_lmdb=True, verify_id_existence=False)
        self.assertTrue(v.memory_efficient)
        result = v.validate()
        self.assertTrue(result)
        self.assertTrue(exists(v.output_fp_json))
        self.assertTrue(exists(v.output_fp_txt))

    def test_validate_cits_with_lmdb(self):
        outdir = join(self.test_output_dir, 'lmdb_cits')
        v = Validator('test_data/valid_cits_mock.csv', outdir, use_lmdb=True, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)
        self.assertTrue(exists(v.output_fp_json))
        self.assertTrue(exists(v.output_fp_txt))

    def test_validate_invalid_meta_with_lmdb(self):
        outdir = join(self.test_output_dir, 'lmdb_meta_inv')
        v = Validator('test_data/invalid_meta_mock.csv', outdir, use_lmdb=True, verify_id_existence=False)
        result = v.validate()
        self.assertFalse(result)

    def test_validate_invalid_cits_with_lmdb(self):
        outdir = join(self.test_output_dir, 'lmdb_cits_inv')
        v = Validator('test_data/invalid_cits_mock.csv', outdir, use_lmdb=True, verify_id_existence=False)
        result = v.validate()
        self.assertFalse(result)

    def test_validate_meta_with_lmdb_and_cache_dir(self):
        cache_dir = join(self.test_output_dir, 'lmdb_cache_dir')
        os.makedirs(cache_dir, exist_ok=True)
        outdir = join(self.test_output_dir, 'lmdb_meta_cachedir')
        v = Validator('test_data/valid_meta_mock.csv', outdir, use_lmdb=True,
                      verify_id_existence=False, cache_dir=cache_dir)
        result = v.validate()
        self.assertTrue(result)


class TestValidatorWithVerboseAndLog(unittest.TestCase):
    """Test Validator with verbose and log_file options."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_verbose_meta(self):
        outdir = join(self.test_output_dir, 'verbose_meta')
        v = Validator('test_data/valid_meta_mock.csv', outdir, verbose=True, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)

    def test_log_file_meta(self):
        outdir = join(self.test_output_dir, 'logfile_meta')
        log_fp = join(outdir, 'test.log')
        os.makedirs(outdir, exist_ok=True)
        v = Validator('test_data/valid_meta_mock.csv', outdir, log_file=log_fp, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)
        self.assertTrue(exists(log_fp))
        # Close the log file handler so Windows can delete it in tearDown
        import logging
        logger = logging.getLogger('oc_validator')
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    def test_verbose_cits(self):
        outdir = join(self.test_output_dir, 'verbose_cits')
        v = Validator('test_data/valid_cits_mock.csv', outdir, verbose=True, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)


class TestValidatorContextManager(unittest.TestCase):
    """Test Validator context manager protocol."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_context_manager_returns_validator(self):
        with Validator('test_data/valid_meta_mock.csv',
                       join(self.test_output_dir, 'ctx'),
                       verify_id_existence=False) as v:
            self.assertIsInstance(v, Validator)
            self.assertEqual(v.table_to_process, 'meta_csv')

    def test_context_manager_validate(self):
        with Validator('test_data/valid_meta_mock.csv',
                       join(self.test_output_dir, 'ctx_val'),
                       verify_id_existence=False) as v:
            result = v.validate()
            self.assertTrue(result)


class TestValidatorClose(unittest.TestCase):
    """Test Validator.close() resource cleanup."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_close_after_validate(self):
        v = Validator('test_data/valid_meta_mock.csv',
                      join(self.test_output_dir, 'close'),
                      verify_id_existence=False)
        v.validate()
        v.close()
        # After close, caches should be cleaned up
        self.assertIsNone(v.duplicate_data_cache)

    def test_double_close_no_error(self):
        v = Validator('test_data/valid_meta_mock.csv',
                      join(self.test_output_dir, 'dbl_close'),
                      verify_id_existence=False)
        v.validate()
        v.close()
        v.close()  # second close should not raise


class TestValidatorCits2Column(unittest.TestCase):
    """Test Validator with 2-column CITS-CSV (no date columns)."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_valid_2col_cits(self):
        outdir = join(self.test_output_dir, 'cits_2col')
        v = Validator('test_data/valid_cits_2col_mock.csv', outdir, verify_id_existence=False)
        self.assertEqual(v.table_to_process, 'cits_csv')
        result = v.validate()
        self.assertTrue(result)
        self.assertTrue(exists(v.output_fp_json))
        self.assertTrue(exists(v.output_fp_txt))

    def test_2col_cits_output_files(self):
        outdir = join(self.test_output_dir, 'cits_2col_out')
        v = Validator('test_data/valid_cits_2col_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        self.assertTrue('cits' in v.output_fp_json)


class TestValidatorOutputContent(unittest.TestCase):
    """Test the content of output files produced by Validator."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_valid_meta_empty_jsonl(self):
        """A valid META-CSV should produce an empty JSONL output."""
        outdir = join(self.test_output_dir, 'empty_jsonl_meta')
        v = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        with JSONLStreamIO(v.output_fp_json, 'r') as reader:
            self.assertTrue(reader.is_empty())

    def test_valid_cits_empty_jsonl(self):
        """A valid CITS-CSV should produce an empty JSONL output."""
        outdir = join(self.test_output_dir, 'empty_jsonl_cits')
        v = Validator('test_data/valid_cits_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        with JSONLStreamIO(v.output_fp_json, 'r') as reader:
            self.assertTrue(reader.is_empty())

    def test_invalid_meta_nonempty_jsonl(self):
        """An invalid META-CSV should produce a non-empty JSONL output."""
        outdir = join(self.test_output_dir, 'nonempty_jsonl_meta')
        v = Validator('test_data/invalid_meta_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        with JSONLStreamIO(v.output_fp_json, 'r') as reader:
            self.assertFalse(reader.is_empty())

    def test_invalid_cits_nonempty_jsonl(self):
        """An invalid CITS-CSV should produce a non-empty JSONL output."""
        outdir = join(self.test_output_dir, 'nonempty_jsonl_cits')
        v = Validator('test_data/invalid_cits_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        with JSONLStreamIO(v.output_fp_json, 'r') as reader:
            self.assertFalse(reader.is_empty())

    def test_invalid_meta_error_labels(self):
        """Check specific error labels in the JSONL output for invalid META."""
        outdir = join(self.test_output_dir, 'labels_meta')
        v = Validator('test_data/invalid_meta_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        errors = []
        with JSONLStreamIO(v.output_fp_json, 'r') as reader:
            for error in reader:
                errors.append(error)
        labels = {e['error_label'] for e in errors}
        # The invalid_meta_mock.csv has: FAKE_ID_SCHEME:value (br_id_format),
        # duplicate publishers (duplicate_ra)
        self.assertTrue(len(errors) > 0)
        self.assertIn('br_id_format', labels)

    def test_invalid_cits_error_labels(self):
        """Check specific error labels in the JSONL output for invalid CITS."""
        outdir = join(self.test_output_dir, 'labels_cits')
        v = Validator('test_data/invalid_cits_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        errors = []
        with JSONLStreamIO(v.output_fp_json, 'r') as reader:
            for error in reader:
                errors.append(error)
        labels = {e['error_label'] for e in errors}
        # The invalid_cits_mock.csv has: empty citing_id (required_value_cits)
        self.assertTrue(len(errors) > 0)
        self.assertIn('required_value_cits', labels)

    def test_valid_meta_txt_summary(self):
        """A valid META-CSV should produce an empty or minimal txt summary."""
        outdir = join(self.test_output_dir, 'txt_meta')
        v = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        with open(v.output_fp_txt, 'r', encoding='utf-8') as f:
            content = f.read()
        # Valid tables produce empty summaries
        self.assertEqual(content, '')

    def test_invalid_meta_txt_summary_not_empty(self):
        """An invalid META-CSV should produce a non-empty txt summary."""
        outdir = join(self.test_output_dir, 'txt_meta_inv')
        v = Validator('test_data/invalid_meta_mock.csv', outdir, verify_id_existence=False)
        v.validate()
        with open(v.output_fp_txt, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertTrue(len(content) > 0)


class TestValidatorRealDataClosed(unittest.TestCase):
    """Test the Validator with the real closed data from data/valid/closed/."""

    def setUp(self):
        os.chdir(dirname(abspath(__file__)))
        # Go up one level to reach project root
        self.project_root = abspath(join(dirname(abspath(__file__)), '..'))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_real_closed_meta(self):
        meta_csv = join(self.project_root, 'data', 'valid', 'closed', 'meta.csv')
        outdir = join(self.test_output_dir, 'real_meta')
        v = Validator(meta_csv, outdir, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)

    def test_real_closed_cits(self):
        cits_csv = join(self.project_root, 'data', 'valid', 'closed', 'cits.csv')
        outdir = join(self.test_output_dir, 'real_cits')
        v = Validator(cits_csv, outdir, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)

    def test_real_closed_closure(self):
        """Test ClosureValidator with the real closed data."""
        meta_csv = join(self.project_root, 'data', 'valid', 'closed', 'meta.csv')
        cits_csv = join(self.project_root, 'data', 'valid', 'closed', 'cits.csv')
        outdir = join(self.test_output_dir, 'real_closed')
        cv = ClosureValidator(
            meta_in=meta_csv,
            meta_out_dir=outdir,
            cits_in=cits_csv,
            cits_out_dir=outdir,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        meta_valid, cits_valid = cv.validate()
        self.assertTrue(meta_valid)
        self.assertTrue(cits_valid)


class TestClosureValidatorConfigurations(unittest.TestCase):
    """Test ClosureValidator with various configurations."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_closure_with_lmdb(self):
        """Test ClosureValidator with use_lmdb=True."""
        outdir = join(self.test_output_dir, 'closure_lmdb')
        cv = ClosureValidator(
            meta_in='test_data/valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/valid_cits_mock.csv',
            cits_out_dir=outdir,
            use_lmdb=True,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        result = cv.validate()
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertTrue(exists(cv.meta_validator.output_fp_json))
        self.assertTrue(exists(cv.cits_validator.output_fp_json))

    def test_closure_verbose(self):
        """Test ClosureValidator with verbose=True."""
        outdir = join(self.test_output_dir, 'closure_verbose')
        cv = ClosureValidator(
            meta_in='test_data/valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/valid_cits_mock.csv',
            cits_out_dir=outdir,
            verbose=True,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        result = cv.validate()
        self.assertIsInstance(result, tuple)

    def test_closure_context_manager(self):
        """Test ClosureValidator as context manager."""
        outdir = join(self.test_output_dir, 'closure_ctx')
        with ClosureValidator(
            meta_in='test_data/valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/valid_cits_mock.csv',
            cits_out_dir=outdir,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        ) as cv:
            result = cv.validate()
            self.assertIsInstance(result, tuple)

    def test_closure_strict_sequentiality_valid(self):
        """When both tables are valid per se and strict_sequentiality=True, closure should still be checked."""
        outdir = join(self.test_output_dir, 'closure_strict_valid')
        cv = ClosureValidator(
            meta_in='test_data/valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/valid_cits_mock.csv',
            cits_out_dir=outdir,
            strict_sequentiality=True,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        result = cv.validate()
        # Both are valid per se and closed, so (True, True)
        self.assertEqual(result, (True, True))

    def test_closure_strict_sequentiality_invalid(self):
        """When one table is invalid and strict_sequentiality=True, closure is skipped."""
        outdir = join(self.test_output_dir, 'closure_strict_inv')
        cv = ClosureValidator(
            meta_in='test_data/other_valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/invalid_cits_mock.csv',
            cits_out_dir=outdir,
            strict_sequentiality=True,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        result = cv.validate()
        # meta valid per se (True), cits invalid (False), closure skipped
        self.assertEqual(result[0], True)
        self.assertEqual(result[1], False)

    def test_closure_non_strict_both_valid_not_closed(self):
        """Non-strict: both tables valid per se but not closed -> closure errors."""
        outdir = join(self.test_output_dir, 'closure_non_strict')
        cv = ClosureValidator(
            meta_in='test_data/other_valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/other_valid_cits_mock.csv',
            cits_out_dir=outdir,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        result = cv.validate()
        self.assertEqual(result, (False, False))

    def test_closure_with_lmdb_and_cache_dir(self):
        """Test ClosureValidator with LMDB and custom cache dir."""
        cache_dir = join(self.test_output_dir, 'closure_cache')
        os.makedirs(cache_dir, exist_ok=True)
        outdir = join(self.test_output_dir, 'closure_lmdb_cachedir')
        cv = ClosureValidator(
            meta_in='test_data/valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/valid_cits_mock.csv',
            cits_out_dir=outdir,
            use_lmdb=True,
            cache_dir=cache_dir,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        result = cv.validate()
        self.assertIsInstance(result, tuple)

    def test_closure_close_cleanup(self):
        """Test ClosureValidator.close() cleans up resources."""
        outdir = join(self.test_output_dir, 'closure_cleanup')
        cv = ClosureValidator(
            meta_in='test_data/valid_meta_mock.csv',
            meta_out_dir=outdir,
            cits_in='test_data/valid_cits_mock.csv',
            cits_out_dir=outdir,
            meta_kwargs={'verify_id_existence': False},
            cits_kwargs={'verify_id_existence': False}
        )
        cv.validate()
        cv.close()
        self.assertIsNone(cv._meta_positions_cache)
        self.assertIsNone(cv._cits_positions_cache)


class TestValidatorWithExistenceChecks(unittest.TestCase):
    """Test Validator with verify_id_existence=True (using external services)."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_meta_with_existence_checks(self):
        """Test that verify_id_existence=True runs without error (IDs may or may not exist)."""
        outdir = join(self.test_output_dir, 'existence_meta')
        v = Validator('test_data/valid_meta_mock.csv', outdir, verify_id_existence=True)
        result = v.validate()
        self.assertIsInstance(result, bool)
        self.assertTrue(exists(v.output_fp_json))

    def test_cits_with_existence_checks(self):
        """Test that verify_id_existence=True runs without error for CITS."""
        outdir = join(self.test_output_dir, 'existence_cits')
        v = Validator('test_data/valid_cits_mock.csv', outdir, verify_id_existence=True)
        result = v.validate()
        self.assertIsInstance(result, bool)
        self.assertTrue(exists(v.output_fp_json))

    def test_meta_with_meta_endpoint(self):
        """Test with use_meta_endpoint=True and verify_id_existence=True."""
        outdir = join(self.test_output_dir, 'meta_endpoint')
        v = Validator('test_data/valid_meta_mock.csv', outdir,
                      use_meta_endpoint=True, verify_id_existence=True)
        result = v.validate()
        self.assertIsInstance(result, bool)


class TestValidatorAllOptions(unittest.TestCase):
    """Test Validator with all option combinations."""

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.test_output_dir = 'test_output'
        if not exists(self.test_output_dir):
            makedirs(self.test_output_dir)

    def tearDown(self):
        if exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        if exists('./storage'):
            shutil.rmtree('./storage')
        _cleanup_lmdb_orphans()

    def test_lmdb_with_map_size(self):
        """Test with custom map_size for LMDB."""
        outdir = join(self.test_output_dir, 'mapsize')
        v = Validator('test_data/valid_meta_mock.csv', outdir,
                      use_lmdb=True, map_size=10 * 1024**2, verify_id_existence=False)
        result = v.validate()
        self.assertTrue(result)

    def test_lmdb_invalid_cits_with_map_size(self):
        outdir = join(self.test_output_dir, 'mapsize_inv')
        v = Validator('test_data/invalid_cits_mock.csv', outdir,
                      use_lmdb=True, map_size=10 * 1024**2, verify_id_existence=False)
        result = v.validate()
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
