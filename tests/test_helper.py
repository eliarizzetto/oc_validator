import unittest
import os
import json
import tempfile
import shutil
from oc_validator.helper import UnionFind, Helper, CSVStreamReader, JSONLStreamIO, read_csv


class TestUnionFind(unittest.TestCase):
    """Tests for the UnionFind data structure."""

    def setUp(self):
        self.uf = UnionFind()

    def test_find_new_element_is_own_root(self):
        self.assertEqual(self.uf.find('a'), 'a')

    def test_union_merges_components(self):
        self.uf.union('a', 'b')
        self.assertEqual(self.uf.find('a'), self.uf.find('b'))

    def test_union_transitive(self):
        self.uf.union('a', 'b')
        self.uf.union('b', 'c')
        self.assertEqual(self.uf.find('a'), self.uf.find('c'))

    def test_single_element_is_own_component(self):
        self.uf.find('x')
        self.assertEqual(self.uf.find('x'), 'x')

    def test_union_same_element(self):
        self.uf.find('a')
        self.uf.union('a', 'a')
        self.assertEqual(self.uf.find('a'), 'a')

    def test_path_compression(self):
        # Create a chain: a -> b -> c -> d
        self.uf.union('a', 'b')
        self.uf.union('b', 'c')
        self.uf.union('c', 'd')
        # After find('a'), path should be compressed
        root = self.uf.find('a')
        # All should share the same root
        self.assertEqual(self.uf.find('b'), root)
        self.assertEqual(self.uf.find('c'), root)
        self.assertEqual(self.uf.find('d'), root)

    def test_multiple_disjoint_sets(self):
        self.uf.union('a', 'b')
        self.uf.union('c', 'd')
        # a and b are in one set, c and d in another
        self.assertEqual(self.uf.find('a'), self.uf.find('b'))
        self.assertEqual(self.uf.find('c'), self.uf.find('d'))
        self.assertNotEqual(self.uf.find('a'), self.uf.find('c'))


class TestHelperGroupIds(unittest.TestCase):
    """Tests for Helper.group_ids."""

    def setUp(self):
        self.helper = Helper()

    def test_single_group_single_id(self):
        result = self.helper.group_ids([{'doi:10.1234/a'}])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {'doi:10.1234/a'})

    def test_single_group_multiple_ids(self):
        """IDs in the same row belong to the same entity."""
        result = self.helper.group_ids([{'doi:10.1234/a', 'pmid:12345'}])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {'doi:10.1234/a', 'pmid:12345'})

    def test_co_occurring_ids_merged(self):
        """IDs appearing together in one row are grouped."""
        result = self.helper.group_ids([{'doi:10.1234/a', 'pmid:12345'}, {'doi:10.1234/a', 'isbn:9783161484100'}])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {'doi:10.1234/a', 'pmid:12345', 'isbn:9783161484100'})

    def test_disjoint_groups(self):
        result = self.helper.group_ids([{'doi:10.1234/a'}, {'doi:10.5678/b'}])
        self.assertEqual(len(result), 2)

    def test_empty_input(self):
        result = self.helper.group_ids([])
        self.assertEqual(result, [])

    def test_transitive_grouping(self):
        """doi:a appears with pmid:1 in one row, pmid:1 appears with isbn:2 in another."""
        result = self.helper.group_ids([
            {'doi:10.1234/a', 'pmid:1'},
            {'pmid:1', 'isbn:9783161484100'}
        ])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {'doi:10.1234/a', 'pmid:1', 'isbn:9783161484100'})


class TestHelperCreateErrorDict(unittest.TestCase):
    """Tests for Helper.create_error_dict."""

    def setUp(self):
        self.helper = Helper()

    def test_basic_error(self):
        table = {0: {'id': [0]}}
        result = self.helper.create_error_dict(
            validation_level='csv_wellformedness',
            error_type='error',
            message='Test message',
            error_label='test_label',
            located_in='item',
            table=table
        )
        self.assertEqual(result['validation_level'], 'csv_wellformedness')
        self.assertEqual(result['error_type'], 'error')
        self.assertEqual(result['error_label'], 'test_label')
        self.assertFalse(result['valid'])
        self.assertEqual(result['message'], 'Test message')
        self.assertEqual(result['position']['located_in'], 'item')
        self.assertEqual(result['position']['table'], table)

    def test_warning_with_valid_true(self):
        result = self.helper.create_error_dict(
            validation_level='csv_wellformedness',
            error_type='warning',
            message='Warning',
            error_label='test',
            located_in='field',
            table={},
            valid=True
        )
        self.assertEqual(result['error_type'], 'warning')
        self.assertTrue(result['valid'])

    def test_error_default_valid_false(self):
        result = self.helper.create_error_dict(
            validation_level='existence',
            error_type='error',
            message='msg',
            error_label='lbl',
            located_in='row',
            table={}
        )
        self.assertFalse(result['valid'])

    def test_all_validation_levels(self):
        for level in ['csv_wellformedness', 'external_syntax', 'existence', 'semantic']:
            result = self.helper.create_error_dict(
                validation_level=level,
                error_type='error',
                message='msg',
                error_label='lbl',
                located_in='item',
                table={}
            )
            self.assertEqual(result['validation_level'], level)


class TestCSVStreamReader(unittest.TestCase):
    """Tests for CSVStreamReader."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_csv(self, filename, content, delimiter=','):
        filepath = os.path.join(self.tmpdir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath

    def test_comma_delimited(self):
        fp = self._write_csv('test.csv', 'a,b,c\n1,2,3\n')
        reader = CSVStreamReader(fp)
        rows = list(reader.stream())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['a'], '1')

    def test_semicolon_delimited(self):
        fp = self._write_csv('test.csv', 'a;b;c\n1;2;3\n')
        reader = CSVStreamReader(fp)
        rows = list(reader.stream())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['a'], '1')

    def test_tab_delimited(self):
        fp = self._write_csv('test.csv', 'a\tb\tc\n1\t2\t3\n')
        reader = CSVStreamReader(fp)
        rows = list(reader.stream())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['a'], '1')

    def test_multiple_rows(self):
        fp = self._write_csv('test.csv', 'id,name\n1,Alice\n2,Bob\n')
        reader = CSVStreamReader(fp)
        rows = list(reader.stream())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['id'], '1')
        self.assertEqual(rows[1]['name'], 'Bob')

    def test_invalid_delimiter_raises(self):
        """A file with a single column should fail delimiter detection."""
        fp = self._write_csv('test.csv', 'singlecolumn\n')
        with self.assertRaises(ValueError):
            CSVStreamReader(fp)

    def test_detect_delimiter_and_fieldnames(self):
        fp = self._write_csv('test.csv', 'x,y\n10,20\n')
        reader = CSVStreamReader(fp)
        self.assertEqual(reader._delimiter, ',')
        self.assertEqual(reader._fieldnames, ['x', 'y'])

    def test_iter_protocol(self):
        fp = self._write_csv('test.csv', 'a,b\n1,2\n')
        reader = CSVStreamReader(fp)
        rows = list(reader)
        self.assertEqual(len(rows), 1)

    def test_stream_can_be_reconsumed(self):
        """stream() should reopen the file, allowing multiple passes."""
        fp = self._write_csv('test.csv', 'a,b\n1,2\n')
        reader = CSVStreamReader(fp)
        rows1 = list(reader.stream())
        rows2 = list(reader.stream())
        self.assertEqual(rows1, rows2)

    def test_meta_csv_format(self):
        """Test reading a META-CSV formatted file."""
        fp = self._write_csv('meta.csv',
            '"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"\n'
            '"doi:10.1234/abc","Test Title","Smith, John","2020","Nature","1","2","1-10","journal article","Pub Co",""\n'
        )
        reader = CSVStreamReader(fp)
        rows = list(reader.stream())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['id'], 'doi:10.1234/abc')
        self.assertEqual(rows[0]['type'], 'journal article')


class TestJSONLStreamIO(unittest.TestCase):
    """Tests for JSONLStreamIO."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.jsonl_path = os.path.join(self.tmpdir, 'test.jsonl')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_write_and_read(self):
        data = {'key': 'value', 'num': 42}
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write(data)

        with JSONLStreamIO(self.jsonl_path, 'r') as reader:
            results = list(reader)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['key'], 'value')
        self.assertEqual(results[0]['num'], 42)

    def test_write_multiple_and_read(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({'a': 1})
            writer.write({'b': 2})
            writer.write({'c': 3})

        with JSONLStreamIO(self.jsonl_path, 'r') as reader:
            results = list(reader)
        self.assertEqual(len(results), 3)

    def test_is_empty_with_no_data(self):
        # Create empty file
        with open(self.jsonl_path, 'w') as f:
            pass
        with JSONLStreamIO(self.jsonl_path, 'r') as reader:
            self.assertTrue(reader.is_empty())

    def test_is_empty_with_data(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({'key': 'value'})
        with JSONLStreamIO(self.jsonl_path, 'r') as reader:
            self.assertFalse(reader.is_empty())

    def test_append_mode(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({'a': 1})
        with JSONLStreamIO(self.jsonl_path, 'a') as writer:
            writer.write({'b': 2})
        with JSONLStreamIO(self.jsonl_path, 'r') as reader:
            results = list(reader)
        self.assertEqual(len(results), 2)

    def test_write_without_context_raises(self):
        writer = JSONLStreamIO(self.jsonl_path, 'w')
        with self.assertRaises(ValueError):
            writer.write({'key': 'value'})

    def test_iter_protocol(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({'a': 1})
            writer.write({'b': 2})
        with JSONLStreamIO(self.jsonl_path, 'r') as reader:
            results = list(reader)
        self.assertEqual(len(results), 2)


class TestReadCsv(unittest.TestCase):
    """Tests for the legacy read_csv function."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_csv(self, content):
        filepath = os.path.join(self.tmpdir, 'test.csv')
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath

    def test_read_comma_csv(self):
        fp = self._write_csv('a,b\n1,2\n3,4\n')
        rows = read_csv(fp)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['a'], '1')

    def test_read_semicolon_csv(self):
        fp = self._write_csv('a;b\n1;2\n')
        rows = read_csv(fp)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['a'], '1')

    def test_invalid_csv_raises(self):
        fp = self._write_csv('singlecolumn\n')
        with self.assertRaises(ValueError):
            read_csv(fp)


class TestHelperCreateValidationSummaryStream(unittest.TestCase):
    """Tests for Helper.create_validation_summary_stream."""

    def setUp(self):
        self.helper = Helper()
        self.tmpdir = tempfile.mkdtemp()
        self.jsonl_path = os.path.join(self.tmpdir, 'summary.jsonl')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_summary_with_one_error(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({
                'error_label': 'test_error',
                'error_type': 'error',
                'message': 'Something went wrong.',
                'position': {
                    'table': {0: {'id': [0]}}
                }
            })

        lines = list(self.helper.create_validation_summary_stream(self.jsonl_path))
        text = ''.join(lines)
        self.assertIn('1 test_error issue', text)
        self.assertIn('Something went wrong.', text)
        self.assertIn('row 0', text)

    def test_summary_with_multiple_same_label(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            for i in range(3):
                writer.write({
                    'error_label': 'dup',
                    'error_type': 'error',
                    'message': 'Duplicate found.',
                    'position': {
                        'table': {i: {'id': [0]}}
                    }
                })

        lines = list(self.helper.create_validation_summary_stream(self.jsonl_path))
        text = ''.join(lines)
        self.assertIn('3 dup issues', text)

    def test_summary_with_different_labels(self):
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({
                'error_label': 'error_a',
                'error_type': 'error',
                'message': 'Error A.',
                'position': {'table': {0: {'id': [0]}}}
            })
            writer.write({
                'error_label': 'error_b',
                'error_type': 'warning',
                'message': 'Warning B.',
                'position': {'table': {1: {'title': [0]}}}
            })

        lines = list(self.helper.create_validation_summary_stream(self.jsonl_path))
        text = ''.join(lines)
        self.assertIn('error_a', text)
        self.assertIn('error_b', text)

    def test_summary_single_vs_plural(self):
        """Singular for count=1, plural for count>1."""
        # Single
        with JSONLStreamIO(self.jsonl_path, 'w') as writer:
            writer.write({
                'error_label': 'single',
                'error_type': 'error',
                'message': 'msg',
                'position': {'table': {0: {'id': [0]}}}
            })
        lines = list(self.helper.create_validation_summary_stream(self.jsonl_path))
        text = ''.join(lines)
        self.assertIn('is 1 single issue', text)

        # Plural
        jsonl_path2 = os.path.join(self.tmpdir, 'summary2.jsonl')
        with JSONLStreamIO(jsonl_path2, 'w') as writer:
            writer.write({
                'error_label': 'multi',
                'error_type': 'error',
                'message': 'msg',
                'position': {'table': {0: {'id': [0]}}}
            })
            writer.write({
                'error_label': 'multi',
                'error_type': 'error',
                'message': 'msg',
                'position': {'table': {1: {'id': [0]}}}
            })
        lines = list(self.helper.create_validation_summary_stream(jsonl_path2))
        text = ''.join(lines)
        self.assertIn('are 2 multi issues', text)


if __name__ == '__main__':
    unittest.main()
