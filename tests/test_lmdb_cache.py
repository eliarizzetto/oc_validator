import unittest
import tempfile
import shutil
import os
import lmdb
from oc_validator.lmdb_cache import LmdbCache, InMemoryCache, LmdbUnionFind, InMemoryUnionFind


class TestInMemoryCache(unittest.TestCase):
    """Tests for the InMemoryCache class."""

    def test_put_and_get(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['key1'] = 'value1'
        self.assertEqual(cache.get('key1'), 'value1')
        cache.close()

    def test_get_nonexistent_returns_default(self):
        cache = InMemoryCache('test')
        cache.open()
        self.assertIsNone(cache.get('missing'))
        self.assertEqual(cache.get('missing', 'default'), 'default')
        cache.close()

    def test_contains(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['key1'] = 'value1'
        self.assertIn('key1', cache)
        self.assertNotIn('key2', cache)
        cache.close()

    def test_delete(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['key1'] = 'value1'
        del cache['key1']
        self.assertNotIn('key1', cache)
        cache.close()

    def test_delete_nonexistent_no_error(self):
        cache = InMemoryCache('test')
        cache.open()
        cache.delete('nonexistent')  # should not raise
        cache.close()

    def test_getitem_raises_keyerror(self):
        cache = InMemoryCache('test')
        cache.open()
        with self.assertRaises(KeyError):
            _ = cache['nonexistent']
        cache.close()

    def test_keys(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['a'] = 1
        cache['b'] = 2
        self.assertEqual(set(cache.keys()), {'a', 'b'})
        cache.close()

    def test_values(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['a'] = 1
        cache['b'] = 2
        self.assertEqual(set(cache.values()), {1, 2})
        cache.close()

    def test_items(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['a'] = 1
        cache['b'] = 2
        items = dict(cache.items())
        self.assertEqual(items, {'a': 1, 'b': 2})
        cache.close()

    def test_len(self):
        cache = InMemoryCache('test')
        cache.open()
        self.assertEqual(len(cache), 0)
        cache['a'] = 1
        self.assertEqual(len(cache), 1)
        cache['b'] = 2
        self.assertEqual(len(cache), 2)
        cache.close()

    def test_clear(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['a'] = 1
        cache['b'] = 2
        cache.clear()
        self.assertEqual(len(cache), 0)
        cache.close()

    def test_bool_empty(self):
        cache = InMemoryCache('test')
        cache.open()
        self.assertFalse(cache)
        cache.close()

    def test_bool_nonempty(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['a'] = 1
        self.assertTrue(cache)
        cache.close()

    def test_context_manager(self):
        with InMemoryCache('test') as cache:
            cache['key'] = 'value'
            self.assertEqual(cache.get('key'), 'value')

    def test_store_complex_objects(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['dict'] = {'nested': [1, 2, 3]}
        self.assertEqual(cache.get('dict'), {'nested': [1, 2, 3]})
        cache.close()

    def test_overwrite(self):
        cache = InMemoryCache('test')
        cache.open()
        cache['key'] = 'old'
        cache['key'] = 'new'
        self.assertEqual(cache.get('key'), 'new')
        cache.close()


class TestLmdbCache(unittest.TestCase):
    """Tests for the LmdbCache class."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_put_and_get(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['key1'] = 'value1'
            self.assertEqual(cache.get('key1'), 'value1')

    def test_get_nonexistent_returns_default(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            self.assertIsNone(cache.get('missing'))
            self.assertEqual(cache.get('missing', 'default'), 'default')

    def test_contains(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['key1'] = 'value1'
            self.assertIn('key1', cache)
            self.assertNotIn('key2', cache)

    def test_contains_empty_string(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            self.assertFalse('' in cache)

    def test_delete(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['key1'] = 'value1'
            del cache['key1']
            self.assertNotIn('key1', cache)

    def test_getitem_raises_keyerror(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            with self.assertRaises(KeyError):
                _ = cache['nonexistent']

    def test_keys(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['a'] = 1
            cache['b'] = 2
            self.assertEqual(set(cache.keys()), {'a', 'b'})

    def test_values(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['a'] = 1
            cache['b'] = 2
            self.assertEqual(set(cache.values()), {1, 2})

    def test_items(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['a'] = 1
            cache['b'] = 2
            items = dict(cache.items())
            self.assertEqual(items, {'a': 1, 'b': 2})

    def test_len(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            self.assertEqual(len(cache), 0)
            cache['a'] = 1
            self.assertEqual(len(cache), 1)

    def test_clear(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['a'] = 1
            cache['b'] = 2
            cache.clear()
            self.assertEqual(len(cache), 0)

    def test_bool_empty(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            self.assertFalse(cache)

    def test_bool_nonempty(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['a'] = 1
            self.assertTrue(cache)

    def test_store_complex_objects(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['dict'] = {'nested': [1, 2, 3]}
            self.assertEqual(cache.get('dict'), {'nested': [1, 2, 3]})

    def test_overwrite(self):
        with LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2) as cache:
            cache['key'] = 'old'
            cache['key'] = 'new'
            self.assertEqual(cache.get('key'), 'new')

    def test_operations_on_closed_cache_raise(self):
        cache = LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2)
        cache.open()
        cache.close()
        with self.assertRaises(RuntimeError):
            cache.get('key')
        with self.assertRaises(RuntimeError):
            cache['key'] = 'value'
        with self.assertRaises(RuntimeError):
            len(cache)
        with self.assertRaises(RuntimeError):
            'key' in cache

    def test_cleanup_on_close(self):
        cache = LmdbCache('test', base_dir=self.tmpdir, map_size=10 * 1024**2)
        cache.open()
        temp_dir = cache._temp_dir
        self.assertTrue(os.path.isdir(temp_dir))
        cache.close()
        self.assertFalse(os.path.isdir(temp_dir))


class TestInMemoryUnionFind(unittest.TestCase):
    """Tests for the InMemoryUnionFind class."""

    def test_find_new_element(self):
        uf = InMemoryUnionFind()
        self.assertEqual(uf.find('a'), 'a')

    def test_union_merges(self):
        uf = InMemoryUnionFind()
        uf.union('a', 'b')
        self.assertEqual(uf.find('a'), uf.find('b'))

    def test_union_transitive(self):
        uf = InMemoryUnionFind()
        uf.union('a', 'b')
        uf.union('b', 'c')
        self.assertEqual(uf.find('a'), uf.find('c'))

    def test_contains(self):
        uf = InMemoryUnionFind()
        uf.find('a')
        self.assertIn('a', uf)
        self.assertNotIn('b', uf)

    def test_contains_empty_string(self):
        uf = InMemoryUnionFind()
        self.assertNotIn('', uf)

    def test_find_empty_raises(self):
        uf = InMemoryUnionFind()
        with self.assertRaises(ValueError):
            uf.find('')

    def test_path_compression(self):
        uf = InMemoryUnionFind()
        # Build a chain a -> b -> c -> d
        uf.union('a', 'b')
        uf.union('b', 'c')
        uf.union('c', 'd')
        root = uf.find('a')
        # After path compression, all should point to root
        self.assertEqual(uf._data['a'], root)

    def test_iter_components_single(self):
        uf = InMemoryUnionFind()
        uf.find('a')
        uf.find('b')
        uf.union('a', 'b')
        components = dict(uf.iter_components())
        self.assertEqual(len(components), 1)
        root = uf.find('a')
        self.assertEqual(components[root], {'a', 'b'})

    def test_iter_components_multiple(self):
        uf = InMemoryUnionFind()
        uf.union('a', 'b')
        uf.union('c', 'd')
        components = dict(uf.iter_components())
        self.assertEqual(len(components), 2)

    def test_iter_components_empty(self):
        uf = InMemoryUnionFind()
        components = dict(uf.iter_components())
        self.assertEqual(len(components), 0)

    def test_union_same_element(self):
        uf = InMemoryUnionFind()
        uf.find('a')
        uf.union('a', 'a')
        self.assertEqual(uf.find('a'), 'a')


class TestLmdbUnionFind(unittest.TestCase):
    """Tests for the LmdbUnionFind class."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.env = lmdb.open(self.tmpdir, map_size=10 * 1024**2, sync=False, metasync=False)

    def tearDown(self):
        self.env.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_find_new_element(self):
        uf = LmdbUnionFind(self.env)
        self.assertEqual(uf.find('a'), 'a')

    def test_union_merges(self):
        uf = LmdbUnionFind(self.env)
        uf.union('a', 'b')
        self.assertEqual(uf.find('a'), uf.find('b'))

    def test_union_transitive(self):
        uf = LmdbUnionFind(self.env)
        uf.union('a', 'b')
        uf.union('b', 'c')
        self.assertEqual(uf.find('a'), uf.find('c'))

    def test_contains(self):
        uf = LmdbUnionFind(self.env)
        uf.find('a')
        self.assertIn('a', uf)
        self.assertNotIn('b', uf)

    def test_contains_empty_string(self):
        uf = LmdbUnionFind(self.env)
        self.assertNotIn('', uf)

    def test_find_empty_raises(self):
        uf = LmdbUnionFind(self.env)
        with self.assertRaises(ValueError):
            uf.find('')

    def test_path_compression(self):
        uf = LmdbUnionFind(self.env)
        uf.union('a', 'b')
        uf.union('b', 'c')
        uf.union('c', 'd')
        root = uf.find('a')
        self.assertEqual(uf.find('b'), root)
        self.assertEqual(uf.find('c'), root)
        self.assertEqual(uf.find('d'), root)

    def test_iter_components_single(self):
        uf = LmdbUnionFind(self.env)
        uf.union('a', 'b')
        components = dict(uf.iter_components())
        self.assertEqual(len(components), 1)

    def test_iter_components_multiple(self):
        uf = LmdbUnionFind(self.env)
        uf.union('a', 'b')
        uf.union('c', 'd')
        components = dict(uf.iter_components())
        self.assertEqual(len(components), 2)

    def test_iter_components_empty(self):
        uf = LmdbUnionFind(self.env)
        components = dict(uf.iter_components())
        self.assertEqual(len(components), 0)

    def test_union_same_element(self):
        uf = LmdbUnionFind(self.env)
        uf.find('a')
        uf.union('a', 'a')
        self.assertEqual(uf.find('a'), 'a')

    def test_large_component(self):
        """Test creating a larger component."""
        uf = LmdbUnionFind(self.env)
        for i in range(20):
            uf.union(f'item_{i}', f'item_{i+1}')
        # All items should be in the same component
        root = uf.find('item_0')
        for i in range(21):
            self.assertEqual(uf.find(f'item_{i}'), root)


if __name__ == '__main__':
    unittest.main()
