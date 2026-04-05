import lmdb
import pickle
import os
import shutil
import tempfile
from typing import Optional, Any, Iterator, Union
from contextlib import contextmanager


class LmdbCache:
    """
    A memory-efficient cache using LMDB (Lightning Memory-Mapped Database).
    
    This class provides a dictionary-like interface backed by LMDB, allowing
    for constant memory usage regardless of the number of stored items.
    Ideal for caching large amounts of data without risking OOM errors.
    
    Usage:
        with LmdbCache('my_cache') as cache:
            cache['key1'] = 'value1'
            value = cache.get('key2')
            
        # Cache is automatically closed when exiting context
    """
    
    def __init__(self, name: str, path: Optional[str] = None, map_size: int = 1 * 1024**3):
        """
        Initialize LMDB cache.

        :param name: Unique name for this cache (used in directory name)
        :param path: Optional custom path for LMDB database. If None, uses temp directory.
        :param map_size: Maximum database size in bytes.
            On Windows LMDB pre-allocates a file of exactly ``map_size`` bytes;
            on Linux/macOS it uses sparse files so actual disk usage equals only
            the data written.  Increase this value when processing files with
            millions of unique IDs (e.g. ``map_size=20*1024**3`` for 20GB).
        """
        self.name = name
        self._env: Optional[lmdb.Environment] = None
        self.map_size = int(os.getenv('LMDB_MAP_SIZE', str(map_size)))  # use env variable if specified, else default init value
        
        if path is None:
            # Create a temporary directory for the LMDB database
            self._temp_dir = tempfile.mkdtemp(prefix=f'lmdb_{name}_', dir='.')
            self.path = os.path.join(self._temp_dir, 'cache')
        else:
            self._temp_dir = None
            self.path = path
        
        self._is_open = False
    
    def open(self):
        """Open the LMDB environment."""
        if self._is_open:
            return
        
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(self.path) if os.path.dirname(self.path) else '.', exist_ok=True)
        
        self._env = lmdb.open(
            self.path,
            map_size=self.map_size,
            max_dbs=1,  # Allow one sub-database if needed
            writemap=False,  # Use write-through for durability
            metasync=False,   # Flush metadata asynchronously for speed
            sync=False,       # Don't sync to disk on every commit (speed over durability)
            readahead=True    # Enable readahead for better sequential read performance
        )
        self._is_open = True
    
    def close(self):
        """Close the LMDB environment."""
        if self._env is not None:
            self._env.close()
            self._env = None
            self._is_open = False
        
        # Clean up temporary directory if we created one
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass  # Ignore cleanup errors
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __del__(self):
        """Ensure cleanup on deletion."""
        self.close()
    
    def put(self, key: str, value: Any):
        """
        Store a key-value pair.
        
        :param key: String key
        :param value: Any picklable value
        """
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        # Serialize value
        serialized_value = pickle.dumps(value)
        
        with self._env.begin(write=True) as txn:
            txn.put(key.encode('utf-8'), serialized_value)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a value by key.
        
        :param key: String key
        :param default: Default value if key not found
        :return: The stored value or default
        """
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=False) as txn:
            value = txn.get(key.encode('utf-8'))
            if value is None:
                return default
            return pickle.loads(value)
    
    def __setitem__(self, key: str, value: Any):
        """Allow dict-like assignment: cache[key] = value"""
        self.put(key, value)
    
    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access: value = cache[key]"""
        value = self.get(key)
        if value is None:
            raise KeyError(f"Key '{key}' not found in cache")
        return value
    
    def __contains__(self, key: str) -> bool:
        """Allow 'in' operator: if key in cache.
        Returns ``False`` for empty strings (LMDB does not allow zero-length keys).
        """
        if not key:
            return False
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=False) as txn:
            return txn.get(key.encode('utf-8')) is not None
    
    def delete(self, key: str):
        """
        Delete a key-value pair.
        
        :param key: String key to delete
        """
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=True) as txn:
            txn.delete(key.encode('utf-8'))
    
    def __delitem__(self, key: str):
        """Allow dict-like deletion: del cache[key]"""
        self.delete(key)
    
    def keys(self) -> Iterator[str]:
        """Iterate over all keys in the cache."""
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=False) as txn:
            cursor = txn.cursor()
            for key_bytes in cursor.iternext(keys=True, values=False):
                yield key_bytes.decode('utf-8')
    
    def values(self) -> Iterator[Any]:
        """Iterate over all values in the cache."""
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=False) as txn:
            cursor = txn.cursor()
            for value_bytes in cursor.iternext(keys=False, values=True):
                yield pickle.loads(value_bytes)
    
    def items(self) -> Iterator[tuple[str, Any]]:
        """Iterate over all key-value pairs in the cache."""
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=False) as txn:
            cursor = txn.cursor()
            for key_bytes, value_bytes in cursor:
                yield (key_bytes.decode('utf-8'), pickle.loads(value_bytes))
    
    def __len__(self) -> int:
        """Return the number of items in the cache."""
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=False) as txn:
            return txn.stat()['entries']
    
    def clear(self):
        """Remove all items from the cache."""
        if not self._is_open:
            raise RuntimeError("LMDB cache is not open. Use context manager or call open() first.")
        
        with self._env.begin(write=True) as txn:
            cursor = txn.cursor()
            for key in cursor.iternext(keys=True, values=False):
                txn.delete(key)
    
    def __bool__(self) -> bool:
        """Return True if cache is not empty."""
        return len(self) > 0


class InMemoryCache:
    """
    A simple in-memory cache that mimics the LmdbCache interface.
    
    This is used for small datasets where LMDB overhead is unnecessary.
    It provides the same interface as LmdbCache but stores everything in RAM.
    """
    
    def __init__(self, name: str, path: Optional[str] = None, max_size: int = 10**10):
        """
        Initialize in-memory cache.
        
        :param name: Name of the cache (unused, for API compatibility)
        :param path: Unused, for API compatibility
        :param max_size: Unused, for API compatibility
        """
        self.name = name
        self._data: dict = {}
        self._is_open = True
    
    def open(self):
        """No-op for in-memory cache."""
        self._is_open = True
    
    def close(self):
        """No-op for in-memory cache."""
        self._is_open = False
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def put(self, key: str, value: Any):
        """Store a key-value pair."""
        self._data[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value by key."""
        return self._data.get(key, default)
    
    def __setitem__(self, key: str, value: Any):
        self.put(key, value)
    
    def __getitem__(self, key: str) -> Any:
        if key not in self._data:
            raise KeyError(f"Key '{key}' not found in cache")
        return self._data[key]
    
    def __contains__(self, key: str) -> bool:
        return key in self._data
    
    def delete(self, key: str):
        """Delete a key-value pair."""
        if key in self._data:
            del self._data[key]
    
    def __delitem__(self, key: str):
        self.delete(key)
    
    def keys(self) -> Iterator[str]:
        return iter(self._data.keys())
    
    def values(self) -> Iterator[Any]:
        return iter(self._data.values())
    
    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self._data.items())
    
    def __len__(self) -> int:
        return len(self._data)
    
    def clear(self):
        """Remove all items from the cache."""
        self._data.clear()
    
    def __bool__(self) -> bool:
        return bool(self._data)


class LmdbUnionFind:
    """
    A Union-Find (Disjoint Set Union) data structure persisted entirely in LMDB.

    Each element maps to its parent. Roots map to themselves.
    Supports path-compression on ``find``.  Union is by arbitrary root
    (i.e. root of x becomes child of root of y).

    This class does NOT manage the lifecycle of the LMDB environment it
    receives; the caller is responsible for opening and closing ``env``.

    Typical usage inside a ``try/finally`` block::

        env = lmdb.open(tmp_path, map_size=10 * 1024**3, sync=False)
        try:
            uf = LmdbUnionFind(env)
            uf.union('a', 'b')
            uf.union('b', 'c')
            for root, members in uf.iter_components():
                print(root, members)
        finally:
            env.close()
            shutil.rmtree(tmp_path)
    """

    def __init__(self, env: lmdb.Environment):
        """
        :param env: An already-opened ``lmdb.Environment``.
        """
        self._env = env

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def find(self, x: str) -> str:
        """
        Return the root of the component containing *x*.

        If *x* has never been seen before it is registered as its own root.
        Path compression is applied: all nodes along the path to the root are
        updated to point directly to the root.

        :param x: Element identifier (arbitrary non-empty string).
        :return:  Root identifier of the component.
        :raises ValueError: if *x* is an empty string (LMDB does not allow
            zero-length keys).
        """
        if not x:
            raise ValueError("LmdbUnionFind: element identifier must be a non-empty string.")
        path: list[str] = []
        current = x

        # --- read-only traversal to locate root ---
        with self._env.begin(write=False) as txn:
            while True:
                raw = txn.get(current.encode('utf-8'))
                if raw is None:
                    # 'current' (and therefore 'x') has never been seen.
                    root = current
                    is_new = True
                    break
                parent = raw.decode('utf-8')
                if parent == current:
                    root = current
                    is_new = False
                    break
                path.append(current)
                current = parent

        # --- write phase: register new node and/or apply path compression ---
        if is_new or path:
            with self._env.begin(write=True) as txn:
                if is_new:
                    # Register x (and any un-initialised ancestors) as self-root.
                    txn.put(current.encode('utf-8'), current.encode('utf-8'))
                # Path compression: point every node on the path directly to root.
                root_bytes = root.encode('utf-8')
                for node in path:
                    txn.put(node.encode('utf-8'), root_bytes)

        return root

    def __contains__(self, x: str) -> bool:
        """
        Return ``True`` if *x* has been registered in the Union-Find.

        Unlike ``find``, this method does **not** register *x* as a new node
        if it is absent.  Returns ``False`` immediately for empty strings
        (LMDB does not allow zero-length keys).

        :param x: Element identifier to test.
        :return: ``True`` if *x* is a known element, ``False`` otherwise.
        """
        if not x:
            return False
        with self._env.begin(write=False) as txn:
            return txn.get(x.encode('utf-8')) is not None

    def union(self, x: str, y: str) -> None:
        """
        Merge the components containing *x* and *y*.

        After the call, ``find(x) == find(y)``.  Specifically the root of *x*
        is made a child of the root of *y*.

        :param x: First element.
        :param y: Second element.
        """
        rx = self.find(x)
        ry = self.find(y)
        if rx != ry:
            with self._env.begin(write=True) as txn:
                txn.put(rx.encode('utf-8'), ry.encode('utf-8'))

    # ------------------------------------------------------------------
    # Component enumeration
    # ------------------------------------------------------------------

    def iter_components(self) -> Iterator[tuple[str, set]]:
        """
        Iterate over all components, yielding ``(root, members_set)`` pairs.

        This performs one full key-scan of the LMDB database followed by one
        ``find`` call per element (with path-compression side-effects).
        Peak RAM usage is proportional to the number of distinct components,
        not the total number of elements.

        :return: Iterator of ``(root_str, set_of_member_strings)`` pairs.
        """
        # Collect all keys in a single read transaction.
        with self._env.begin(write=False) as txn:
            all_keys: list[str] = [
                k.decode('utf-8')
                for k in txn.cursor().iternext(keys=True, values=False)
            ]

        # Group by root.  find() may write back path-compressed parents but
        # that is safe because we are no longer inside the read transaction.
        components: dict[str, set] = {}
        for key in all_keys:
            root = self.find(key)
            if root not in components:
                components[root] = set()
            components[root].add(key)

        yield from components.items()


class InMemoryUnionFind:
    """
    An in-memory Union-Find (Disjoint Set Union) data structure.

    Each element maps to its parent. Roots map to themselves.
    Supports path-compression on ``find``.  Union is by arbitrary root
    (i.e. root of x becomes child of root of y).

    This is a memory-based alternative to LmdbUnionFind, suitable for
    small datasets where LMDB overhead is unnecessary.

    Usage::

        uf = InMemoryUnionFind()
        uf.union('a', 'b')
        uf.union('b', 'c')
        for root, members in uf.iter_components():
            print(root, members)
    """

    def __init__(self):
        """
        Initialize an empty Union-Find structure.
        """
        self._data: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def find(self, x: str) -> str:
        """
        Return the root of the component containing *x*.

        If *x* has never been seen before it is registered as its own root.
        Path compression is applied: all nodes along the path to the root are
        updated to point directly to the root.

        :param x: Element identifier (arbitrary non-empty string).
        :return:  Root identifier of the component.
        :raises ValueError: if *x* is an empty string.
        """
        if not x:
            raise ValueError("InMemoryUnionFind: element identifier must be a non-empty string.")

        # If x is not registered, register it as its own root
        if x not in self._data:
            self._data[x] = x
            return x

        # Find root with path compression
        path: list[str] = []
        current = x
        while current != self._data[current]:
            path.append(current)
            current = self._data[current]
        root = current

        # Apply path compression
        for node in path:
            self._data[node] = root

        return root

    def __contains__(self, x: str) -> bool:
        """
        Return ``True`` if *x* has been registered in the Union-Find.

        Unlike ``find``, this method does **not** register *x* as a new node
        if it is absent.

        :param x: Element identifier to test.
        :return: ``True`` if *x* is a known element, ``False`` otherwise.
        """
        if not x:
            return False
        return x in self._data

    def union(self, x: str, y: str) -> None:
        """
        Merge the components containing *x* and *y*.

        After the call, ``find(x) == find(y)``.  Specifically the root of *x*
        is made a child of the root of *y*.

        :param x: First element.
        :param y: Second element.
        """
        rx = self.find(x)
        ry = self.find(y)
        if rx != ry:
            self._data[rx] = ry

    # ------------------------------------------------------------------
    # Component enumeration
    # ------------------------------------------------------------------

    def iter_components(self) -> Iterator[tuple[str, set]]:
        """
        Iterate over all components, yielding ``(root, members_set)`` pairs.

        This performs one full iteration over all elements followed by one
        ``find`` call per element (with path-compression side-effects).
        Peak RAM usage is proportional to the number of distinct components,
        not the total number of elements.

        :return: Iterator of ``(root_str, set_of_member_strings)`` pairs.
        """
        # Group elements by root
        components: dict[str, set] = {}
        for key in self._data:
            root = self.find(key)
            if root not in components:
                components[root] = set()
            components[root].add(key)

        yield from components.items()


# Type alias for Union-Find implementations
UnionFind = Union[LmdbUnionFind, InMemoryUnionFind]
