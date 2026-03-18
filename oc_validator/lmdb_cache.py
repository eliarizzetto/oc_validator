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

import lmdb
import pickle
import os
import tempfile
from typing import Optional, Any, Iterator
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
    
    def __init__(self, name: str, path: Optional[str] = None, max_size: int =30*1024**3):
        """
        Initialize LMDB cache.
        
        :param name: Unique name for this cache (used in directory name)
        :param path: Optional custom path for LMDB database. If None, uses temp directory.
        :param max_size: Maximum database size in bytes (default: 30GB)
        """
        self.name = name
        self._env: Optional[lmdb.Environment] = None
        self.max_size = max_size
        
        if path is None:
            # Create a temporary directory for the LMDB database
            self._temp_dir = tempfile.mkdtemp(prefix=f'lmdb_{name}_')
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
            map_size=self.max_size,
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
                import shutil
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
        """Allow 'in' operator: if key in cache"""
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