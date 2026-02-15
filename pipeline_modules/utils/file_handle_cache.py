"""
LRU File Handle Cache for managing large numbers of per-patient output files.

Solves the file descriptor exhaustion problem when processing data for 500K+ patients.
Instead of keeping all files open simultaneously (hitting OS ulimit), this cache
keeps at most `max_handles` files open at any time, closing the least-recently-used
ones when the limit is reached.

Usage:
    from utils.file_handle_cache import FileHandleCache

    cache = FileHandleCache(max_handles=200)
    try:
        for patient_id, data in process_data():
            file_path = get_patient_file(patient_id)
            handle, is_new = cache.get_handle(file_path)
            if is_new:
                data.to_csv(handle, index=False)  # Write with header
            else:
                data.to_csv(handle, header=False, index=False)  # Append without header
    finally:
        cache.close_all()
"""

from collections import OrderedDict
from pathlib import Path
from typing import Tuple
import io


class FileHandleCache:
    """
    LRU cache for file handles with a configurable maximum.
    
    When the cache is full and a new file needs to be opened,
    the least-recently-used file handle is closed. If that file
    is accessed again later, it's reopened in append mode.
    
    Thread safety: NOT thread-safe. Use one cache per worker process.
    
    Args:
        max_handles: Maximum number of simultaneously open file handles.
                     Default 200 is safe for most OS configurations.
        buffer_size: Write buffer size for each file handle.
                     Default 65536 (64KB) balances memory and I/O performance.
    """
    
    def __init__(self, max_handles: int = 200, buffer_size: int = 65536):
        self.max_handles = max_handles
        self.buffer_size = buffer_size
        self.handles: OrderedDict[str, io.TextIOWrapper] = OrderedDict()
        self._files_created: set = set()  # Track which files we've created (written header to)
        self._stats = {
            'opens': 0,
            'reopens': 0,
            'evictions': 0,
            'hits': 0,
        }
    
    def get_handle(self, file_path) -> Tuple[io.TextIOWrapper, bool]:
        """
        Get a file handle for writing. Returns (handle, needs_header).
        
        - If file is already open: returns it (LRU hit), needs_header=False
        - If file was opened before but evicted: reopens in append mode, needs_header=False  
        - If file is brand new: opens in write mode, needs_header=True
        
        Args:
            file_path: Path to the file (str or Path)
            
        Returns:
            (handle, needs_header): The file handle and whether a header should be written
        """
        file_path = str(file_path)
        
        # Case 1: Already open — LRU hit
        if file_path in self.handles:
            self.handles.move_to_end(file_path)
            self._stats['hits'] += 1
            return self.handles[file_path], False
        
        # Need to open — first check if we need to evict
        if len(self.handles) >= self.max_handles:
            self._evict_oldest()
        
        # Case 2: Previously created but evicted — reopen in append mode
        if file_path in self._files_created:
            handle = open(file_path, 'a', newline='', encoding='utf-8',
                         buffering=self.buffer_size)
            self.handles[file_path] = handle
            self._stats['reopens'] += 1
            return handle, False
        
        # Case 3: Brand new file — open in write mode
        # Ensure parent directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        handle = open(file_path, 'w', newline='', encoding='utf-8',
                     buffering=self.buffer_size)
        self.handles[file_path] = handle
        self._files_created.add(file_path)
        self._stats['opens'] += 1
        return handle, True
    
    def _evict_oldest(self):
        """Close and remove the least-recently-used file handle."""
        if self.handles:
            oldest_path, oldest_handle = self.handles.popitem(last=False)
            try:
                oldest_handle.close()
            except Exception:
                pass  # Handle may already be closed
            self._stats['evictions'] += 1
    
    def close_all(self):
        """Close all open file handles. Call this when done processing."""
        for handle in self.handles.values():
            try:
                handle.close()
            except Exception:
                pass
        self.handles.clear()
    
    def get_stats(self) -> dict:
        """Return cache statistics for debugging/logging."""
        return {
            **self._stats,
            'currently_open': len(self.handles),
            'total_files_created': len(self._files_created),
        }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()
        return False
    
    def __del__(self):
        self.close_all()
