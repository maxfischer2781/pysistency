import cython

from pysistency.backend.base_store cimport BaseBucketStore

cdef class DictBucket(dict):
    cdef object __weakref__

cdef class PersistentDict(object):
    # Fields
    cdef public str bucket_key_fmt
    cdef public set _keys_cache
    cdef public object _bucket_cache

    cdef public BaseBucketStore _bucket_store
    cdef public int _bucket_count
    cdef public int _bucket_salt
    cdef public int _cache_size
    cdef public bint _updating_layout

    cdef object _active_buckets
    cdef object _active_items

    # Methods
    cpdef _store_head(self)
    cpdef _load_head(self)
    cpdef _bucket_fmt_digits(self, int bucket_count=*)
    # Bucket Management
    cpdef _update_bucket_key_fmt(self)
    cpdef bint _is_current_bucket_key(self, str bucket_key)
    cpdef update_layout(self)
    cpdef str _bucket_key(self, object key)
    cpdef DictBucket _get_bucket(self, str bucket_key)
    cpdef _store_bucket(self, str bucket_key, DictBucket bucket)
    cpdef _set_cached_item(self, object key, object item)
    cpdef object _get_cached_item(self, object key)
    cpdef _del_cached_item(self, object key)
    cpdef flush(self)
    # Dict Interface
    cpdef object get(self, object key, object default=*)
    cpdef object pop(self, object key, object default=*)
    cpdef object popitem(self)
    cpdef object setdefault(self, object key, object default=*)
    cpdef clear(self)
    cpdef _update_buckets(self, list key_values)
    cpdef keys(self)
    cpdef items(self)
    cpdef values(self)
    cpdef copy(self)
