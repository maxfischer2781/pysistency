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
    cpdef _bucket_fmt_digits(self, bucket_count=*)
    # Bucket Management
    cpdef _update_bucket_key_fmt(self)
    cpdef _is_current_bucket_key(self, bucket_key)
    cpdef update_layout(self)
    cpdef _bucket_key(self, key)
    cpdef _get_bucket(self, bucket_key)
    cpdef _store_bucket(self, bucket_key, bucket)
    cpdef _set_cached_item(self, key, item)
    cpdef _get_cached_item(self, key)
    cpdef _del_cached_item(self, key)
    cpdef flush(self)
    # Special Methods
    # Dict Interface
    cpdef get(self, key, default=*)
    cpdef pop(self, key, default=*)
    cpdef popitem(self)
    cpdef setdefault(self, key, default=*)
    cpdef clear(self)
    #cpdef update(self, other=*, **kwargs)
    #    cpdef updatebuckets(key_values)
    cpdef keys(self)
    cpdef items(self)
    cpdef values(self)
    cpdef copy(self)
