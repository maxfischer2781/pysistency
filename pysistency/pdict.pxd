from pysistency.backend.base_store cimport BaseBucketStore

cdef class DictBucket(dict):
    cdef object __weakref__

cdef class PersistentDict(object):
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