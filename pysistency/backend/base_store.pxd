cdef class BaseBucketStore(object):
    cdef public bint _stores_head
    cdef public str _store_uri

    cdef public set bucket_keys

    cpdef _load_record(self)
    cpdef _store_record(self)

    cpdef free_head(self)
    cpdef fetch_head(self)
    cpdef store_head(self, object head)
    cpdef free_bucket(self, str bucket_key)
    cpdef store_bucket(self, str bucket_key, object bucket)
    cpdef fetch_bucket(self, str bucket_key)
