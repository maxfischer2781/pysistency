cdef class BaseBucketStore(object):
    cdef bint _stores_head
    cdef str _store_uri

    #cdef public str uri_scheme
    cdef public set bucket_keys

    cpdef free_head(self)
    cpdef fetch_head(self)
    cpdef store_head(self, object head)
    cpdef free_bucket(self, object bucket_key)
    cpdef store_bucket(self, object bucket_key, str bucket)
    cpdef fetch_bucket(self, object bucket_key)
