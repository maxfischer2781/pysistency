cimport base_store

cdef class FileBucketStore(base_store.BaseBucketStore):
    cdef public int _pickle_protocol
    cdef public int _permissions
    cdef public str _path

    cpdef _get_bucket_path(self, bucket_key)
    cpdef _store_bucket(self, bucket_key, bucket)
    cpdef _free_bucket(self, bucket_key)

    cpdef _load_record(self)
    cpdef _store_record(self)

    cpdef free_head(self)
    cpdef fetch_head(self)
    cpdef store_head(self, object head)
    cpdef free_bucket(self, str bucket_key)
    cpdef store_bucket(self, str bucket_key, object bucket)
    cpdef fetch_bucket(self, str bucket_key)
