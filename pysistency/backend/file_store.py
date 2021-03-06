import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

from . import base_store


class FileBucketStore(base_store.BaseBucketStore):
    """
    Bucket Store to files

    :param store_uri: URI defining path to store buckets
    :type store_uri: str

    Recognized URI parameters:

    `pickleprotocol` -> int
      The :py:mod:`pickle` protocol to use. Defaults to :py:data:`pickle.HIGHEST_PROTOCOL`.

    `permissions` -> int
      The permissions to set/expect on the containing folder. Defaults to `0o777`.

    Example URI:

    `file:///tmp/persist/`
      Store files in folder `/tmp/persist/`.

    `file:///tmp/persist/foo`
      Store files in folder `/tmp/persist/`, prefixed with `foo`.

    `file://cache/persist/?pickleprotocol=2`
       Store files in folder `cache/persist`, using :py:mod:`pickle` protocol 2.
    """
    uri_scheme = 'file'

    def __init__(self, store_uri):
        self._pickle_protocol = None
        self._permissions = None
        self._path = None
        base_store.BaseBucketStore.__init__(self, store_uri=store_uri)
        os.makedirs(self._path, mode=self._permissions, exist_ok=True)

    def _digest_uri(self, parsed_url):
        if parsed_url.netloc:  # netloc is set for relative paths
            self._path = os.path.join(
                os.getcwd(),
                parsed_url.netloc.lstrip('/'),
                parsed_url.path.lstrip('/')
            )
        else:
            self._path = parsed_url.path
        if ';' in self._path:  # file URI does not accept parameter
            raise ValueError('URI contains parameter(s)')  # ...;foo=bar
        parameters = self._parse_query(parsed_url.query)
        self._pickle_protocol = int(parameters.pop(
            'pickleprotocol',
            pickle.HIGHEST_PROTOCOL
        ))
        self._permissions = int(parameters.pop(
            'permissions',
            (os.stat(os.path.dirname(self._path)).st_mode & 0o777)
            if os.path.exists(os.path.dirname(self._path))
            else 0o777
        ))
        if parameters:  # file URI does not accept leftover queries
            raise ValueError('Unrecognized URI query parameter(s) %s' % list(parameters.keys()))  # ...?foo=bar

    def _get_bucket_path(self, bucket_key):
        return self._path + bucket_key + '.pkl'

    def free_head(self):
        return self.free_bucket(bucket_key='_header')

    def fetch_head(self):
        return self.fetch_bucket(bucket_key='_header')

    def store_head(self, head):
        return self.store_bucket(bucket_key='_header', bucket=head)

    def free_bucket(self, bucket_key):
        try:
            os.unlink(self._get_bucket_path(bucket_key=bucket_key))
        except FileNotFoundError:
            raise base_store.BucketNotFound

    def store_bucket(self, bucket_key, bucket):
        with open(self._get_bucket_path(bucket_key=bucket_key), 'wb') as bucket_file:
            pickle.dump(bucket, bucket_file)

    def fetch_bucket(self, bucket_key):
        try:
            with open(self._get_bucket_path(bucket_key=bucket_key), 'rb') as bucket_file:
                return pickle.load(bucket_file)
        except FileNotFoundError:
            raise base_store.BucketNotFound
