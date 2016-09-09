import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

from pysistency.utilities.constants import NOTSET

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
        try:
            self._pickle_protocol = int(parameters.pop('pickleprotocol'))
        except KeyError:
            self._pickle_protocol = NOTSET
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

    def _store_bucket(self, bucket_key, bucket):
        with open(self._get_bucket_path(bucket_key=bucket_key), 'wb') as bucket_file:
            pickle.dump(bucket, bucket_file)

    def _free_bucket(self, bucket_key):
        try:
            os.unlink(self._get_bucket_path(bucket_key=bucket_key))
        except FileNotFoundError:
            raise base_store.BucketNotFound

    def _load_record(self):
        try:
            record = self.fetch_bucket('record')
        except base_store.BucketNotFound:
            record = {}
        self.bucket_keys = record.get('bucket_keys', set())
        self._stores_head = record.get('_stores_head', False)
        if '_pickle_protocol' in record and self._pickle_protocol is not NOTSET:
            # resolve conflicting setting by rewriting buckets
            if record['_pickle_protocol'] != self._pickle_protocol:
                for bucket_key in self.bucket_keys:
                    self.store_bucket(bucket_key, self.fetch_bucket(bucket_key))
        elif '_pickle_protocol' not in record and self._pickle_protocol is NOTSET:
            # default if none set
            self._pickle_protocol = pickle.HIGHEST_PROTOCOL
        elif '_pickle_protocol' in record:
            self._pickle_protocol = record['_pickle_protocol']

    def _store_record(self):
        if self.bucket_keys or self._stores_head:
            self._store_bucket(
                'record',
                {
                    attr: getattr(self, attr) for attr in
                    ('bucket_keys', '_pickle_protocol', '_stores_head')
                }
            )
        else:
            # free our record if we don't actually hold data
            try:
                self._free_bucket('record')
            except base_store.BucketNotFound:
                pass

    def free_head(self):
        self._free_bucket(bucket_key='head')
        self._stores_head = False
        self._store_record()

    def fetch_head(self):
        return self.fetch_bucket(bucket_key='head')

    def store_head(self, head):
        self._store_bucket(bucket_key='head', bucket=head)
        self._stores_head = True
        self._store_record()

    def free_bucket(self, bucket_key):
        self._free_bucket(bucket_key)
        self.bucket_keys.discard(bucket_key)
        self._store_record()

    def store_bucket(self, bucket_key, bucket):
        self._store_bucket(bucket_key, bucket)
        if bucket_key not in self.bucket_keys:
            self.bucket_keys.add(bucket_key)
            self._store_record()

    def fetch_bucket(self, bucket_key):
        try:
            with open(self._get_bucket_path(bucket_key=bucket_key), 'rb') as bucket_file:
                return pickle.load(bucket_file)
        except FileNotFoundError:
            raise base_store.BucketNotFound
