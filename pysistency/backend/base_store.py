import urllib.parse
import pysistency.utilities.exceptions
import collections

from pysistency.utilities.constants import NOTSET


class PTPStoreException(pysistency.utilities.exceptions.PTPException):
    pass


class BucketNotFound(PTPStoreException):
    """A requested bucket is not stored"""


class BaseBucketStore(object):
    """
    Baseclass for Bucket Stores

    Bucket Stores are interfaces to backends persistently storing *buckets*:
    blobs of data identfied by a key. While generic in their implementation,
    the API of the stores is tailored specifically to the
    :py:mod:`~pysistency` containers.

    The following elements are used:

    **bucket**
      A blob of data: any pickle'able data structures is allowed.

    **bucket_key**
      Key to a bucket: any alphanumeric string is allowed, though some names
      are reserved and should not be used.

    **head**
      Reserved bucket containing metadata of the content of a store.

    **record**
      Reserved bucket containing metadata of the store itself.

    :note: A BucketStore represents the actual data of a persistent container.
           As such, it doesn't make sense to try and store multiple, different
           containers in the same BucketStore. While possible, it will lead to
           undefined behaviour. Future implementations may actively guard
           against this.
    """
    uri_scheme = None
    #: options accepted via the URI query part (?foo=2)
    uri_query_options = {}

    def __init__(self, store_uri):
        self._store_uri = None
        self.store_uri = store_uri
        #: whether a container's head is stored
        self._stores_head = False
        #: buckets *currently* provided by this store, exlucing head and record
        self.bucket_keys = set()

    def __repr__(self):
        return '%s(store_uri=%r)' % (self.__class__.__qualname__, self.store_uri)

    # URI handling
    ##############
    @property
    def store_uri(self):
        return self._store_uri

    @store_uri.setter
    def store_uri(self, value):
        parsed_url = urllib.parse.urlsplit(value)
        if not parsed_url.scheme == self.uri_scheme:
            raise ValueError('Class %s expected URI of scheme %s, got %s' % (
                self.__class__.__name__, self.uri_scheme, parsed_url.scheme
            ))
        self._store_uri = value
        self._digest_uri(parsed_url=parsed_url)
        self._load_record()

    def _digest_uri(self, parsed_url):
        raise NotImplementedError

    @staticmethod
    def _parse_query(url_query):
        """Parse a URL query component"""
        if not url_query:
            return {}
        key_values = [param.split('=', 1) for param in url_query.split('&')]
        return collections.OrderedDict(key_values)

    @classmethod
    def supports_uri(cls, store_uri):
        return urllib.parse.urlsplit(store_uri).scheme == cls.uri_scheme

    @classmethod
    def from_uri(cls, store_uri, default_scheme=NOTSET):
        # patch in scheme if none provided
        if not urllib.parse.urlsplit(store_uri).scheme:
            if default_scheme == NOTSET:
                raise ValueError('URI %r does not provide scheme and no fallback defined' % store_uri)
            store_uri = default_scheme + '://' + store_uri
        # prefer subclasses in case they overwrite the use of our protocol
        for sub_cls in cls.__subclasses__():
            try:
                return sub_cls.from_uri(store_uri=store_uri)
            except ValueError:
                continue
        # check whether we support it
        if cls.supports_uri(store_uri=store_uri):
            return cls(store_uri=store_uri)
        raise ValueError('URI %r not supported' % store_uri)

    # Bucket handling
    #################
    def _load_record(self):
        """Load and apply the store meta-data"""
        raise NotImplementedError

    def _store_record(self):
        """Store meta-data of the bucket; not for external use"""
        raise NotImplementedError

    def _fetch_record(self):
        """Fetch meta-data of the bucket; not for external use"""
        raise NotImplementedError

    def free_head(self):
        """
        Free the metadata of the stored container

        :warning: This operation likely makes content unreadable.

        :raises BucketNotFound: if no head is stored
        """
        raise NotImplementedError

    def fetch_head(self):
        """
        Fetch the metadata of the stored container

        :raises BucketNotFound: if no head is stored
        """
        raise NotImplementedError

    def store_head(self, head):
        """
        Store the metadata of the stored container

        :param head: data to store in the head
        """
        raise NotImplementedError

    def free_bucket(self, bucket_key):
        """
        Free a bucket; data will be no longer accessible afterwards

        :param bucket_key: key to the bucket
        :type bucket_key: str
        :raises BucketNotFound: if no head is stored
        """
        raise NotImplementedError

    def store_bucket(self, bucket_key, bucket):
        """
        Store a bucket, potentially overwriting previous versions

        :param bucket_key: key to the bucket
        :type bucket_key: str
        :type bucket: data to store in the bucket
        """
        raise NotImplementedError

    def fetch_bucket(self, bucket_key):
        """
        Fetch a bucket, potentially overwriting previous versions

        :param bucket_key: key to the bucket
        :type bucket_key: str
        :type bucket: data to store in the bucket
        :raises BucketNotFound: if no head is stored
        """
        raise NotImplementedError
