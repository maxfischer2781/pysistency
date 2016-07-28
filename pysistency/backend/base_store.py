import urllib.parse
import pysistency.utilities.exceptions

from pysistency.utilities.constants import NOTSET


class PTPStoreException(pysistency.utilities.exceptions.PTPException):
    pass


class BucketNotFound(PTPStoreException):
    """A requested bucket is not stored"""


class BaseBucketStore(object):
    uri_scheme = None

    def __init__(self, store_uri):
        self._store_uri = None
        self.store_uri = store_uri

    def __repr__(self):
        return '%s(store_uri=%r)' % (self.__class__.__qualname__, self.store_uri)

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

    def _digest_uri(self, parsed_url):
        raise NotImplementedError

    @classmethod
    def supports_uri(cls, store_uri):
        return urllib.parse.urlsplit(store_uri).scheme == cls.uri_scheme

    @classmethod
    def from_uri(cls, store_uri, default_scheme=NOTSET):
        # patch in scheme if none provided
        if not urllib.parse.urlsplit(store_uri).scheme:
            if default_scheme == NOTSET:
                raise ValueError('URI %r does not provide scheme and no fallback defined' % store_uri)
            store_uri = default_scheme + ':' + store_uri
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
