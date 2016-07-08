from weakref import WeakValueDictionary
from collections import deque
import math

from pysistency.utilities.keys import hashkey, HASHKEY_HEXFMT
from pysistency.utilities.constants import NOTSET
from pysistency.backend.base_store import BaseBucketStore, BucketNotFound


class DictBucket(dict):
    """Subclass of dict allowing for weak references"""
    pass


class PersistentDict(object):
    """
    Mapping object that is persistently stored

    :param store_uri: URI for storing buckets; see :py:class:`~BaseBucketStore`
    :type store_uri: :py:class:`str`

    :param bucket_count: number of buckets to use for storing data
    :type bucket_count: :py:class:`int`
    :param bucket_salt: salt for finding buckets to store data
    :type bucket_salt: :py:class:`int`

    :param cache_size: number of buckets to LRU-cache in memory
    :type cache_size: :py:class:`int`
    :param cache_keys: whether to cache all keys in memory
    :type cache_keys: :py:class:`bool`
    """
    persistent_defaults = {
        'bucket_count': 32,
        'bucket_salt': 0,
    }

    def __init__(self, store_uri, bucket_count=NOTSET, bucket_salt=NOTSET, cache_size=3, cache_keys=True):
        self._bucket_store = BaseBucketStore.from_uri(store_uri=store_uri, default_scheme='file')
        # set empty fields
        self._bucket_count = None
        self._bucket_salt = None
        self._bucket_keys = set()
        self.bucket_key_fmt = None
        self._keys_cache = None
        self._bucket_cache = None
        self._cache_size = None
        # load current settings
        try:
            for attr, value in self._bucket_store.fetch_head().items():
                setattr(self, attr, value)
        except BucketNotFound:
            pass
        # apply new settings
        self.bucket_count = bucket_count
        self.bucket_salt = bucket_salt
        # LRU store for objects fetched from disk
        self.cache_size = cache_size
        # weakref store for objects still in use
        self._active_buckets = WeakValueDictionary()
        self._active_items = WeakValueDictionary()
        # store new settings
        self._store_head()
        # cache keys in memory
        if cache_keys:
            self._keys_cache = set(self.keys())

    # Settings
    def _store_head(self):
        """
        Store the meta-information of the dict
        """
        self._bucket_store.store_head({
                attr: getattr(self, attr) for attr in
                ('bucket_count', 'bucket_salt', '_bucket_keys')
            })

    def _bucket_fmt_digits(self, bucket_count=None):
        """Return the number of hex digits required for the bucket name"""
        bucket_count = bucket_count or self._bucket_count
        return max(int(math.ceil(math.log(bucket_count, 16))), 1)

    # exposed settings
    @property
    def cache_size(self):
        return self._cache_size

    @cache_size.setter
    def cache_size(self, value):
        self._cache_size = int(value or 1)
        self._bucket_cache = deque(maxlen=self.cache_size)

    @property
    def bucket_salt(self):
        """
        Get/Set the ``bucket_salt`` of the persistent mapping

        :note: Setting ``bucket_salt`` causes **all** buckets storing data to be
               recreated. Until the new buckets have been created, changes to the
               mapping content may be silently dropped.
        """
        return self._bucket_salt

    @bucket_salt.setter
    def bucket_salt(self, value):
        # default if unset
        if value == NOTSET:
            if self._bucket_salt is not None:
                return
            self._bucket_salt = self.persistent_defaults['bucket_salt']
        else:
            value = int(value)
            # no change
            if self._bucket_salt == value:
                return
            # uninitialized, we don't have content yet
            elif self._bucket_salt is None:
                self._bucket_salt = value
            # TODO: allow resalting backend
            else:
                raise NotImplementedError('Changing bucket salt not implemented yet')
        self._update_bucket_key_fmt()

    @property
    def bucket_count(self):
        """
        Get/Set the ``bucket_count`` of the persistent mapping

        :note: Setting ``bucket_count`` causes **all** buckets storing data to be
               recreated. Until the new buckets have been created, changes to the
               mapping content may be silently dropped.
        """
        return self._bucket_count

    @bucket_count.setter
    def bucket_count(self, value):
        # default if unset
        if value == NOTSET:
            if self._bucket_count is not None:
                return
            self._bucket_count = self.persistent_defaults['bucket_count']
        else:
            value = int(value)
            if value < 1:
                raise ValueError('At least one bucket must be used')
            # no change
            elif self._bucket_count == value:
                return
            # uninitialized, we don't have content yet
            elif self._bucket_count is None:
                self._bucket_count = value
            # TODO: allow resizing backend
            else:
                raise NotImplementedError('Changing bucket count not implemented yet')
        # apply secondary settings
        self._update_bucket_key_fmt()

    def _update_bucket_key_fmt(self):
        # key: count, salt, index
        self.bucket_key_fmt = "pdictbkt_%(bucket_count)x%(bucket_salt)s%%0%(index_digits)dx" % {
            'bucket_count': self.bucket_count,
            'bucket_salt': HASHKEY_HEXFMT % hashkey(self.bucket_salt, self.bucket_salt),
            'index_digits': self._bucket_fmt_digits(),
        }

    # bucket management
    def _bucket_key(self, key):
        """
        Create the bucket identifier for a given key

        :param key: key to the content in-memory
        :return: key to the bucket stored persistently
        :rtype: str
        """
        return self.bucket_key_fmt % (hashkey(key) % self._bucket_count)

    def _fetch_bucket(self, bucket_key):
        """
        Return a bucket from disk or create a new one

        :param bucket_key: key for the bucket
        :return: bucket for ``bucket_key``
        :rtype: :py:class:`~DictBucket`
        """
        try:
            bucket = self._bucket_store.fetch_bucket(bucket_key=bucket_key)
        except BucketNotFound:
            bucket = DictBucket()
        self._active_buckets[bucket_key] = bucket
        self._bucket_cache.appendleft(bucket)
        return bucket

    def _get_bucket(self, bucket_key):
        """
        Return the appropriate bucket

        May return the cached bucket if available.

        :param bucket_key: key for the bucket
        :return: bucket for ``bucket_key``
        :rtype: :py:class:`~DictBucket`
        """
        try:
            return self._active_buckets[bucket_key]
        except KeyError:
            return self._fetch_bucket(bucket_key)

    def _store_bucket(self, bucket_key, bucket=None):
        """
        Store a bucket on disk

        :param bucket_key: key for the entire bucket
        """
        if bucket is None:
            try:
                bucket = self._active_buckets[bucket_key]
            except KeyError:
                return
        if bucket:
            self._bucket_store.store_bucket(bucket_key=bucket_key, bucket=bucket)
            self._add_bucket_key(bucket_key)
        # free empty buckets
        else:
            self._bucket_store.free_bucket(bucket_key)
            self._discard_bucket_key(bucket_key)

    def _add_bucket_key(self, bucket_key):
        if bucket_key not in self._bucket_keys:
            self._bucket_keys.add(bucket_key)
            self._store_head()

    def _discard_bucket_key(self, bucket_key):
        if bucket_key in self._bucket_keys:
            self._bucket_keys.add(bucket_key)
            self._store_head()

    # cache management
    # Item cache
    def _set_cached_item(self, key, item):
        """Cache reference to existing item"""
        try:
            self._active_items[key] = item
        except TypeError:
            pass

    def _get_cached_item(self, key):
        """Get reference to existing item; raises KeyError if item cannot be fetched"""
        try:
            return self._active_items[key]
        except TypeError:
            raise KeyError

    def _del_cached_item(self, key):
        """Release reference to existing item"""
        try:
            del self._active_items[key]
        except (TypeError, KeyError):
            pass

    # paths and files
    def flush(self):
        """
        Commit all outstanding changes to persistent store
        """
        for bucket_key, bucket in self._active_buckets.values():
            self._store_bucket(bucket_key, bucket)

    # dictionary interface
    def __getitem__(self, key):
        # - use cached reference to existing item
        # - fetch item from cached reference to existing bucket
        # - fetch item from fetched bucket
        try:
            return self._get_cached_item(key)
        except KeyError:
            bucket = self._get_bucket(self._bucket_key(key))
            item = bucket[key]
        self._set_cached_item(key, item)
        return item

    def __setitem__(self, key, value):
        bucket_key = self._bucket_key(key)
        bucket = self._get_bucket(bucket_key)
        bucket[key] = value
        self._store_bucket(bucket_key, bucket)
        if self._keys_cache is not None:
            self._keys_cache.add(key)
        # update item cache
        self._set_cached_item(key, value)

    def __delitem__(self, key):
        bucket_key = self._bucket_key(key)
        bucket = self._get_bucket(bucket_key)
        del bucket[key]
        self._store_bucket(bucket_key)
        if self._keys_cache is not None:
            self._keys_cache.discard(key)
        self._del_cached_item(key)

    # container protocol
    def __contains__(self, key):
        if self._keys_cache is not None:
            return key in self._keys_cache
        elif key in self._active_items:
            return True
        else:
            bucket = self._get_bucket(self._bucket_key(key))
            return key in bucket

    def __len__(self):
        return len(self.keys())

    def __bool__(self):
        # can only have items if we have buckets
        return bool(self._bucket_keys)

    __nonzero__ = __bool__

    def __iter__(self):
        """:see: :py:meth:`~.PersistentDict.iterkeys`"""
        read_buckets = set()
        # start with the buckets we have in memory
        for bucket_key in self._active_buckets.keys():
            for item_key in self._active_buckets[bucket_key].keys():
                yield item_key
            read_buckets.add(bucket_key)
        # pull in all buckets
        for bucket_key in self._bucket_keys:
            if bucket_key not in read_buckets:
                bucket = self._fetch_bucket(bucket_key)
                for item_key in bucket.keys():
                    yield item_key
                read_buckets.add(bucket_key)

    # dictionary methods
    def get(self, key, default=None):
        """
        Return the value for key if key is in the dictionary, else default. If
        default is not given, it defaults to ``None``, so that this method never
        raises a :py:exc:`KeyError`.

        :param key: key to an item in the dictionary
        :param default: default to return if no item exists
        :raises KeyError: if no items exists and no default is given
        """
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, default=NOTSET):
        """
        If ``key`` is in the dictionary, remove it and return its value, else return
        ``default``. If ``default`` is not given and ``key`` is not in the dictionary,
        a KeyError is raised.

        :param key: key to an item in the dictionary
        :param default: default to return if no item exists
        :raises KeyError: if no items exists and no default is given
        """
        try:
            item = self[key]
            del self[key]
        except KeyError:
            if default is NOTSET:
                raise
            item = default
        return item

    def setdefault(self, key, default=None):
        """
        If key is in the dictionary, return its value. If not, insert key with a
        value of ``default`` and return ``default``. ``default`` defaults to
        ``None``.

        :param key: key to an item in the dictionary
        :param default: default to insert and return if no item exists
        """
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def clear(self):
        """Remove all items from the dictionary."""
        # clear persistent storage
        for bucket_key in self._bucket_keys:
            self._bucket_store.free_bucket(bucket_key=bucket_key)
        self._bucket_keys = type(self._bucket_keys)()
        self._store_head()
        # reset caches
        self._bucket_cache = deque(maxlen=self.cache_size)
        self._active_buckets = type(self._active_buckets)()
        self._active_items = type(self._active_items)()
        self._keys_cache = None if self._keys_cache is None else type(self._keys_cache)()

    def update(self, other=None, **kwargs):
        """
        Update the dictionary with the ``(key,value)`` pairs from other,
        overwriting existing keys.

        :py:meth:`~.PersistentDict.update` accepts either another dictionary
        object or an iterable of ``(key,value)`` pairs (as tuples or other
        iterables of length two). If keyword arguments are specified, the
        dictionary is then updated with those ``(key,value)`` pairs:
        ``d.update(red=1, blue=2)``.

        :param other: mapping or iterable of ``(key,value)`` pairs
        :param kwargs: ``key=value`` arguments to insert
        :return: None

        :note: This function is faster for large collections as changes are made
               per bucket, not per item. The drawback is a larger memory consumption
               as the entire input is sorted in memory.
        """
        def updatebuckets(key_values):
            """
            Commit entire buckets from key, value pairs

            :param key_values: iterable of ``(key, value)`` pairs
            """
            # sort kvs by bucket
            key_values = sorted(key_values, key=lambda key_val: self._bucket_key(key_val[0]))
            # insert kvs by bucket
            last_bucket_key, bucket = None, None
            for key, value in key_values:
                bucket_key = self._bucket_key(key)
                # cycle to next bucket if current one is done
                if bucket_key != last_bucket_key:
                    if last_bucket_key is not None:
                        self._store_bucket(last_bucket_key)
                    last_bucket_key = bucket_key
                    bucket = self._get_bucket(bucket_key)
                # update bucket
                bucket[key] = value
                # update caches
                if self._keys_cache is not None:
                    self._keys_cache.add(key)
                self._set_cached_item(key, value)
            # commit outstanding bucket, if any
            if last_bucket_key is not None:
                self._store_bucket(last_bucket_key)
        if other is not None:
            if hasattr(other, "items"):  # dictionary
                updatebuckets(other.items())
            else:  # sequence
                updatebuckets(other)
        updatebuckets(kwargs.items())

    # iterations
    def iter(self):
        """
        Return an iterator over the keys of the dictionary. This is a shortcut
        for :py:meth:`~.PersistentDict.iterkeys`.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        return self.iterkeys()

    def iterkeys(self):
        """
        Return an iterator over the keys of the dictionary.

        This iterates over all keys in a semi-deterministic way. First, all keys
        from buckets cached in memory are returned. Following this, keys from the
        remaining buckets are returned.

        :note: This function does not benefit from ``cache_keys``.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        return iter(self)

    def keys(self):
        """
        Return a copy of the dictionary’s list of keys.

        :note: If ``cache_keys`` is set, keys are returned in arbitrary order.
               Otherwise, the order of :py:meth:`~.PersistentDict.iterkeys` is
               used.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        if self._keys_cache is not None:
            return list(self._keys_cache)
        return list(iter(self))

    def iteritems(self):
        """
        Return an iterator over the dictionary’s list of ``(key, value)`` pairs.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        for item_key in self:
            yield (item_key, self[item_key])

    def items(self):
        """
        Return a copy of the dictionary’s list of ``(key, value)`` pairs.

        :note: Since the state of the mapping also depends on accesses, the strict
               guarantee for iteration sequence equivalence given by ``dict`` is
               not replicated. Thus, it cannot be assumed that
               ``d.items() == zip(d.values(), d.keys()) == zip(d.itervalues(), d.iterkeys()) == [(v, k) for (k, v) in d.iteritems()]``
               holds true in any case.
        """
        return list(self.iteritems())

    def itervalues(self):
        """
        Return an iterator over the dictionary’s values.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        try:
            for item_key in self:
                yield self[item_key]
        except KeyError:
            raise RuntimeError("dictionary changed size during iteration")

    def values(self):
        """
        Return a copy of the dictionary’s list of values.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        return list(self.itervalues())

    # high level operations
    def copy(self):
        """
        Return a shallow copy of the dictionary.

        :note: This will return a ``dict``, not a :py:class:`~.PersistentDict`.
        """
        return dict(self.iteritems())

    def __repr__(self):
        return "%s(bucket_store=%r, bucket_count=%r, cache_size=%r, cache_keys=%r, items={%s})" % (
            self.__class__.__name__,
            self._bucket_store,
            self.bucket_count,
            self.cache_size,
            self._keys_cache is not None,
            self.__repr_content(),
        )

    def __repr_content(self):
        reprs = []
        read_keys = set()
        for bucket_key in self._active_buckets.keys():
            try:
                bucket = self._active_buckets[bucket_key]
                if not bucket:
                    continue
                reprs.append(repr(bucket)[1:-1])
                read_keys.update(bucket.keys())
            except KeyError:
                pass
        if self._keys_cache is None:
            reprs.append(", ...")
        elif self._keys_cache:
            cache_repr = ": <?>, ".join(repr(key) for key in self._keys_cache if key not in read_keys)
            if cache_repr:
                reprs.append(cache_repr + ": <?>")
        return ",".join(reprs)
