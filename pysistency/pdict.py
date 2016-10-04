from weakref import WeakValueDictionary
from collections import deque, abc
import math
import string

from pysistency.utilities.std_clone import inherit_docstrings
from pysistency.utilities.keys import hashkey, HASHKEY_HEXFMT
from pysistency.utilities.constants import NOTSET
from pysistency.backend.base_store import BaseBucketStore, BucketNotFound


class DictBucket(dict):
    """Subclass of dict allowing for weak references"""
    pass


@inherit_docstrings(inherit_from=dict)
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
        self.bucket_key_fmt = None
        self._keys_cache = None
        self._bucket_cache = None
        self._cache_size = None
        self._updating_layout = False
        # load current settings
        self._load_head()
        # resume outstanding updates
        # this is only valid but also only possible if we have an
        # existing data structure that was being updated.
        if self._updating_layout:
            self.update()
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
        self.cache_keys = cache_keys

    @property
    def store_uri(self):
        return self._bucket_store.store_uri

    # Settings
    def _store_head(self):
        """Store the meta-information of the dict"""
        self._bucket_store.store_head({
            attr: getattr(self, attr) for attr in
            # work directly on internal values, setters are called as part of init for finalization
            ('_bucket_count', '_bucket_salt', '_updating_layout')
        })

    def _load_head(self):
        """Load the meta-information of the dict"""
        try:
            head = self._bucket_store.fetch_head()
        except BucketNotFound:
            pass
        else:
            for attr, value in head.items():
                setattr(self, attr, value)
            self._update_bucket_key_fmt()

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
                self._bucket_salt = value
                self._update_bucket_key_fmt()
                self.update_layout()
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
            else:
                self._bucket_count = value
                self._update_bucket_key_fmt()
                self.update_layout()
        # apply secondary settings
        self._update_bucket_key_fmt()

    @property
    def cache_keys(self):
        return self._keys_cache is not None

    @cache_keys.setter
    def cache_keys(self, value):
        if value and self._keys_cache is None:  # switch on
            self._keys_cache = set(self.keys())
        elif not value and self._keys_cache is not None:  # switch off
            self._keys_cache = None

    # data placement
    # Note: updating layouts relies on new/old bucket keys not conflicting
    def _update_bucket_key_fmt(self):
        # FIX: during initialization, either count or salt may be None
        # delay until everything is there
        if self._bucket_count is None or self._bucket_salt is None:
            assert self.bucket_key_fmt is None, 'Cound and Salt must exist after initialization'
            return
        # key: count, salt, index
        self.bucket_key_fmt = "%(bucket_count)x%(bucket_salt)s%%0%(index_digits)dx" % {
            'bucket_count': self.bucket_count,
            'bucket_salt': HASHKEY_HEXFMT % hashkey(self.bucket_salt, self.bucket_salt),
            'index_digits': self._bucket_fmt_digits(),
        }

    def _is_current_bucket_key(self, bucket_key):
        """Test whether `bucket_key` belongs to the currently used data layout"""
        layout = "%(bucket_count)x%(bucket_salt)s" % {
            'bucket_count': self.bucket_count,
            'bucket_salt': HASHKEY_HEXFMT % hashkey(self.bucket_salt, self.bucket_salt),
        }
        if bucket_key.startswith(layout):
            position = bucket_key[len(layout):]
            if len(position) == self._bucket_fmt_digits():
                return all(char in string.hexdigits for char in position)
        return False

    def update_layout(self):
        """
        Recreate the data layout

        :note: This method is primarily intended for internal use when the data
               layout *changes*. However, it does have application beyond, e.g.
               to recover a broken layout.
        """
        # start an update by marking the data structure as being updated
        # if we get interrupted, the next process may resume.
        self._updating_layout = True
        self._store_head()
        # If we are resuming an update, we get both new and old buckets
        # from the store. If a bucket is new, rewriting will not give us
        # a benefit: it's readable and we have its data.
        # If a bucket is old, fetch its data and write right back to us
        # regularly. We can release old buckets since keys *must* never
        # collide for different layouts.
        # We don't try to optimize reading old to writing new to avoid
        # having to make assumptions. The bucket key format should be
        # optimized so that new and old buckets are sub/supersets.
        for old_bucket_key in list(self._bucket_store.bucket_keys):
            if not self._is_current_bucket_key(old_bucket_key):
                bucket_data = self._get_bucket(old_bucket_key)
                self.update(bucket_data)
                self._bucket_store.free_bucket(old_bucket_key)
        # reset bucket caches - do not reset active item and key cache, they remain valid
        self._bucket_cache = deque(maxlen=self.cache_size)
        self._active_buckets = type(self._active_buckets)()
        # mark done
        self._updating_layout = False
        self._store_head()

    # bucket management
    def _bucket_key(self, key):
        """
        Create the bucket identifier for a given key

        :param key: key to the content in-memory
        :return: key to the bucket stored persistently
        :rtype: str
        """
        return self.bucket_key_fmt % (hashkey(key) % self._bucket_count)

    def _get_bucket(self, bucket_key):
        """
        Return the appropriate bucket from the store

        May return the cached bucket if available.

        :param bucket_key: key for the bucket
        :return: bucket for ``bucket_key``
        :rtype: :py:class:`~DictBucket`
        """
        try:
            bucket = self._active_buckets[bucket_key]
        except KeyError:
            try:
                bucket = self._bucket_store.fetch_bucket(bucket_key)
            except BucketNotFound:
                bucket = DictBucket()
        self._active_buckets[bucket_key] = bucket
        self._bucket_cache.appendleft(bucket)
        return bucket

    def _store_bucket(self, bucket_key, bucket):
        """
        Flush a bucket to the store

        :param bucket_key: key for the entire bucket
        :param bucket: the bucket to store
        """
        if bucket:
            self._bucket_store.store_bucket(bucket_key, bucket=bucket)
        # free empty buckets
        else:
            self._bucket_store.free_bucket(bucket_key)

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
        for bucket_key, bucket in self._active_buckets.items():
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
        self._store_bucket(bucket_key, bucket)
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
        # try cached
        if self._keys_cache is not None:
            return len(self._keys_cache)
        # count each bucket, see 'keys' for iteration scheme
        read_buckets, length = set(), 0
        # start with the buckets we have in memory
        for bucket_key, bucket in self._active_buckets.items():
            length += len(bucket)
            read_buckets.add(bucket_key)
        # pull in remaining buckets
        for bucket_key in self._bucket_store.bucket_keys:
            if bucket_key not in read_buckets:
                length += len(self._get_bucket(bucket_key))
                read_buckets.add(bucket_key)
        return length

    def __bool__(self):
        # can only have items if we have buckets
        return bool(self._bucket_store.bucket_keys)

    __nonzero__ = __bool__

    def __eq__(self, other):
        # other is pdict, try some fast comparisons
        if isinstance(other, PersistentDict):
            # we are the same store
            if (
                self._bucket_store == other._bucket_store and
                self.bucket_count == other.bucket_count and
                self.bucket_salt == other.bucket_salt
            ):
                return True
            # different keys, cannot be equal
            if self._keys_cache is not None and self._keys_cache != other._keys_cache:
                return False
        # not a mapping, cannot be equal
        elif not isinstance(other, abc.Mapping):
            return NotImplemented
        # no fast path resolved...
        # try a not-quite slow path
        if len(self) // self.bucket_count <= self.cache_size:  # we're probably in memory already, just rewrap content
            return self.copy() == other
        # len defines the number of unique keys. If other has *all* our
        # keys, it cannot also have others if its len is the same as ours
        if len(self) == len(other):
            try:
                return all(other[key] == value for key, value in self.items())
            except KeyError:
                return False
        return False

    def __ne__(self, other):
        return not self == other

    def __iter__(self):
        """:see: :py:meth:`~.PersistentDict.keys`"""
        read_buckets = set()
        # start with the buckets we have in memory
        # FIX: we need to actually freeze the buckets we have now, since the
        #      weakrefs may get collected during iterations.
        for bucket_key, bucket in list(self._active_buckets.items()):
            for item_key in bucket.keys():
                yield item_key
            read_buckets.add(bucket_key)
        # pull in all buckets
        try:
            for bucket_key in self._bucket_store.bucket_keys:
                if bucket_key not in read_buckets:
                    bucket = self._get_bucket(bucket_key)
                    for item_key in bucket.keys():
                        yield item_key
                    read_buckets.add(bucket_key)
        except RuntimeError:
            # NOTE: the store's bucket key set may change size
            #       during iteration if client code modifies
            #       the dict. We catch and reraise to show that
            #       it's a problem of modifying the dict.
            raise RuntimeError('%s changed size during iteration' % self.__class__.__name__)

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

    def popitem(self):
        """
        Remove and return an arbitrary (key, value) pair from the dictionary.

        popitem() is useful to destructively iterate over a dictionary, as
        often used in set algorithms. If the dictionary is empty, calling
        popitem() raises a KeyError.

        :raises KeyError: if no items exists and no default is given
        """
        try:
            key = next(iter(self))
        except StopIteration:
            raise KeyError
        else:
            return key, self.pop(key)

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
        for bucket_key in list(self._bucket_store.bucket_keys):
            self._bucket_store.free_bucket(bucket_key)
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
                        self._store_bucket(last_bucket_key, bucket)
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
                self._store_bucket(last_bucket_key, bucket)
        if other is not None:
            # mapping types
            if hasattr(other, "items"):  # dictionary
                updatebuckets(other.items())
            elif isinstance(other, abc.Mapping):
                updatebuckets((key, other[key]) for key in other)
            elif hasattr(other, "keys"):  # partial dictionary
                updatebuckets((key, other[key]) for key in other.keys())
            else:  # sequence
                updatebuckets(other)
        updatebuckets(kwargs.items())

    # iterations
    def keys(self):
        """
        :__doc__:

        If ``d.cache_keys == True``, the view provides keys without access to the
        persistent backend, but in arbitrary order. This is likely to jump between
        persistent buckets.

        If ``d.cache_keys == False``, iteration is aligned to buckets - this is only
        an implementation detail and may change in the future. If you need aligned
        iteration, use ``for key in d`` or directly access :py:meth:`.items`.

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        return PersistentDictKeysView(self)

    def items(self):
        """
        :__doc__:

        This iterates over all keys in a semi-deterministic way. First, all keys
        from buckets cached in memory are returned. Following this, keys from the
        remaining buckets are returned.

        :note: Due to aligning keys to buckets, this function does not benefit from
               ``d.cache_keys == True``.

        :note: Since the state of the mapping also depends on accesses, the strict
               guarantee for iteration sequence equivalence given by ``dict`` is
               not replicated. Thus, it cannot be assumed that
               ``d.items() == zip(d.values(), d.keys()) == [(v, k) for (k, v) in d]``
               holds true in any case.
        """
        return PersistentDictItemsView(self)

    def values(self):
        """
        :__doc__:

        :note: See the note on iterator equivalency for :py:meth:`~.PersistentDict.items`.
        """
        return PersistentDictValuesView(self)

    # high level operations
    def copy(self):
        """
        :__doc__:

        :note: This will return a ``dict``, not a :py:class:`~.PersistentDict`.
        """
        return dict(self.items())

    def __repr__(self):
        return "%s(bucket_store=%r, bucket_count=%r, cache_size=%r, cache_keys=%r, items={%s})" % (
            self.__class__.__name__,
            self._bucket_store,
            self.bucket_count,
            self.cache_size,
            self._keys_cache is not None,
            self.__repr_content(),
        )

    def __repr_content(self):  # pragma: no cover
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

# register at ABC
abc.MutableMapping.register(PersistentDict)


class PersistentDictView(abc.MappingView):  # pylint: disable=too-many-ancestors
    """View to a :py:class:`~.PersistentDict`"""
    short_name = 'pdict_keys'

    def __bool__(self):
        return bool(self._mapping)

    def __repr__(self):
        return '%s([%s])' % (self.short_name, ', '.join(str(item) for item in self))


class PersistentDictKeysView(PersistentDictView, abc.KeysView):  # pylint: disable=too-many-ancestors
    short_name = 'pdict_keys'

    def __iter__(self):
        return iter(self._mapping)

    def __contains__(self, item):
        return item in self._mapping


class PersistentDictValuesView(PersistentDictView, abc.ValuesView):  # pylint: disable=too-many-ancestors
    short_name = 'pdict_values'

    def __iter__(self):
        try:
            for item_key in self._mapping:
                yield self._mapping[item_key]
        except KeyError:  # pragma: no cover
            raise RuntimeError("dictionary changed size during iteration")

    def __contains__(self, item):
        return any(item == element for element in self)


class PersistentDictItemsView(PersistentDictView, abc.ItemsView):  # pylint: disable=too-many-ancestors
    short_name = 'pdict_items'

    def __iter__(self):
        try:
            for item_key in self._mapping:
                yield item_key, self._mapping[item_key]
        except KeyError:  # pragma: no cover
            raise RuntimeError("dictionary changed size during iteration")

    def __contains__(self, item):
        try:
            key, value = item
        except TypeError:  # not a tuple
            return False
        else:
            try:
                return value == self._mapping[key]
            except KeyError:
                return False
