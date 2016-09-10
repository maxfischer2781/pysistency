import os
import unittest
import tempfile
import pickle

from pysistency.backend import file_store


class TestFileBucketStore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store_uri = 'file://' + os.path.abspath(self.temp_dir.name).rstrip('/') + '/'

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_store_uri(self):
        # relative path
        rel_store_uri = 'file://' + os.path.relpath(self.temp_dir.name).rstrip('/') + '/'
        self.assertEqual(
            os.path.abspath(file_store.FileBucketStore(store_uri=rel_store_uri)._path),
            os.path.abspath(self.temp_dir.name)
        )
        # recognized parameter
        self.assertEqual(
            file_store.FileBucketStore(store_uri=self.store_uri + '?pickleprotocol=0')._pickle_protocol,
            0
        )
        # unrecognized parameter
        with self.assertRaises(ValueError):
            file_store.FileBucketStore(store_uri=self.store_uri + '?barfoo=0')
        # wrong parameter type
        with self.assertRaises(ValueError):
            file_store.FileBucketStore(store_uri=self.store_uri + ';barfoo=0')

    def test_bucket_store(self):
        bucket_store = file_store.FileBucketStore(store_uri=self.store_uri)
        # head just uses buckets, sufficent to test both
        with self.assertRaises(file_store.base_store.BucketNotFound):
            bucket_store.fetch_head()
        bucket_store.store_head(1)
        self.assertEqual(bucket_store.fetch_head(), 1)
        bucket_store.free_head()
        with self.assertRaises(file_store.base_store.BucketNotFound):
            bucket_store.free_head()

    def test_bucket_store_persists_buckets(self):
        def setup(bucket_store):
            bucket_store.store_bucket('foobar', 'foobar')

        def test(bucket_store):
            self.assertEqual(bucket_store.fetch_bucket('foobar'), 'foobar')
        self._bucket_store_persists(setup=setup, test=test)

    def test_bucket_store_persists_head(self):
        def setup(bucket_store):
            bucket_store.store_head(1)

        def test(bucket_store):
            self.assertEqual(bucket_store.fetch_head(), 1)
        self._bucket_store_persists(setup=setup, test=test)

    def test_bucket_store_persists_all(self):
        def setup(bucket_store):
            bucket_store.store_head(1)
            bucket_store.store_bucket('foobar', 'foobar')

        def test(bucket_store):
            self.assertEqual(bucket_store.fetch_head(), 1)
            self.assertEqual(bucket_store.fetch_bucket('foobar'), 'foobar')
        self._bucket_store_persists(setup=setup, test=test)

    def _bucket_store_persists(self, setup, test):
        bucket_store = file_store.FileBucketStore(store_uri=self.store_uri)
        setup(bucket_store)
        test(bucket_store)
        # fetch again
        del bucket_store
        bucket_store = file_store.FileBucketStore(store_uri=self.store_uri)
        test(bucket_store)
        del bucket_store
        # change pickle protocol gracefully
        bucket_store = file_store.FileBucketStore(store_uri=self.store_uri + '?pickleprotocol=0')
        test(bucket_store)
        del bucket_store
        bucket_store = file_store.FileBucketStore(store_uri=self.store_uri + '?pickleprotocol=%d' % pickle.HIGHEST_PROTOCOL)
        test(bucket_store)
        del bucket_store
        # explicitly set same protocol
        bucket_store = file_store.FileBucketStore(store_uri=self.store_uri + '?pickleprotocol=%d' % pickle.HIGHEST_PROTOCOL)
        test(bucket_store)
