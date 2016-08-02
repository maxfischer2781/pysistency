import os
import unittest
import tempfile

from pysistency.backend import file_store


class TestFileBucketStore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store_uri = 'file://' + os.path.abspath(self.temp_dir.name).rstrip('/') + '/'

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_store_uri(self):
        # recognized parameter
        self.assertEqual(
            file_store.FileBucketStore(store_uri=self.store_uri + '?pickleprotocol=0')._pickle_protocol,
            0
        )
        # unrecognized parameter
        with self.assertRaises(ValueError):
            file_store.FileBucketStore(store_uri=self.store_uri + '?barfoo=0')

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
