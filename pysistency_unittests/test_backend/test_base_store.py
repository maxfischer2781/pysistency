import unittest

from pysistency.backend import  base_store


class TestBaseBucketStore(unittest.TestCase):
    def test_store_uri(self):
        with self.assertRaises(ValueError):
            base_store.BaseBucketStore(store_uri='foo://bar')

    def test_from_uri(self):
        with self.assertRaises(ValueError):
            base_store.BaseBucketStore.from_uri(store_uri='foo://bar')
        with self.assertRaises(ValueError):
            base_store.BaseBucketStore.from_uri(store_uri='/bar')

