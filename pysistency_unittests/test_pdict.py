import unittest
import random
import tempfile
import datetime
import time

import pysistency.pdict


class DictTestcases(unittest.TestCase):
    """Tests for dictionary like objects"""
    def setUp(self):
        self.persistent_paths = []
        self.test_objects = []
        self.key_values = []
        # numerical
        self.key_values += [
            ((idx + random.random()) * 1024, idx) for idx in range(256)
            ]
        # string
        self.key_values += [
            (str((idx + random.random()) * 1024), str(idx+256)) for idx in range(256)
            ]
        # bytes
        self.key_values += [
            (str((idx + random.random()) * 1024).encode(), str(idx+512).encode()) for idx in range(256)
            ]
        # datetime
        now = time.time()
        self.key_values += [
            (datetime.datetime.fromtimestamp(now + idx), datetime.datetime.fromtimestamp(now + idx)) for idx in range(256)
            ]
        self.no_key_value = (-999, -999)

    def tearDown(self):
        for temp_dir in self.persistent_paths:
            temp_dir.cleanup()

    def test_brackets(self):
        # set all
        for test_target in self.test_objects:
            for key, value in self.key_values:
                test_target[key] = value
                self.assertEqual(test_target[key], value)
        # get all
        for test_target in self.test_objects:
            for key, value in self.key_values:
                self.assertEqual(test_target[key], value)
        # del all
        for test_target in self.test_objects:
            for key, value in self.key_values:
                del test_target[key]
                with self.assertRaises(KeyError):
                    _ = test_target[key]

    def test_container_len(self):
        # test empty
        for test_target in self.test_objects:
            self.assertEqual(len(test_target), 0)
        # test filling
        for test_target in self.test_objects:
            for key, value in self.key_values:
                test_target[key] = value
                self.assertEqual(test_target[key], value)
            self.assertEqual(len(test_target), len(self.key_values))
        # test filled
        for test_target in self.test_objects:
            self.assertEqual(len(test_target), len(self.key_values))
        # test emptied
        for test_target in self.test_objects:
            test_target.clear()
            self.assertEqual(len(test_target), 0)

    def test_container_bool(self):
        # test empty
        for test_target in self.test_objects:
            self.assertEqual(bool(test_target), False)
        # test filling
        for test_target in self.test_objects:
            for key, value in self.key_values:
                test_target[key] = value
                self.assertEqual(test_target[key], value)
            self.assertEqual(bool(test_target), True)
        # test filled
        for test_target in self.test_objects:
            self.assertEqual(bool(test_target), True)
        # test emptied
        for test_target in self.test_objects:
            test_target.clear()
            self.assertEqual(bool(test_target), False)

    def test_container_in(self):
        # test empty
        for test_target in self.test_objects:
            for key, value in self.key_values:
                self.assertFalse(key in test_target)
        # test filling
        for test_target in self.test_objects:
            for key, value in self.key_values:
                test_target[key] = value
                self.assertTrue(key in test_target)
        # test filled
        for test_target in self.test_objects:
            for key, value in self.key_values:
                self.assertTrue(key in test_target)
        # test emptied
        for test_target in self.test_objects:
            test_target.clear()
            for key, value in self.key_values:
                self.assertFalse(key in test_target)

    def test_container_iteration(self):
        kv_dict = dict(self.key_values)
        kv_keys = kv_dict.keys()
        kv_values = kv_dict.values()
        kv_items = kv_dict.items()
        for test_target in self.test_objects:
            test_target.update(kv_dict)
        # implicit
        for test_target in self.test_objects:
            for key in test_target:
                self.assertTrue(key in test_target)
                self.assertTrue(key in kv_dict)
                self.assertEqual(test_target[key], kv_dict[key])
            self.assertFalse(self.no_key_value[0] in test_target)
        # views
        for test_target in self.test_objects:
            test_keys = test_target.keys()
            for key in test_keys:
                self.assertTrue(key in test_keys)
                self.assertTrue(key in kv_keys)
                self.assertEqual(test_target[key], kv_dict[key])
            self.assertFalse(self.no_key_value[0] in test_keys)
            self.assertFalse(self.no_key_value[0] in kv_keys)
        for test_target in self.test_objects:
            test_values = test_target.values()
            for value in test_values:
                self.assertTrue(value in test_values)
                self.assertTrue(value in kv_values)
            self.assertFalse(self.no_key_value[1] in test_values)
            self.assertFalse(self.no_key_value[1] in kv_values)
        for test_target in self.test_objects:
            test_items = test_target.items()
            for item in test_items:
                self.assertTrue(item in test_items)
                self.assertTrue(item in kv_items)
            self.assertFalse(self.no_key_value in test_items)
            self.assertFalse(self.no_key_value in kv_items)


    def test_dict_update(self):
        kv_dict = dict(self.key_values)
        # test empty
        for test_target in self.test_objects:
            self.assertEqual(len(test_target), 0)
        # test update
        for test_target in self.test_objects:
            test_target.update(kv_dict)
            self.assertEqual(len(test_target), len(self.key_values))

    def test_persist(self):
        kv_dict = dict(self.key_values)
        # test update
        for test_target in self.test_objects:
            test_target.update(kv_dict)
            self.assertEqual(len(test_target), len(self.key_values))
        # destroy and recreate
        for idx, test_target in enumerate(self.test_objects):
            if not isinstance(test_target, pysistency.pdict.PersistentDict):
                continue
            # clone
            kwargs = {
                'store_uri': test_target.store_uri,
                'bucket_count': test_target.bucket_count,
                'bucket_salt': test_target.bucket_salt,
                'cache_size': test_target.cache_size,
                'cache_keys': test_target.cache_keys,
            }
            test_target.flush()
            # release
            self.test_objects[idx] = None
            del test_target
            # recreate test target
            self.test_objects[idx] = pysistency.pdict.PersistentDict(**kwargs)
        # test content
        for test_target in self.test_objects:
            for key, value in kv_dict.items():
                self.assertEqual(test_target[key], value)


class TestPythonDict(DictTestcases):
    """Test for consistency with regular dict"""
    def setUp(self):
        DictTestcases.setUp(self)
        self.test_objects = [{}]


class TestPersistentDict(DictTestcases):
    def setUp(self):
        DictTestcases.setUp(self)
        self.persistent_paths = [tempfile.TemporaryDirectory()]
        self.test_objects = [
            pysistency.pdict.PersistentDict(
                store_uri=self.persistent_paths[0].name
            )
        ]


class TestPersistentDictSalted(DictTestcases):
    def setUp(self):
        DictTestcases.setUp(self)
        salt_count = 8
        self.persistent_paths = [tempfile.TemporaryDirectory() for _ in range(salt_count)]
        self.test_objects = [
            pysistency.pdict.PersistentDict(
                store_uri=self.persistent_paths[idx].name,
                bucket_salt=random.randint(0, 1024*1024*1024)
            )
            for idx in range(salt_count)
            ]


class TestPersistentDictBucketCount(DictTestcases):
    def setUp(self):
        DictTestcases.setUp(self)
        bucket_exponential = 10  # 1024 buckets
        self.persistent_paths = [tempfile.TemporaryDirectory() for _ in range(bucket_exponential)]
        self.test_objects = [
            pysistency.pdict.PersistentDict(
                store_uri=self.persistent_paths[idx].name,
                bucket_count=2**(idx+1)
            )
            for idx in range(bucket_exponential)
            ]


class TestPersistentDictCacheSize(DictTestcases):
    def setUp(self):
        DictTestcases.setUp(self)
        cache_size = [128, 5, 4, 0]
        self.persistent_paths = [tempfile.TemporaryDirectory() for _ in range(len(cache_size))]
        self.test_objects = [
            pysistency.pdict.PersistentDict(
                store_uri=self.persistent_paths[idx].name,
                cache_size=cache_size[idx]
            )
            for idx in range(len(cache_size))
            ]


class TestPersistentDictCacheKeys(DictTestcases):
    def setUp(self):
        DictTestcases.setUp(self)
        self.persistent_paths = [tempfile.TemporaryDirectory()]
        self.test_objects = [
            pysistency.pdict.PersistentDict(
                store_uri=self.persistent_paths[0].name,
                cache_keys=False
            )
        ]
