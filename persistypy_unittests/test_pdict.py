import unittest
import random
import tempfile

import persistypy.pdict


class _DictTestcasesMixin(object):
    """Tests for dictionary like objects"""
    def setUp(self):
        self.test_objects = []
        self.key_values = []
        # numerical
        self.key_values += [
            (random.random() * 1024, random.random() * 1024) for _ in range(256)
            ]
        # string
        self.key_values += [
            (str(random.random() * 1024), str(random.random() * 1024)) for _ in range(256)
            ]
        # bytes
        self.key_values += [
            (str(random.random() * 1024).encode(), str(random.random() * 1024).encode()) for _ in range(256)
            ]

    def test_brackets(self):
        # set all
        for test_target in self.test_objects:
            for key, value in self.key_values:
                test_target[key] = value
                self.assertEqual(test_target[key], value)
        # get all
        for test_target in self.test_objects:
            for key, value in self.key_values:
                test_target[key] = value
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

    def test_dict_update(self):
        kv_dict = dict(self.key_values)
        # test empty
        for test_target in self.test_objects:
            self.assertEqual(len(test_target), 0)
        # test update
        for test_target in self.test_objects:
            test_target.update(kv_dict)
            self.assertEqual(len(test_target), len(self.key_values))


class TestPythonDict(unittest.TestCase, _DictTestcasesMixin):
    """Test for consistency with regular dict"""
    def setUp(self):
        _DictTestcasesMixin.setUp(self)
        self.test_objects = [{}]


class TestPersistentDict(unittest.TestCase, _DictTestcasesMixin):
    def setUp(self):
        _DictTestcasesMixin.setUp(self)
        self.persistent_paths = [tempfile.TemporaryDirectory()]
        self.test_objects = [
            persistypy.pdict.PersistentDict(
                store_uri=self.persistent_paths[0].name
            )
        ]


class TestPersistentDictSalted(unittest.TestCase, _DictTestcasesMixin):
    def setUp(self):
        _DictTestcasesMixin.setUp(self)
        salt_count = 8
        self.persistent_paths = [tempfile.TemporaryDirectory() for _ in range(salt_count)]
        self.test_objects = [
            persistypy.pdict.PersistentDict(
                store_uri=self.persistent_paths[idx].name,
                bucket_salt=random.randint(0, 1024*1024*1024)
            )
            for idx in range(salt_count)
        ]


class TestPersistentDictBucketCount(unittest.TestCase, _DictTestcasesMixin):
    def setUp(self):
        _DictTestcasesMixin.setUp(self)
        bucket_exponential = 10  # 1024 buckets
        self.persistent_paths = [tempfile.TemporaryDirectory() for _ in range(bucket_exponential)]
        self.test_objects = [
            persistypy.pdict.PersistentDict(
                store_uri=self.persistent_paths[idx].name,
                bucket_count=2**(idx+1)
            )
            for idx in range(bucket_exponential)
        ]


class TestPersistentDictCacheSize(unittest.TestCase, _DictTestcasesMixin):
    def setUp(self):
        _DictTestcasesMixin.setUp(self)
        cache_size = [32, 16, 8, 4, 2, 0]
        self.persistent_paths = [tempfile.TemporaryDirectory() for _ in range(len(cache_size))]
        self.test_objects = [
            persistypy.pdict.PersistentDict(
                store_uri=self.persistent_paths[idx].name,
                cache_size=cache_size[idx]
            )
            for idx in range(len(cache_size))
        ]


class TestPersistentDictCacheKeys(unittest.TestCase, _DictTestcasesMixin):
    def setUp(self):
        _DictTestcasesMixin.setUp(self)
        self.persistent_paths = [tempfile.TemporaryDirectory()]
        self.test_objects = [
            persistypy.pdict.PersistentDict(
                store_uri=self.persistent_paths[0].name,
                cache_keys=False
            )
        ]
