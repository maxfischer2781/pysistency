import unittest
import random
import tempfile

import pysistency.plist


class ListTestcases(unittest.TestCase):
    def setUp(self):
        self.test_objects = []
        self.values = [(random.random() * 1024) for _ in range(256)]

    def test_append(self):
        # set all
        for test_target in self.test_objects:
            for idx, value in enumerate(self.values):
                test_target.append(value)
                self.assertEqual(test_target[idx], value)
        # get all
        for test_target in self.test_objects:
            for idx, value in enumerate(self.values):
                self.assertEqual(test_target[idx], value)

    def test_extend_all(self):
        # set all
        for test_target in self.test_objects:
            test_target.extend(self.values)
        # get all
        for test_target in self.test_objects:
            for idx, value in enumerate(self.values):
                self.assertEqual(test_target[idx], value)

    def test_extend_chunks(self):
        # set all
        for test_target in self.test_objects:
            base_idx = 0
            while base_idx < len(self.values):
                stride = random.randint(1, 64)
                test_target.extend(self.values[base_idx: base_idx+stride])
                base_idx += stride
        # get all
        for test_target in self.test_objects:
            for idx, value in enumerate(self.values):
                self.assertEqual(test_target[idx], value)

    def test_brackets(self):
        # create empty
        for test_target in self.test_objects:
            for _ in range(len(self.values)):
                test_target.append(None)
        # set all
        for test_target in self.test_objects:
            for idx, value in enumerate(self.values):
                test_target[idx] = value
                self.assertEqual(test_target[idx], value)
        # get all
        for test_target in self.test_objects:
            for idx, value in enumerate(self.values):
                self.assertEqual(test_target[idx], value)
        # del all
        for test_target in self.test_objects:
            for idx, value in enumerate(reversed(self.values)):
                idx = len(self.values) - idx - 1
                del test_target[idx]
                with self.assertRaises(IndexError):
                    _ = test_target[idx]


class TestPythonList(ListTestcases):
    """Test for consistency with regular dict"""
    def setUp(self):
        super().setUp()
        self.test_objects = [[]]


class TestPersistentList(ListTestcases):
    """Test for consistency with regular dict"""
    def setUp(self):
        super().setUp()
        self.persistent_paths = [tempfile.TemporaryDirectory()]
        self.test_objects = [
            pysistency.plist.PersistentList(
                store_uri=self.persistent_paths[0].name
            )
        ]

del ListTestcases
