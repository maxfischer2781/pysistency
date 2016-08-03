"""
Replication of official dict tests for pdict

:see: :py:mod:`test.test_dict`
"""
import tempfile
import random

from test import mapping_tests

import pysistency.pdict


def _pdict_uri(path):
    return path.rstrip('/') + '/'


class DefaultMappingTests(mapping_tests.BasicTestMappingProtocol):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {})

    def setUp(self):
        self.persistent_paths = []

        def type2test_mockup(*args, **kwargs):
            """Mockup "type" creating new persistent dict every time"""
            self.persistent_paths.append(tempfile.TemporaryDirectory())
            pdict = pysistency.pdict.PersistentDict(
                store_uri=_pdict_uri(self.persistent_paths[-1].name),
                *self.type2test_mockup_params[0],
                **self.type2test_mockup_params[1]
            )
            pdict.update(*args, **kwargs)
            return pdict
        self.type2test = type2test_mockup

    def tearDown(self):
        for temp_dir in self.persistent_paths:
            temp_dir.cleanup()


class SaltedMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'bucket_salt': random.randint(0, 1024*1024*1024)})


class SmallMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'bucket_count': 1})


class BigMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'bucket_count': 1024})


class NoCacheMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'cache_size': 0})


class SmallCacheMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'cache_size': 1})


class BigCacheMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'cache_size': 128})


class KeyCacheMappingTests(DefaultMappingTests):
    #: arguments for creating persistend dict before filling it
    type2test_mockup_params = ((), {'cache_keys': False})

