import unittest

from pysistency.utilities import std_clone


class Base(object):
    def doc(self):
        """method_doc"""
        pass

    def nodoc(self):
        pass


class TestInheritDocstring(unittest.TestCase):
    def test_inherit(self):
        @std_clone.inherit_docstrings(inherit_from=Base)
        class Inherit(object):
            def doc(self):
                pass

            def nodoc(self):
                pass

            def nobase(self):
                pass
        # docstring equal to base
        self.assertEqual(Base.doc.__doc__, Inherit.doc.__doc__)
        self.assertEqual(Base.nodoc.__doc__, Inherit.nodoc.__doc__)
        self.assertEqual(None, Inherit.nodoc.__doc__)
        self.assertEqual(None, Inherit.nobase.__doc__)

    def test_inherit_augment(self):
        # explicit inheritance from missing docstring should always fail
        with self.assertRaises(ValueError):
            @std_clone.inherit_docstrings(inherit_from=Base)
            class Inherit(object):
                def nodoc(self):
                    """:__doc__:"""
        with self.assertRaises(ValueError):
            @std_clone.inherit_docstrings(inherit_from=Base)
            class Inherit(object):
                def nobase(self):
                    """:__doc__:"""
        @std_clone.inherit_docstrings(inherit_from=Base)
        class Inherit(object):
            def doc(self):
                """doc:__doc__:"""
        self.assertEqual(Inherit.doc.__doc__, 'doc' + Base.doc.__doc__)

