import unittest

from pysistency.utilities import constants


class TestConstant(unittest.TestCase):
    def test_init(self):
        # arguments are optional
        self.assertIsNotNone(constants.Default())
        self.assertIsNotNone(constants.Default(name='foo'))
        self.assertIsNotNone(constants.Default(representation='<default foo>'))
        self.assertIsNotNone(constants.Default(name='foo', representation='<default foo>'))

    def test_str(self):
        self.assertEqual('foo', str(constants.Default(name='foo')))
        self.assertEqual('<default foo>', str(constants.Default(representation='<default foo>')))
        self.assertEqual('foo', str(constants.Default(name='foo', representation='<default foo>')))
        self.assertRegex(
            str(constants.Default()),
            r'<.*\.%s object at 0x\w*>' % constants.Default.__name__,
        )

    def test_cmp(self):
        last_instance = constants.Default(name='qwertz')
        for kwargs in [
            {},
            {'name': 'foo'},
            {'representation': 'foo_repr'},
            {'name': 'bar', 'representation': 'bar_repr'},
        ]:
            instance = constants.Default(**kwargs)
            self.assertNotEqual(last_instance, instance)
            self.assertEqual(instance, instance)
            self.assertEqual(instance, constants.Default(**kwargs))
            last_instance = instance
