

# pickle
try:
    import cPickle as pickle
except ImportError:
    import pickle

# range/xrange
try:
    rangex = xrange
except NameError:
    rangex = range

try:
    string_abc = basestring
except NameError:
    string_abc = (str, bytes)

__all__ = ['string_abc', 'pickle']
