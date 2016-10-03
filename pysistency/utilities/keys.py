import os
import zlib
from datetime import datetime as datetime_type

#: maximum integer returned by :py:func:`~.hashkey`
HASHKEY_MAXINT = 0xffffffff
#: fixed length format for hash returned :py:func:`~.hashkey`
HASHKEY_HEXFMT = '%08x'


def hashkey(obj, salt=0):
    """
    Create a key suitable for use in hashmaps

    :param obj: object for which to create a key
    :type: str, bytes, object
    :param salt: an optional salt to add to the key value
    :type salt: int
    :return: numeric key to `obj`
    :rtype: int
    """
    if isinstance(obj, str):
        return zlib.adler32(obj.encode(), salt) & 0xffffffff
    elif isinstance(obj, bytes):
        return zlib.adler32(obj, salt) & 0xffffffff
    elif isinstance(obj, datetime_type):
        return zlib.adler32(str(obj).encode(), salt) & 0xffffffff
    return hash(obj) & 0xffffffff


def hashkey_fast(obj, salt=0):
    """Optimized version of :py:func:`~.hashkey`"""
    if obj.__class__ in hashkey_fast.types:
        if obj.__class__ is str:
            return zlib.adler32(obj.encode(), salt) & 0xffffffff
        elif obj.__class__ is bytes:
            return zlib.adler32(obj, salt) & 0xffffffff
        # must be datetime_type
        else:
            return zlib.adler32(str(obj).encode(), salt) & 0xffffffff
    return hash(obj) & 0xffffffff
hashkey_fast.types = {str, bytes, datetime_type}


def hashkey_fast_cython(obj, salt=0):
    """Optimized version of :py:func:`~.hashkey` for cython"""
    if obj.__class__ in HASHKEY_FAST_TYPES:
        if obj.__class__ is str:
            return zlib.adler32(obj.encode(), salt) & 0xffffffff
        elif obj.__class__ is bytes:
            return zlib.adler32(obj, salt) & 0xffffffff
        # must be datetime_type
        else:
            return zlib.adler32(str(obj).encode(), salt) & 0xffffffff
    return hash(obj) & 0xffffffff
HASHKEY_FAST_TYPES = {str, bytes, datetime_type}

if os.environ.get('__PYSISTENCY_FASTHASH__'):
    try:
        import cython as _cython
        if not _cython.compiled:
            raise ImportError
    except ImportError:
        hashkey = hashkey_fast  # noqa
    else:
        hashkey = hashkey_fast_cython  # noqa
