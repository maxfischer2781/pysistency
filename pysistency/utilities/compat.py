try:
    import cython as _cython
    if not _cython.compiled:
        raise ImportError
except ImportError:
    CYTHON_COMPILED = False  # noqa
else:
    CYTHON_COMPILED = True  # noqa