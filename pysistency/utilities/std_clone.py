"""
Tools for cloning standard library objects
"""


def inherit_docstrings(inherit_from):
    """
    Decorator for classes whose attributes/methods inherit docstrings

    For any *undocumented* attribute of the decorated class, try to copy
    the docstring from the source class' attribute. For any *documented*
    attribute of the decorated class, if the docstring includes the
    expression `:__doc__:`, this is replaced by the docstring from the
    source class' attribute.

    :param inherit_from:
    :return:
    """
    def inherit_docstrings_deco(cls):
        for name, attr in cls.__dict__.items():
            attr_doc = getattr(attr, '__doc__', None)
            # clone docstring
            if attr_doc is None:
                if hasattr(inherit_from, name):
                    try:
                        setattr(attr, '__doc__', getattr(
                            getattr(inherit_from, name),
                            '__doc__'
                        ))
                    except AttributeError:
                        pass
            # insert docstring - be pedantic as it's an explicit request
            elif ':__doc__:' in attr_doc:
                if not hasattr(inherit_from, name):
                    raise ValueError('Cannot inherit docstring %s.%s: No source attribute' % (cls.__qualname__, name))
                if not hasattr(getattr(inherit_from, name), '__doc__'):
                    raise ValueError('Cannot inherit docstring %s.%s: No source docstring' % (cls.__qualname__, name))
                # setattr will raise by itself if __doc__ cannot be changed
                setattr(
                    attr,
                    '__doc__',
                    attr_doc.replace(
                        ':__doc__:',
                        getattr(
                            getattr(inherit_from, name),
                            '__doc__'
                        )
                    )
                )
        return cls
    return inherit_docstrings_deco

