"""
Tools for cloning standard library objects
"""


def inherit_docstrings(inherit_from):
    """
    Decorator for classes whose attributes/methods inherit docstrings

    :param inherit_from:
    :return:
    """
    def inherit_docstrings_deco(cls):
        for name, attr in cls.__dict__.items():
            if getattr(attr, '__doc__', None) is None:
                if hasattr(inherit_from, name):
                    try:
                        setattr(attr, '__doc__', getattr(
                            getattr(inherit_from, name),
                            '__doc__'
                        ))
                    except AttributeError:
                        pass
        return cls
    return inherit_docstrings_deco

