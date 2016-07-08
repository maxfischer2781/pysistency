class Default(object):
    """
    Class for default placeholders
    """
    def __init__(self, name=None, representation=None):
        self.name = name
        self.representation = representation

    def __str__(self):
        if self.name is not None:
            return self.name
        elif self.representation is not None:
            return self.representation
        return object.__str__(self)

    def __repr__(self):
        if self.representation is not None:
            return self.representation
        elif self.name is not None:
            return '<%s %r at 0x%x>' % (self.__class__.__name__, self.name, id(self))
        return object.__repr__(self)

    def __eq__(self, other):
        try:
            return self.name == other.name and self.representation == other.representation
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

NOTSET = Default(name='No Default')
