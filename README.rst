++++++++++
Pysistency
++++++++++

Python containers with persistency

|landscape| |travis| |codecov|

`pysistency` provides clones of standard containers backed by persistent data
storage. These containers allow to wrk with data too large for memory, and to
seamlessly keep data across program executions.

.. |landscape| image:: https://landscape.io/github/maxfischer2781/pysistency/master/landscape.svg?style=flat
   :target: https://landscape.io/github/maxfischer2781/pysistency/develop
   :alt: Code Health

.. |travis| image:: https://travis-ci.org/maxfischer2781/pysistency.svg?branch=develop
    :target: https://travis-ci.org/maxfischer2781/pysistency
    :alt: Test Health

.. |codecov| image:: https://codecov.io/gh/maxfischer2781/pysistency/branch/develop/graph/badge.svg
  :target: https://codecov.io/gh/maxfischer2781/pysistency
  :alt: Code Coverage

.. contents:: **Table of Contents**
    :depth: 2

Containers
==========

Efficiently implementing each container requires code tailored to its data
structure and usage. Each implementation is developed separately from others.

================ =========================================== ==============
Python           Pysistency                                   Status
================ =========================================== ==============
:py:class:`dict` :py:class:`pysistency.pdict.PersistentDict`  Stable
================ =========================================== ==============
:py:class:`list` :py:class:`pysistency.plist.PersistentList`  Experimental
================ =========================================== ==============

The following `Status` categories are used:

**Experimental**
    Public interfaces and data storage may change at any time. Functionality
    is lacking.

**Stable**
    Core functionality fully implemented, data storage will remain stable.
    Some functionality still missing.

**Complete**
    All functionality available. Internal implementation details may change.

**Done**
    All functionality available, internal implementation stable, passes all
    applicable, official tests.

Where's X?
==========

Priority for new/existing types depends mostly on ease of implementation and
need. If you require a specific type or interface, let me know.

Backends
========

The containers of `pysistency` rely on backends to store data. This allows
switching the storage backend for each container. Currently, there is only
one backend: a file-based backend using `pickle`.
