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

================ =========================================== ============
Python           Pysistency                                  Status
================ =========================================== ============
:py:class:`dict` :py:class:`pysistency.pdict.PersistentDict` Stable
================ =========================================== ============
:py:class:`list` :py:class:`pysistency.plist.PersistentList` Experimental
================ =========================================== ============
