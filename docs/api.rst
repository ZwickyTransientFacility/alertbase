API Reference
=======================

.. py:module:: alertbase

Database
--------

:py:obj:`Database` is the main entrypoint for interacting with
:py:obj:`alertbase`. A :py:class:`Database` provides functions for adding
new data, as well as for retrieving it.

:py:class:`Database` hold some state, so you should generally use them in as
context managers, like this:

.. code-block:: python

   import alertbase
   with alertbase.Database.open("alerts.db") as db:
      for alert in db.get_by_object_id("ZTF19abf123g"):
          print(alert.candidate_id)

If you like, you can alternatively call :py:obj:`Database.close` directly:

.. code-block:: python

   import alertbase
   db = alertbase.Database.open("alerts.db")
   for alert in db.get_by_object_id("ZTF19abf123g"):
       print(alert.candidate_id)
   db.close()

If you don't remember to close a database, then database metadata might get
corrupted, and the LevelDB indexes might be left in a strange state.

.. py:class:: Database


   .. automethod:: open
   .. automethod:: create
   .. automethod:: close

   .. automethod:: get_by_candidate_id
   .. automethod:: get_by_object_id
   .. automethod:: get_by_object_id_stream
   .. automethod:: get_by_time_range
   .. automethod:: get_by_time_range_stream
   .. automethod:: get_by_cone_search
   .. automethod:: get_by_cone_search_stream

   .. automethod:: write
   .. automethod:: write_many

   .. automethod:: __enter__
   .. automethod:: __exit__

AlertRecord
-----------

  An AlertRecord is a wrapper around a ZTF Alert. It provides a few utility
  functions, but for the most part it is used as a type that's passed into and
  out of a :py:obj:`Database`.

.. autoclass:: AlertRecord
   :members:
