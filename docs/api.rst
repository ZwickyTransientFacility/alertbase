API Reference
=======================

.. py:module:: alertbase

.. py:class:: Database

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

   .. automethod:: open
   .. automethod:: create
   .. automethod:: close

   .. automethod:: get_by_candidate_id
   .. automethod:: get_by_object_id
   .. automethod:: get_by_object_id_async
   .. automethod:: get_by_time_range
   .. automethod:: get_by_time_range_async
   .. automethod:: get_by_cone_search
   .. automethod:: get_by_cone_search_async

   .. automethod:: write
   .. automethod:: write_many

   .. automethod:: __enter__
   .. automethod:: __exit__

.. autoclass:: AlertRecord
   :members:
