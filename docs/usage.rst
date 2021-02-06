=======
 Usage
=======

Once you have the :ref:`required credentials<Required Credentials>` and
:ref:`IndexDB<Getting an IndexDB>` installed, you're ready to go.

If your IndexDB is at ``./path/to/alerts.db``, you can open and use the database
like this:

.. code-block:: python

   import alertbase
   with alertbase.Database.open("./path/to/alerts.db") as db:
       alerts = db.get_by_object_id("ZTF28abmodkj")
   for a in alerts:
       print(a.raw_dict['candidate']['magpsf'])


Searching by ZTF Object ID
--------------------------

Use :py:meth:`alertbase.Database.get_by_object_id`:

.. code-block:: python

   import alertbase
   with alertbase.Database.open("./path/to/alerts.db") as db:
       alerts = db.get_by_object_id("ZTF28abmodkj")
   for a in alerts:
       print(a.raw_dict['candidate']['magpsf'])

Cone search
-----------

Use :py:meth:`alertbase.Database.get_by_cone_search`. You pass in a
:py:obj:`astropy.coordinate.SkyCoord` to define the center of the cone, and a
:py:obj:`astropy.coordinate.Angle` to define the radius, so you can use any
notation you like that is supported by ``astropy``:

You'll get all alerts that were in that region of the sky, according to your index.

.. code-block:: python

   import alertbase
   from astropy.coordinate import SkyCoord, Angle

   # Use decimal degrees:
   center = SkyCoord(ra="50.123", dec="21.22245", unit="deg")
   radius = Angle("10 arcsec")

   with alertbase.Database.open("./path/to/alerts.db") as db:
       alerts = db.get_cone_search(center, radius)
   for a in alerts:
       print(a.raw_dict['candidate']['magpsf'])

   # Get weird and nautical:
   center = SkyCoord(ra="3h 12m 13s", dec="4h 4m 12s", unit="deg")
   radius = Angle("1°2′3″")

   with alertbase.Database.open("./path/to/alerts.db") as db:
       alerts = db.get_by_cone_search(center, radius)
   for a in alerts:
       print(a.raw_dict['candidate']['magpsf'])

Searching by Time Range
-----------------------

Use :py:meth:`alertbase.Database.get_by_time_range`. You pass in
:py:obj:`astropy.time.Time` values for the start and end of your desired range.
You'll get all alerts that come from exposures within that time range.

.. code-block:: python

   import alertbase
   from astropy.time import Time

   start = Time("2021-01-12 20:00:00")
   end = Time("2021-01-12 20:15:00")

   with alertbase.Database.open("./path/to/alerts.db") as db:
       alerts = db.get_by_time_range(start, end)
   for a in alerts:
       print(a.raw_dict['candidate']['magpsf'])

Advanced: Using :py:mod:`asyncio`
-----------------------

If you're processing a lot of large queries, or just feeling particularly brave,
you can use the ``async`` APIs of :py:obj:`alertbase.Database`. Each ``get_``
method has a streaming counterpart which returns an asynchronous generator.

So, for example, if you wanted to stream a query's results into your terminal
for some reason, you could do something like this:

.. code-block:: python

   import alertbase
   from astropy.time import Time

   start = Time("2021-01-12")
   end = Time("2021-01-13")

   async def stream_alerts():
       with alertbase.Database.open("./path/to/alerts.db"):
           stream = db.get_by_time_range_async(start, end)
           async for alert in stream:
               print(alert.candidate_id)

   asyncio.run(stream_alerts())
