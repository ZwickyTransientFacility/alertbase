=====================
 Creating a Database
=====================

For most users, a database should already be provided, pre-made. But of course,
*someone* has to make the database. This is the guide for how to do that.

First, you'll need a bunch of data. You can get alert archive tarballs from the
`UW ZTF public archive <https://ztf.uw.edu/alerts/public/>`__. Or, if you're at
UW, you can access the tarballs directly from ``epyc`` at
``/epyc/data/ztf/alerts/public/``.

Next, you'll need AWS credentials to be able to upload alerts, putting them
`somewhere that boto can find them
<https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`__.
If you're creating a new database from scratch, you'll also need to create the
S3 bucket were alert blobs will be stored.

You'll also want to make a directory on disk where the index will be stored.

Use the :py:obj:`alertbase.Database.create` method to create a fresh new
database. Pass it the values you just made. Then, upload your tarfile by calling
the :py:meth:`upload_tarfile` method of :py:obj:`alertbase.Database`:

.. automethod:: alertbase.Database.upload_tarfile

This is an ``async`` method, so you call it in a slightly unusual way. This should do the trick:

.. code-block:: python

   import asyncio
   import alertbase
   import pathlib

   with alertbase.Database.create("us-west-2", "bucket-name", "./path/to/alertdb") as db:
       asyncio.run(db.upload_tarfile(pathlib.Path("./path/to/tar")))


You call this directly on the ``.tar.gz`` file without untarring or unzipping it.

Expect this to take a long time! A single tarfile can easily take over an hour.

If you want logging and debugging output, you can do:

.. code-block:: python

   import logging
   logging.basicConfig()
   logging.getLogger("alertbase").setLevel(logging.DEBUG)
