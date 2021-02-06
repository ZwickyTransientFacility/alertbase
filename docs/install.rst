============
Installation
============

Welcome to alertbase! To get started, you'll need to install alertbase, get
credentials, and get an IndexDB.

Installing the Python Package
=============================

Some day™ you'll be able to install with ``pip``. For now, you'll need to clone
the dang thing. Like this:

.. code-block:: bash

   # Clone the repo:
   git clone https://github.com/ZwickyTransientFacility/alertbase
   cd alertbase

   # Install it:
   pip install .

That's it!

Required Credentials
====================

You'll need AWS credentials to access the alert data. If you've been talking to
Spencer, that probably means you need creds from him to the Heising-Simons ZTF
AWS account.

You'll then need to install them somewhere that libraries can find them. The
Boto library `has a guide
<https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`__
for that.

The credentials will need ``GetObject`` permissions on the bucket that's storing
data.

Getting an IndexDB
==================

Well, there's no good way to write this documentation yet. You can find a copy
on ``epyc``, if you know what that is - there's one at
``/astro/users/swnelson/src/alertbase/alerts.db.tar.gz``. Copy it wherever you like and untar it. For example:

.. code-block:: bash

   # Download it
   scp epyc.astro.washington.edu:/astro/users/swnelson/src/alertbase/alerts.db.tar.gz alerts.db.tar.gz

   # Unpack it
   tar -xzvf alerts.db.tar.gz

   # The database should be in ./alerts.db now
   cat alerts.db/meta.json
