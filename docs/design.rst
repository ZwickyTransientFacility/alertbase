========
 Design
========

Alertbase is designed as a two-level database. One level is the index. This is
relatively small (typically measured in megabytes, or single-digit gigabytes).
The other level is the bulk storage, also known as the "blobstore."

The index's purpose is to help a user filter the vast number of alerts down to a
small number that are of interest. For example, you can ask the index "what are
all the alerts within 10 arcsec of 54°18′12″, -22°30′2″?" Because the index is
stored locally, this computation can be done very quickly, giving back a list of
alert IDs.

The blobstore's purpose is to be a repository for getting the full alert
contents once you've filtered down to a specific set of alert IDs you're
interested in. The index necessarily only includes a small number of the
features in ZTF alert packets - just IDs, the timestamp of the observation, and
its position in the sky, currently. It does this to save space, because *every*
user has a copy of the index. The blobstore holds all the bulk data in a single
spot to avoid copying these terabytes of data.

IndexDB Design
-----------------------

IndexDB is implemented as a plain directory on disk, using :ref:`LevelDB<A bit
on LevelDB>` for the physical storage.

IndexDB holds four indexes:

 1. Candidate ID → Blobstore URL
 2. Object ID → List of Candidate IDs
 3. Observation Timestamp → List of Candidate IDs
 4. HEALPix Pixel → List of Candidate IDs

(note that "Candidate ID" is another name for the alert ID).

That means that it supports four attributes to filter on - candidate ID, object
ID, timestamp, and pixel. For the last three, querying takes two steps - first,
a list of candidate IDs is found, and then each is resolved into a blobstore
URL.

The HEALPix pixel index is included to support cone searches, which are queries
over a particular region of the sky. `HEALPix
<https://healpix.sourceforge.io/>`__ is a scheme for assigning integer pixel IDs
to regions of the sky. HEALPix has the convenient property that sequential pixel
IDs are usually close together on the sky.

A bit on LevelDB
^^^^^^^^^^^^^^^^
The index itself is implemented by using `LevelDB
<https://github.com/google/leveldb>`__. We keep a LevelDB database for each
attribute that is indexed. Each LevelDB database functions as an ordered
key-value map backed by pages on disk.

That means it supports:

 - :math:`O(lg(N))` disk reads for a lookup of random key-value pairs by key,
   with a very large branching factor - in practice, usually it's less than 10
   disk reads for any key.
 - :math:`O(1)` disk reads for lookups of the `next` key after a given key,
   which allows it to find all the keys in a particular range in :math:`O(M)`
   time, where :math:`M` is the number of keys which match.

Perhaps most unconventionally, LevelDB is a library which is embedded in a
program. It has no running server. This should be re-emphasized: **there is no
running index DB process**. Everything happens within the Python program.

The underlying data for the LevelDB is stored in a plain folder on disk. That
folder is entirley portable - it can be zipped up, emailed, unpacked, and used
directly without installing any extra software beyond Alertbase.

For more reading, `this High Scalability
article <http://highscalability.com/blog/2011/8/10/leveldb-fast-and-lightweight-keyvalue-database-from-the-auth.html>`__
on LevelDB is very good.

Data encoding
^^^^^^^^^^^^^

LevelDB only supports storing byte sequences for keys and values, and it sorts
the keys lexicographically. To support efficient range queries (like iterating
over a contiguous sequence of HEALPix pixels IDs in a cone search), it's
important then that we store keys in a way that sorts well lexicographically.

Furthermore, since LevelDB just stores bytes for values, we need our own way to
represent sequences of values, since a single pixel, object, or exposure ought
to point to many different candidate IDs.

Finally, space comes at a bit of a premium, and many of these values (like alert
IDs) come from a relatively small range of values.

These are the encodings that are used, therefore:

+------------------------------+----------------------------------------------------------------------------+
|                              |Encoding                                                                    |
|Logical Type                  |                                                                            |
+==============================+============================================================================+
|Candidate ID                  |`Zig-zag varint                                                             |
|                              |<https://en.wikipedia.org/wiki/Variable-length_quantity#Zigzag_encoding>`__ |
+------------------------------+----------------------------------------------------------------------------+
|Object ID                     |utf-8                                                                       |
+------------------------------+----------------------------------------------------------------------------+
|Timestamp                     |64-bit unsigned big-endian integer (UNIX epoch nanoseconds)                 |
+------------------------------+----------------------------------------------------------------------------+
|HEALPIX Pixel                 |64-bit unsigned big-endian integer                                          |
+------------------------------+----------------------------------------------------------------------------+
|List[Candidate ID]            |Concatenated zig-zag varints                                                |
+------------------------------+----------------------------------------------------------------------------+

These have the properties we want:

- Candidate IDs are never ranged against, only plucked out by exact matches, so
  we use a space-efficient representation.
- Object ID is a string, so we have to use a string codec; utf-8 is fine.
- Timestamps should be sorted, ideally in a monotonically fashion. UNIX epoch
  nanoseconds do this with high precision. Storing them as big-endian integers
  makes them sort lexicographically, just as LevelDB wants.
- HEALPix pixels should be sorted so that cone search _mostly_ hits consecutive
  LevelDB keys. Again, big-endian ints does what we need.
- Candidate ID lists are only used as values, so we don't need them to sort
  well. Varints use the most significant bit as a continuation flag, so we don't
  need to include any record separators - each separate integer value is obvious
  in the byte stream.

Blobstore Design
----------------

Compared to the index, the blob store is relatively simple. It just has one
task - it takes in URL references to alert payloads, and it fetches and
downloads them to serve them locally.

The current version of Alertbase uses S3 as its backing blobstore. All alerts
are placed in a bucket under the object naming scheme
``/alerts/v2/<OBJECT_ID>/<CANDIDATE_ID>``. But on retrieval, the blobstore will
try to go and get any URL you ask it to get, and will try to parse what it gets
as an encoded Avro file; this will permit gradual adaptation of the blobstore
backend without rebuilding or redistributing indexes.

Users might ask for lengthy lists of alerts to retrieve, like if they ask for a
particularly broad time range or large cone search in a dense region. In these
cases, sequentially downloading each alert can be quite slow. A round-trip time
to S3 of 50 milliseconds would be typical, but that will still take 10 seconds
just to download 200 alerts.

As a result, the blobstore is written to make many requests to S3 concurrently.
It does this with ``asyncio`` code that spins up many downloader tasks. Each
task has a bit of startup overhead, so the blobstore attempts to pick a
reasonable number of tasks given the size of the query - more if the query is
larger so the startup cost will be drowned out by the higher parallelism.

This use of ``asyncio`` can make the Blobstore tricky to work with, and it still
can be relatively slow. Alertbase's modular design permits replacing the S3
Blobstore with a more sophisticated backend in the future if this proves to be
too inefficient.
