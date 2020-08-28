# Alertbase Design

This document lays out the design rationale for `alertbase`. It doesn't get too
obsessive about the details of the implementation; rather, it's intended to
explain the important characteristics of the design, particularly when
contrasted against a simple SQL-backed database of alert packets.

## Summary of the design

`alertbase` has two components: An "index database" and a "blob store."

The blob store is a key-value database, holding big piles of bytes under
arbitrary keys. We use S3 for this.

The index database is a collection of indexes which provide references to blob
store keys. The database holds multiple indexes, each of which permits different
queries. For example, one index maps `object_id -> [list of blobstore keys for
alerts for that object]`. We use
[LevelDB](https://en.wikipedia.org/wiki/LevelDB) for the index databases.

Each day, we consume the latest tarball of public alert data from last night's
observing run. We iterate over every alert, storing it in S3 and adding to the
indexes.

This two-part design separates storage of ZTF alert packets, which are large
(~45KB typically), from the metadata used to query alerts, which is small (~20
bytes per alert). This has several advantages, which we'll go over now.

## Advantages of the two-part design

Each of this design's components scales well to handle hundreds of millions of
alert packets. This is an important requirement since ZTF produces about 200,000
alerts in a typical night of observing, which approaches ~100M alerts per year.

### S3 can handle many requests per second

S3 keys are POSIX-like paths, like
`"/alerts/v1/ZTF18aaoecoe/1309289752315010000"`. The "directory" component
(here, `/alerts/v1/ZTF18aaoecoe/` is called the "prefix," and it is the scaling
unit for S3.

Each unique prefix can handle up to 5,500 requests per second. This is plenty
for our purposes, as long as we choose good prefixes. In the current design,
each ZTF object gets a unique prefix.

### S3 can store many bytes cheaply

AWS does not impose limits on the size of data in a single S3 bucket. It should
be able to handle petabytes. The cost of storage is about $0.02/GB/month; this
can be tuned and optimized down to about $0.01/GB/month.

ZTF will generate about 4TB of alert data per year (100M alerts at 40KB each).
This yields a total storage cost of about $1,000 per year for 1 year of data.

### Index DB stays small

In a test based on one day of data, the index database required about 8MB to
index about 20GB of alert data (one night of data). At this rate, it grows to
about 3GB per year of data.

This is stored on disk, not in memory. That's small enough to be negligible.

### Index DB can be replicated for read-only usage

Since the index is built all at once by ingesting a night of data, it has a
generally read-only access pattern. The database itself is just a bunch of files
which can be copied onto multiple hosts. This has many advantages.

- We can provision multiple hosts to *horizontally serve request volume*. This
  characteristic is ideal for cloud computing - we can elastically scale up our
  index database cluster in response to demand surges when someone triggers a
  large workload, and then we can kill off instances when the workload dies
  down, all without any coordination.
- Read-only replication also lets us perform host maintenance with no downtime.
- It also makes it possible to isolate workloads - if someone wants to do a
  _lot_ of work, they can get a complete copy of the index database for
  themselves, so they don't clobber other users' performance.

### "Requester Pays" pricing controls network egress cost

The dominant cost when using S3 for bulk storage of this kind is likely to be
network egress. Data sent to the internet is priced at about $0.08/GB; data sent
to an AWS region is priced at $0.02/GB.

At 40KB/alert, this is equivalent to $1 per 312,500 alerts sent to the internet,
and $1 per 1.25M alerts sent to AWS. Each night of data is about 200,000 alerts,
so large scans of lots of data could hit these costs.

Those costs can be managed, though, with an S3 option called "requester pays"
billing. With this option enabled, requests for data can only be made by users
with an AWS account, and _they_ pay for the transfer fees. This protects the ZTF
project from excessive queries and from budget risk of providing open data
access, while still making data available.

### Deployment is simple

One particular advantage of the design which is hard to quantify is its
simplicity. A minimal go program which implements this architecture took 765
lines of Go code which builds into two self-contained tools, a
`alertbase-ingest` program which populates a database and an `alertbase-query`
program which queries it.

This small codebase should make maintenance and modification simpler.

## Risks and weak points

### S3's latency requires concurrency

When issuing a bulk query, like "get me all alerts produced in this time range,"
we have to issue a sequence of GET requests to S3. Each request is independent,
and each requires its own round-trip; S3 does not support HTTP pipelining or
http/2, so retrieving 1,000 alerts at 100ms each will take 100 seconds for
example.

The right approach here is to concurrently issue all 1,000 GET requests. This
needs to be implemented in the Go code for `alertbase-query`. It poses more risk
for pure Python clients that directly request data from S3 (like we'd want to
take advantage of 'Requester Pays' billing). Concurrency is not well-supported
in Python, so we might need a fairly sophisticated client library.

In practice, we see about 100-200ms round trip times from the internet to S3,
and about 2-4ms RTTs from AWS's `us-west-2` region to S3.

### No complex indexing

All indices need to be keyed with a single value, and there is no support for
joins. This means it's not possible to issue queries like "please provide all
alerts that are near this <ra, dec> pair, and which exceeeded 10 sigma."

This is a fundamental limitation of the choice of a separate index database. We
could perform some in-memory computations by querying multiple index DBs and
calculating the intersection of the result set, but this could be memory
intensive; at least one of the two result sets would need to be held in memory,
which could be gigabytes in size. This might be fine, but would be a substantial
increase in complexity of the Go client.

### Cone search seems hard

Cone searches are queries for all alerts within a particular region of the sky.
I don't know how we'll implement this with our index database system. We might
be able to do it with HEALPIX, but I'm not certain. If we must, we can store two
separate `ra` and `dec` indexes and compute an intersection of result sets
directly.

## Open questions

### LevelDB alternatives

I chose LevelDB because I'm familiar with it. I know that it's stable, efficient
at handling large datasets (as measured in number of key-value pairs), and
portable. I like that it requires no separate database process - it's entirely
implemented through files on disk.

But LevelDB is not the only option. We could use MongoDB, BoltDB, or Badger. Or,
we might find something different altogether.

LevelDB in particular has been designed to have relatively good write
performance, which motivated its backend design which uses log-structured merge
trees. We don't really care about write performance, since database construction
is done offline; our access patterns are generally read-only. Are there
write-once databases which would support this design better?

### Compression

Is there some way we can compress objects efficiently? We could just GZIP alert
packets before writing them into S3, but compression efficiency might not be
great. It would probably be better if we could share a compression dictionary
across all objects uploaded.

But this may be premature optimization; testing out naive compression is the
right next step, if saving on storage cost seems important.

### Cone search and complex indexes

As described above, these could use a lot of investigation. How can we
efficiently store sky positions in a key-value database?
