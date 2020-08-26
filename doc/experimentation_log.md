# Experimentation Log

This is a journal of experimentation results while working with this codebase.
It is a chronological journal, with newest results on the bottom; it is mostly
kept as a record, not as a human-readable document.

## 2020-08-25

### Ingestion of 1 day of data

#### Key results

 - Ingested 1 day of data (205,422 alerts; 24GB of avro)
   - Took 3h19m (17/s, 0.5MB/s)
   - Created an indexdb which is 7.8MB
   - Time to create looks like it's dominated by S3 write latency

#### Discussion

Able to ingest 1 full day of daya in 3h19m on my laptop. There's no parallelism.
Time seems mostly limited by S3 write time, because each PutObject call requires
a separate HTTP request.

"1 day of data" here means the public tarball from 2020-08-04. This tarball had
205,422 alerts. I downloaded the tarball, unpacked it to
`~/code/ztf/ztf_data_samples/2020-08-04`, and then ran:

```sh
cd ~/code/ztf/ztf_data_samples/2020-08-04
time alertbase-ingest ~/code/ztf/alertbase/alerts.db '*.avro'
```

This chugged away with no errors and produced the DB.

**Note that querying was not possible while the DB was being written; only one
process can access the DB at a time.**

### Query speed

Queries seem to be nearly instant on IndexDB for a 1-day archive. They're well
within the time limit. Eg:

#### Query by object ID: 0.25s
```
-> % time alertbase-query -object=ZTF18abmwrai                                                  <aws:ztf>
0: alert id=1309477146315015022  jd=2459063.977  obj=ZTF18abmwrai  n_prev=54  mag=20.1318
alertbase-query -object=ZTF18abmwrai  0.09s user 0.03s system 46% cpu 0.253 total
```

#### Query by candidate ID: 0.2s
```
-> % time alertbase-query -candidate=1309477146315015022                                        <aws:ztf>
alert id=1309477146315015022  jd=2459063.977  obj=ZTF18abmwrai  n_prev=54  mag=20.1318
alertbase-query -candidate=1309477146315015022  0.06s user 0.03s system 48% cpu 0.184 total
```

#### Query by time range, getting all alerts for one exposure: 45s

This is the slow one. It's slow because it sequentially pulls each S3 object;
the indexDB returns almost instantly with all the right URLs. Can we do better?

```
-> % time alertbase-query -db alerts.db -time-start=2459063.9751 -time-end=2459063.9752 2>/dev/null
alert id=1309475180015010010  jd=2459063.975  obj=ZTF18abmohdb  n_prev=56  mag=20.0321
alert id=1309475180015010011  jd=2459063.975  obj=ZTF18acyoewk  n_prev=58  mag=20.0321
alert id=1309475180015010033  jd=2459063.975  obj=ZTF19aabgitk  n_prev=55  mag=20.0321
alert id=1309475180015015002  jd=2459063.975  obj=ZTF18abncmjy  n_prev=56  mag=20.0321
alert id=1309475180015015005  jd=2459063.975  obj=ZTF19abctuff  n_prev=56  mag=20.0321
[.....]
alert id=1309475186315015018  jd=2459063.975  obj=ZTF20abqbtea  n_prev=110  mag=20.1510
alert id=1309475186315015025  jd=2459063.975  obj=ZTF20abqbtdz  n_prev=110  mag=20.1510
alertbase-query -db alerts.db -time-start=2459063.9751 -time-end=2459063.9752  8.79s user 0.66s system 21% cpu 44.532 total
```

### Thoughts

#### Ingest performance

We could parallelize the writes to S3. This may hit rate limiting issues though;
need to investigate default quotas.

#### Range query performance
To make range queries more efficient, we could pack more data into a single S3
object. The 'pluck' operations might do range queries; the range information
could be stored inside the indexDBs.

We can only optimize for one packing. For example, we could pack all images from
the same exposure into one S3 object; but then looking for all observations of
one object requires a bunch of "random" reads of different exposures. If we pack
by objectID, then each time query will be random access.

We could redundantly store data, but that would be expensive.
