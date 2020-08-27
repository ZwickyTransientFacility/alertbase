# Experimentation Log

This is a journal of experimentation results while working with this codebase.
It is a chronological journal, with newest results on the bottom; it is mostly
kept as a record, not as a human-readable document.

## 2020-08-27

### Running on EC2

I set up a m5.large EC2 instance in us-west-2 to see whether speed improves. It
does!

#### Key results

Using the same indexdb as below, S3 query speed drops to ~10-30ms.

Specifically, here's the response times in 10%ile buckets (units are seconds)

```
count         745
mean     0.031772
std      0.023132

0%       0.002300
10%      0.012900
20%      0.014800
30%      0.021020
40%      0.025760
50%      0.028700
60%      0.032040
70%      0.035900
80%      0.040800
90%      0.051660
max      0.425500
```

At this rate, we get ~40 alerts/sec. This still seems pretty slow! But it's
faster at least.

#### Log of how to do this

Created an amazon linux 2 ec2 instance by clicking through the console. Gave it
an IAM Role, `alertbase-dev`, which has full access to S3. Gave it an SSH Key
Pair and downloaded the keys.

Here's what I did:
```bash
# Build the program (I'm on linux so the compiled binary works)
go build ./cmd/alertbase-query

# Copy the program onto the host
scp -i ~/.ssh/awskeys/swnelson-dev.pem alertbase-query ec2-user@44.226.205.70:/home/ec2-user/alertbase-query

# Copy the database
rsync -r -v -e 'ssh -i  ~/.ssh/awskeys/swnelson-dev.pem' ./alerts.db ec2-user@44.226.205.70:/home/ec2-user/alerts.db

# SSH onto the host
ssh -i ~/.ssh/awskeys/swnelson-dev.pem ec2-user@44.226.205.70

# Run query to test that things work:
./alertbase-query -db ./alerts.db/alerts.db/ -candidate=1309477146315015022
# OUTPUT: alert id=1309477146315015022  jd=2459063.977  obj=ZTF18abmwrai  n_prev=54  mag=20.1318

# Install 'gnomon', a utility to measure timestamp deltas in log outpuit
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
. ~/.nvm/nvm.sh
nvm install node
npm install -g gnomon

# Run the "real" query, piping output to a file:
./alertbase-query -db ./alerts.db/alerts.db -time-start=2459063.9751 -time-end=2459063.9752 2>/dev/null | gnomon 2>&1 > speed.log

# Process the file to strip it down to a list of time deltas:
cat speed.log | head -n -2 | awk '{print $1}' | sed 's/s//g' > times.log

# Download the file
scp -i ~/.ssh/awskeys/swnelson-dev.pem ec2-user@44.226.205.70:/home/ec2-user/times.log .
```

### Thoughts

Parallelizing the requests seems important, but it's great to see how low the
RTT can get inside the region.

I'm really glad that the DB is genuinely portable!


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
