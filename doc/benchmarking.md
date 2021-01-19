# benchmarking

When benchmarking, I'm interested in these characteristics:

- wall time to build the DB
- wall time to execute simple queries (cone search, lookup by source ID, lookup by time range)
- wall time to execute compound queries (combine the above)
- size of the DB on disk
- peak memory when executing queries

I'm interested in analyzing things along these dimensions:
- Number of days of data
- NESTED vs RING
- IndexDB implementation (LevelDB? Badger? Something else?)
- Running on laptop vs EC2, maybe for a few flavors of EC2 instance/volume
- Size of the query (eg bigger vs smaller cone search)
