package benchutil

type BenchmarkEnvironment struct {
	DataVolumeDays   int64
	DataVolumeAlerts int64
	DataVolumeBytes  int64

	Scheme    HEALPixOrderingScheme
	IndexDB   IndexDBImplementation
	Blobstore BlobstoreImplementation
	Env       ExecutionEnvironment

	Indexes []IndexDBType

	QueryType QueryType
	Query     QueryParameters
}

type HEALPixOrderingScheme int

const (
	NESTED HEALPixOrderingScheme = iota
	RING
)

type BlobstoreImplementation int

const (
	S3 BlobstoreImplementation = iota
)

type IndexDBImplementation int

const (
	LevelDB IndexDBImplementation = iota
	Badger
	BoltDB
)

type IndexDBType int

const (
	CandidateID IndexDBType = iota
	SourceID
	Timestamp
	HEALPix
)

var AllIndexTypes = []IndexDBType{CandidateID, SourceID, Timestamp, HEALPix}

type ExecutionEnvironment int

const (
	Laptop ExecutionEnvironment = iota
)

type QueryType int

const (
	TimeRange QueryType = iota
	ConeSearch
	AlertID
	Source
	Ingest
)

type QueryParameters struct {
	TimeStart, TimeEnd float64
	RA, Dec, R         float64
	CandidateID        string
	SourceID           uint64
}
