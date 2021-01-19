package alertdb

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"sort"
	"time"

	"github.com/ZwickyTransientFacility/alertbase/schema"
)

type DBMeta struct {
	Earliest, Latest time.Time // Timestamps of first and last pieces of data.
	Days             DaySet    // Unique days of data
	NAlerts          int       // Total count of alerts stored
	NBytes           int       // Total number of bytes stored
}

func NewDBMeta() *DBMeta {
	return &DBMeta{
		Earliest: time.Unix(1<<63-62135596801, 999999999), // max time
		Latest:   time.Unix(0, 0),
		Days:     make(DaySet),
	}
}

func (m DBMeta) WriteTo(w io.Writer) error {
	buf, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (m *DBMeta) ReadFrom(r io.Reader) error {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, m)
}

func (m DBMeta) markTimestamps(a *schema.Alert) {
	if a.Candidate == nil {
		return
	}
	t := time.Unix(0, int64(jd2unix(a.Candidate.Jd)))
	if t.Before(m.Earliest) || m.Earliest.IsZero() {
		m.Earliest = t
	} else if t.After(m.Latest) {
		m.Latest = t
	}
	m.Days.Add(t)
}

type DaySet map[time.Time]struct{}

func (ds DaySet) Add(t time.Time) {
	t2 := t.UTC()
	ds[time.Date(t2.Year(), t2.Month(), t2.Day(), 0, 0, 0, 0, t2.Location())] = struct{}{}
}

func (ds DaySet) All() []time.Time {
	var all []time.Time
	for k, _ := range ds {
		all = append(all, k)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].Before(all[j])
	})
	return all
}

func (ds DaySet) MarshalJSON() ([]byte, error) {
	return json.Marshal(ds.All())
}

// jd2unix converts a Julian Date to a Unix Nanosecond Timestamp (doesn't
// attempt to handle leap seconds).
func jd2unix(jd float64) uint64 {
	return uint64((jd - 2440587.5) * 86400000000000)
}
