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
		// Initialize Earliest and Latest to plausible Max and Min values - but also
		// ones that will be happily marshaled by the encoding/json package, which
		// demands that years be in [0, 9999]. Some normalization for weird calendar
		// stuff occurs if you try to use the zero time, bringing it to the year -1,
		// so that one gets set to the year 1000.
		Earliest: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		Latest:   time.Date(1000, 00, 00, 00, 00, 00, 0, time.UTC),
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
	if t.Before(m.Earliest) {
		m.Earliest = t
	}
	if t.After(m.Latest) {
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

func (ds DaySet) UnmarshalJSON(v []byte) error {
	var times []time.Time
	err := json.Unmarshal(v, &times)
	if err != nil {
		return err
	}
	for _, t := range times {
		ds[t] = struct{}{}
	}
	return nil
}

// jd2unix converts a Julian Date to a Unix Nanosecond Timestamp (doesn't
// attempt to handle leap seconds).
func jd2unix(jd float64) uint64 {
	return uint64((jd - 2440587.5) * 86400000000000)
}
