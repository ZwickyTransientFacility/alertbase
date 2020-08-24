package main

import (
	"fmt"
	"strconv"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
)

func queryCandidate(db *alertdb.Database, candidateID uint64) error {
	alert, err := db.GetByCandidateID(candidateID)
	if err != nil {
		return err
	}
	printAlert(alert)
	return nil
}

func queryObject(db *alertdb.Database, object string) error {
	alerts, err := db.GetByObjectID(object)
	if err != nil {
		return err
	}
	printAlerts(alerts)
	return nil
}

func queryTimerange(db *alertdb.Database, start, end, format string) error {
	if format != "jd" {
		return fmt.Errorf("format not implemented: %q", format)
	}
	startFloat, err := strconv.ParseFloat(start, 64)
	if err != nil {
		return err
	}
	endFloat, err := strconv.ParseFloat(end, 64)
	if err != nil {
		return err
	}
	alerts, err := db.GetByTimerange(startFloat, endFloat)
	if err != nil {
		return err
	}
	printAlerts(alerts)
	return nil
}
