package main

import (
	"fmt"

	"github.com/ZwickyTransientFacility/alertbase/schema"
)

func printAlert(a *schema.Alert) {
	fmt.Printf("alert id=%d  jd=%.3f  n_prev=%d  mag=%.4f\n",
		a.Candid, a.Candidate.Jd,
		len(a.Prv_candidates.ArrayPrv_candidate),
		a.Candidate.Diffmaglim.Float,
	)
}

func printAlerts(as []*schema.Alert) {
	for i, a := range as {
		fmt.Printf("%d: ", i)
		printAlert(a)
	}
}
