package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	TotalDials            uint64
	TotalConnects         uint64
	TotalAbandons         uint64
	TotalDNCHits          uint64
	TotalTimezoneBlocks   uint64
	TotalLeadsIngested    uint64
	ActiveDialingRoutines int64
}

var DefaultMetrics = NewMetrics()

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncDial() {
	atomic.AddUint64(&m.TotalDials, 1)
}

func (m *Metrics) IncConnect() {
	atomic.AddUint64(&m.TotalConnects, 1)
}

func (m *Metrics) IncAbandon() {
	atomic.AddUint64(&m.TotalAbandons, 1)
}

func (m *Metrics) IncDNCHit() {
	atomic.AddUint64(&m.TotalDNCHits, 1)
}

func (m *Metrics) IncTimezoneBlock() {
	atomic.AddUint64(&m.TotalTimezoneBlocks, 1)
}

func (m *Metrics) AddLeads(n int) {
	atomic.AddUint64(&m.TotalLeadsIngested, uint64(n))
}

func (m *Metrics) IncActiveDialer() {
	atomic.AddInt64(&m.ActiveDialingRoutines, 1)
}

func (m *Metrics) DecActiveDialer() {
	atomic.AddInt64(&m.ActiveDialingRoutines, -1)
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dials := atomic.LoadUint64(&m.TotalDials)
		connects := atomic.LoadUint64(&m.TotalConnects)
		abandons := atomic.LoadUint64(&m.TotalAbandons)
		dnc := atomic.LoadUint64(&m.TotalDNCHits)
		tz := atomic.LoadUint64(&m.TotalTimezoneBlocks)
		leads := atomic.LoadUint64(&m.TotalLeadsIngested)
		active := atomic.LoadInt64(&m.ActiveDialingRoutines)

		abandonRate := float64(0)
		if connects+abandons > 0 {
			abandonRate = float64(abandons) / float64(connects+abandons) * 100.0
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP dialpulse_total_dials Total phone call attempts initiated\n")
		fmt.Fprintf(w, "# TYPE dialpulse_total_dials counter\n")
		fmt.Fprintf(w, "dialpulse_total_dials %d\n", dials)

		fmt.Fprintf(w, "# HELP dialpulse_total_connects Total calls answered by lead and connected to agent\n")
		fmt.Fprintf(w, "# TYPE dialpulse_total_connects counter\n")
		fmt.Fprintf(w, "dialpulse_total_connects %d\n", connects)

		fmt.Fprintf(w, "# HELP dialpulse_total_abandons Total calls answered but dropped due to no agent available\n")
		fmt.Fprintf(w, "# TYPE dialpulse_total_abandons counter\n")
		fmt.Fprintf(w, "dialpulse_total_abandons %d\n", abandons)

		fmt.Fprintf(w, "# HELP dialpulse_abandon_rate_pct Percentage of answered calls abandoned\n")
		fmt.Fprintf(w, "# TYPE dialpulse_abandon_rate_pct gauge\n")
		fmt.Fprintf(w, "dialpulse_abandon_rate_pct %.2f\n", abandonRate)

		fmt.Fprintf(w, "# HELP dialpulse_dnc_hits Total calls blocked by Do-Not-Call Bloom filter\n")
		fmt.Fprintf(w, "# TYPE dialpulse_dnc_hits counter\n")
		fmt.Fprintf(w, "dialpulse_dnc_hits %d\n", dnc)

		fmt.Fprintf(w, "# HELP dialpulse_timezone_blocks Total calls blocked by legal curfew window\n")
		fmt.Fprintf(w, "# TYPE dialpulse_timezone_blocks counter\n")
		fmt.Fprintf(w, "dialpulse_timezone_blocks %d\n", tz)

		fmt.Fprintf(w, "# HELP dialpulse_total_leads_ingested Total leads ingested into campaign queues\n")
		fmt.Fprintf(w, "# TYPE dialpulse_total_leads_ingested counter\n")
		fmt.Fprintf(w, "dialpulse_total_leads_ingested %d\n", leads)

		fmt.Fprintf(w, "# HELP dialpulse_active_dialers Active background dialing workers\n")
		fmt.Fprintf(w, "# TYPE dialpulse_active_dialers gauge\n")
		fmt.Fprintf(w, "dialpulse_active_dialers %d\n", active)
	}
}
