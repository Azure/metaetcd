package watch

import "github.com/prometheus/client_golang/prometheus"

var (
	currentWatchRev = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_current_watch_rev",
			Help: "The last meta cluster revision observed.",
		})

	watchGapTimeoutCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metaetcd_watch_gap_timeout_count",
			Help: "The number of watch event gaps that were never filled.",
		})

	watchLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "metaetcd_watch_latency_seconds",
			Help: "The time between a watch event being received and exposed.",
		})
)

func init() {
	prometheus.MustRegister(currentWatchRev)
	prometheus.MustRegister(watchGapTimeoutCount)
	prometheus.MustRegister(watchLatency)
}
