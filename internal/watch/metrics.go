package watch

import "github.com/prometheus/client_golang/prometheus"

var (
	staleWatchCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metaetcd_stale_watch_count",
			Help: "Number of stale watch connections.",
		})

	watchEventCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_watch_event_count",
			Help: "The total watch events that have been pushed into the buffer.",
		})

	watchesDialing = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_watches_dialing",
			Help: "The total member watch connections currently being established.",
		})

	watchesRunning = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_watches_running",
			Help: "The total member watch connections currently running.",
		})
)

func init() {
	prometheus.MustRegister(staleWatchCount)
	prometheus.MustRegister(watchEventCount)
	prometheus.MustRegister(watchesDialing)
	prometheus.MustRegister(watchesRunning)
}
