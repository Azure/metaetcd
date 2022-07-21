package watch

import "github.com/prometheus/client_golang/prometheus"

var (
	staleWatchCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metaetcd_stale_watch_count",
			Help: "Number of stale watch connections.",
		})
)

func init() {
	prometheus.MustRegister(staleWatchCount)
}
