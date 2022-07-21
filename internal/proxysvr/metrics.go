package proxysvr

import "github.com/prometheus/client_golang/prometheus"

var (
	requestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "metaetcd_request_count",
			Help: "Number of total requests partitioned by method.",
		},
		[]string{"method"},
	)

	activeWatchCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_active_watch_count",
			Help: "Number of active watch connections.",
		})
)

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(activeWatchCount)
}
