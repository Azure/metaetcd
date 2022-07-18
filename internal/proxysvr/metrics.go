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

	getMemberRevDepth = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "metaetcd_get_member_rev_depth",
			Help: "Depth of recursion when mapping meta cluster revision to a specific member cluster.",
		})

	activeWatchCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_active_watch_count",
			Help: "Number of active watch connections.",
		})

	watchLateStartCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metaetcd_watch_late_start_count",
			Help: "Number of total watch requests with revisions that are too late.",
		})
)

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(getMemberRevDepth)
	prometheus.MustRegister(activeWatchCount)
	prometheus.MustRegister(watchLateStartCount)
}
