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
)

func init() {
	prometheus.MustRegister(requestCount)
}
