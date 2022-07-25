package util

import "github.com/prometheus/client_golang/prometheus"

var (
	timeBufferVisibleMax = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_time_buffer_visible_max",
			Help: "Max revision visible from the time buffer",
		})

	timeBufferLength = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "metaetcd_time_buffer_len",
			Help: "The length of the time buffer.",
		})

	timeBufferTimeoutCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metaetcd_time_buffer_timeouts_count",
			Help: "The number of buffer event gaps that were never filled.",
		})
)

func init() {
	prometheus.MustRegister(timeBufferVisibleMax)
	prometheus.MustRegister(timeBufferLength)
	prometheus.MustRegister(timeBufferTimeoutCount)
}
