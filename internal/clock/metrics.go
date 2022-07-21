package clock

import "github.com/prometheus/client_golang/prometheus"

var (
	getMemberRevDepth = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "metaetcd_get_member_rev_depth",
			Help: "Depth of recursion when mapping meta cluster revision to a specific member cluster.",
		})
)

func init() {
	prometheus.MustRegister(getMemberRevDepth)
}
