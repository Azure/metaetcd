package clock

import "github.com/prometheus/client_golang/prometheus"

var (
	getMemberRevDepth = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "metaetcd_get_member_rev_depth",
			Help: "Depth of recursion when mapping meta cluster revision to a specific member cluster.",
		})

	clockReconstitutions = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metaetcd_clock_reconstitution",
			Help: "Total number of times the meta cluster's clock has been reconstituted from its members.",
		})
)

func init() {
	prometheus.MustRegister(getMemberRevDepth)
	prometheus.MustRegister(clockReconstitutions)
}
