package view

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/s3cretagent/kubed/internal/enrich"
)

// generic shows any resource without a dedicated view: name, the Ready
// condition when the object has one, and age.
var generic = View{
	Columns: []string{"NAME", "READY", "AGE"},
	Row: func(_ *enrich.Enricher, o *unstructured.Unstructured) ([]string, Tone) {
		ready, tone := readyCond(o)
		return []string{o.GetName(), ready, age(o)}, tone
	},
	Detail: func(e *enrich.Enricher, o *unstructured.Unstructured) []string {
		return append([]string{argoLine(e, o)}, conditionDetail(e, o)...)
	},
	Time:  created,
	Needs: argoNeeds,
}
