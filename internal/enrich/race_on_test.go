//go:build race

package enrich

// raceOn: built with the race detector, which slows everything several
// times over; speed guards allow for it.
const raceOn = true
