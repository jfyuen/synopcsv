package weather

import (
	"testing"
)

func TestFetchStationCSV(t *testing.T) {
	stations, err := FetchStationCSV()
	if err != nil {
		t.Error(err)
	}
	stationCount := 62
	if len(stations) != stationCount {
		t.Errorf("Invalid number of stations: found %v vs %v expected", len(stations), stationCount)
	}

	last := Station{ID: "89642", Name: "DUMONT D'URVILLE", Latitude: -66.663167, Longitude: 140.001, Altitude: 43}
	if stations[len(stations)-1] != last {
		t.Errorf("last station do not match originale value: %v vs %v", last, stations[len(stations)-1])
	}
}
