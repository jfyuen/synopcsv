package synopcsv

import (
	"fmt"
	"testing"
	"time"
)

func TestFetchMeasureCSVDayTime(t *testing.T) {
	today := time.Now()
	date := today.Format("20060102") + "00" // Load today at midnight for test data
	r, err := FetchMeasureCSV(date)
	if err != nil {
		t.Fatal(fmt.Printf("%+v\n", err))
	}
	measures, err := ParseMeasureCSV(r)
	if err != nil {
		t.Fatal(fmt.Printf("%+v\n", err))
	}
	// Measures count usually go from 46 to 62, usually more around 60
	expectedMin, expectedMax := 46, 62
	if expectedMin > len(measures) || len(measures) > expectedMax {
		t.Fatalf("Invalid number of measures: found %v, expected between %v and %v", len(measures), expectedMin, expectedMax)
	}
}

func TestFetchMeasureCSVMonth(t *testing.T) {
	r, err := FetchMeasureCSV("201705")
	if err != nil {
		t.Fatal(fmt.Printf("%+v\n", err))
	}
	measures, err := ParseMeasureCSV(r)
	if err != nil {
		t.Fatal(fmt.Printf("%+v\n", err))
	}
	expected := 14209
	if len(measures) != expected {
		t.Fatalf("Invalid number of measures: found %v, expected %v", len(measures), expected)
	}
}
