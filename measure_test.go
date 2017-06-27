package weather

import (
	"fmt"
	"testing"
)

func TestFetchMeasureCSVDayTime(t *testing.T) {
	measures, err := FetchMeasureCSV("2017062321")
	if err != nil {
		t.Fatal(fmt.Printf("%+v\n", err))
	}
	expected := 48
	if len(measures) != expected {
		t.Fatalf("Invalid number of measures: found %v, expected %v", len(measures), expected)
	}
}

func TestFetchMeasureCSVMonth(t *testing.T) {
	measures, err := FetchMeasureCSV("201705")
	if err != nil {
		t.Fatal(fmt.Printf("%+v\n", err))
	}
	expected := 14209
	if len(measures) != expected {
		t.Fatalf("Invalid number of measures: found %v, expected %v", len(measures), expected)
	}
}
