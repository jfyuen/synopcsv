package weather

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Station as defined in Synop
type Station struct {
	ID        string
	Name      string
	Latitude  float64
	Longitude float64
	Altitude  float64
}

func fetchURL(url string) (r io.Reader, err error) {
	var client = &http.Client{
		Timeout: time.Second * 10,
	}
	response, err := client.Get(url)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() {
		errClose := response.Body.Close()
		if err == nil {
			err = errors.WithStack(errClose)
		}
	}()
	b, err := ioutil.ReadAll(response.Body)
	// stations, err = parseStationCSV(response.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return bytes.NewReader(b), nil
}

// FetchStationCSV retrieve station lists as csv from
// https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv
func FetchStationCSV() (r io.Reader, err error) {
	url := "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv"
	return fetchURL(url)
}

// ParseStationsCSV parses stations from a CSV file formated as "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv"
func ParseStationsCSV(in io.Reader) ([]Station, error) {
	csvVals, err := parseCSV(in)
	if err != nil {
		return nil, err
	}
	stations := make([]Station, 0)
	for _, row := range csvVals.rows {
		station := Station{ID: row["ID"], Name: row["Nom"]}
		station.Latitude, err = strconv.ParseFloat(row["Latitude"], 64)
		if err != nil {
			return nil, err
		}
		station.Longitude, err = strconv.ParseFloat(row["Longitude"], 64)
		if err != nil {
			return nil, err
		}
		station.Altitude, err = strconv.ParseFloat(row["Altitude"], 64)
		if err != nil {
			return nil, err
		}
		stations = append(stations, station)
	}

	return stations, nil
}
