package weather

import (
	"encoding/csv"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Station as defined in Synop
type Station struct {
	ID        string
	Name      string
	Latitude  float64
	Longitude float64
	Altitude  float64
}

// FetchStationCSV retrieve station lists as csv from
// https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv
func FetchStationCSV() (stations []Station, err error) {
	url := "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv"
	var client = &http.Client{
		Timeout: time.Second * 10,
	}
	response, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		errClose := response.Body.Close()
		if err == nil {
			err = errClose
		}
	}()
	stations, err = parseStationCSV(response.Body)
	if err != nil {
		return nil, err
	}
	return stations, nil
}

func parseStationCSV(in io.Reader) ([]Station, error) {
	r := csv.NewReader(in)
	r.Comma = ';'
	rows := make([]Station, 0)
	headers := true
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if headers {
			headers = false
			continue
		}
		station := Station{ID: record[0], Name: record[1]}
		station.Latitude, err = strconv.ParseFloat(record[2], 64)
		if err != nil {
			return nil, err
		}
		station.Longitude, err = strconv.ParseFloat(record[3], 64)
		if err != nil {
			return nil, err
		}
		station.Altitude, err = strconv.ParseFloat(record[4], 64)
		if err != nil {
			return nil, err
		}
		rows = append(rows, station)
	}
	return rows, nil
}
