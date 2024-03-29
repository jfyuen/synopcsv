package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"path"

	"io"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/jfyuen/synopcsv"
	"github.com/pkg/errors"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func downloadFile(path string, f func() (io.Reader, error)) error {
	if fileExists(path) {
		return nil
	}
	r, err := f()
	if err != nil {
		return errors.WithStack(err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	err = ioutil.WriteFile(path, buf.Bytes(), 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func fetchSingleMeasureCSV(at string, storePath string) ([]synopcsv.Measure, error) {
	_, err := time.Parse("2006010215", at)
	if err != nil {
		return nil, errors.Wrap(err, "invalid date")
	}

	filename := path.Join(storePath, at+".csv")
	err = downloadFile(filename, func() (io.Reader, error) { return synopcsv.FetchMeasureCSV(at) })

	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()
	return synopcsv.ParseMeasureCSV(f)
}

func fetchMultipleMeasureCSV(start string, end string, storePath string) ([]synopcsv.Measure, error) {
	fromDate, err := time.Parse("200601", start)
	if err != nil {
		return nil, errors.Wrap(err, "invalid start date")
	}
	toDate, err := time.Parse("200601", end)
	if err != nil {
		return nil, errors.Wrap(err, "invalid end date")
	}
	measures := make([]synopcsv.Measure, 0)
	for d := fromDate; d.Before(toDate); d = d.AddDate(0, 1, 0) {
		dateStr := d.Format("200601")
		filename := path.Join(storePath, dateStr+".csv")
		err := downloadFile(filename, func() (io.Reader, error) { return synopcsv.FetchMeasureCSV(dateStr) })

		f, err := os.Open(filename)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		defer f.Close()
		monthlyMeasures, err := synopcsv.ParseMeasureCSV(f)
		if err != nil {
			return nil, err
		}
		measures = append(measures, monthlyMeasures...)
	}
	return measures, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func readStations(p string) ([]synopcsv.Station, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()
	return synopcsv.ParseStationsCSV(f)
}

type flags struct {
	dbURL, dbName, user, passwd, from, to, at, downloadPath, seriesName string
}

func (f flags) check() {

	if f.at == "" && f.from == "" && f.to == "" {
		fmt.Fprintf(os.Stderr, "need to provide a date using -at or a range with -from -to\n")
		flag.PrintDefaults()

	}
	if f.at != "" && (f.from != "" || f.to != "") {
		fmt.Fprintf(os.Stderr, "-at option provided with incompatible -from or -to\n")
		flag.PrintDefaults()
	}
}

func newFlags() flags {
	f := flags{}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: fetches SYNOP station and meteo data from meteo france website\nSee https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32 for more info\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.StringVar(&f.dbURL, "url", "", "influxdb url")
	flag.StringVar(&f.dbName, "dbname", "", "influxdb name")
	flag.StringVar(&f.user, "user", "", "influxdb user")
	flag.StringVar(&f.passwd, "passwd", "", "influxdb password")
	flag.StringVar(&f.from, "from", "", "fetch meteo data from date (must also supply -to, incompatible with -at), , use YYYYMM")
	flag.StringVar(&f.to, "to", "", "fetch meteo data to date excluded (must also supply -from, incompatible with -at), use YYYYMM")
	flag.StringVar(&f.at, "at", "", "fetch meteo data at date (incompatible with -from/-to), use YYYYMMDDHH")
	flag.StringVar(&f.downloadPath, "path", ".", "where to store downloaded files (default to current directory)")
	flag.StringVar(&f.seriesName, "seriesName", "measurements", "series to store values in")
	flag.Parse()
	return f
}

func createPoint(m synopcsv.Measure, stationsMap map[string]synopcsv.Station, seriesName string) (*client.Point, error) {
	tags := map[string]string{
		"station_id": m.StationID,
	}
	station := stationsMap[m.StationID]
	fields := map[string]interface{}{
		"longitude": station.Longitude,
		"latitude":  station.Latitude,
		"altitude":  station.Altitude,
		// TODO: add more data
	}
	if m.Temperature != nil {
		fields["temperature"] = *m.Temperature
	}

	if m.Humidity != nil {
		fields["humidity"] = *m.Humidity
	}

	if m.WindSpeed != nil {
		fields["wind_speed"] = *m.WindSpeed
	}

	pt, err := client.NewPoint(
		seriesName,
		tags,
		fields,
		m.Date,
	)
	return pt, errors.WithStack(err)
}

func insertMeasuresInflux(measures []synopcsv.Measure, stationsMap map[string]synopcsv.Station, f flags) error {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     f.dbURL,
		Username: f.user,
		Password: f.passwd,
	})
	if err != nil {
		return errors.Wrap(err, "Error creating InfluxDB Client")
	}
	defer c.Close()

	bpconf := client.BatchPointsConfig{
		Database:  f.dbName,
		Precision: "s",
	}
	bp, _ := client.NewBatchPoints(bpconf)

	batchSize := 5000
	for _, m := range measures {
		pt, err := createPoint(m, stationsMap, f.seriesName)
		if err != nil {
			return errors.Wrap(err, "error creating point")
		}
		bp.AddPoint(pt)
		if len(bp.Points()) > batchSize {
			if err := c.Write(bp); err != nil {
				return errors.Wrap(err, "error batch writing")
			}
			bp, _ = client.NewBatchPoints(bpconf)
		}
	}

	return errors.Wrap(c.Write(bp), "error batch writing")
}

func main() {
	f := newFlags()
	f.check()

	stationFilename := path.Join(f.downloadPath, "stations.csv")
	err := downloadFile(stationFilename, synopcsv.FetchStationCSV)
	checkError(err)

	stations, err := readStations(stationFilename)
	checkError(err)
	stationsMap := make(map[string]synopcsv.Station)
	for _, s := range stations {
		stationsMap[s.ID] = s
	}

	var measures []synopcsv.Measure
	if f.at != "" {
		measures, err = fetchSingleMeasureCSV(f.at, f.downloadPath)
	} else {
		measures, err = fetchMultipleMeasureCSV(f.from, f.to, f.downloadPath)
	}
	checkError(err)

	if f.dbURL != "" {
		err := insertMeasuresInflux(measures, stationsMap, f)
		checkError(err)
	}
}
