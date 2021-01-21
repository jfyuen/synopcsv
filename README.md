# Synopcsv

## Description
This package provides download and basic parsing for CSV files based on SYNOP code provided by Meteo France, as well as stations.
The base page is located at: https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32

**This is not a synop parser**, as the files care in csv format, but contain synop codes, so the page is a bit misleading.

## Build and test

This package has now moved to Go modules (so requires a Go version that supports them). It should work automatically.
- Build with `go build`
- Test with `go test`

## Command line

A command line utility is available to download and insert some data into an influx db database, this is a test and work in progress.
Example:
```bash
# cd cmd/insert && go run main.go -from 199601 -to 201706 -dbname ${INDLUX_DBNAME} -passwd ${INFLUX_PWD} -user ${INFLUX_USER} -url http://localhost:8086
```