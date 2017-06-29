package synopcsv

import (
	"encoding/csv"
	"io"
)

// CSVRow based on csv file header
type CSVRow map[string]string

// CSV is a simple CSV structure to hold tabular data
type CSV struct {
	headers []string
	rows    []CSVRow
}

func parseCSV(in io.Reader) (CSV, error) {
	r := csv.NewReader(in)
	r.Comma = ';'

	rows := make([]CSVRow, 0)
	csvVals := CSV{}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return csvVals, err
		}
		if len(csvVals.headers) == 0 {
			csvVals.headers = record
		} else {
			columns := make(map[string]string)
			for i, val := range record {
				h := csvVals.headers[i]
				columns[h] = val
			}
			rows = append(rows, columns)
		}
	}
	csvVals.rows = rows
	return csvVals, nil
}
