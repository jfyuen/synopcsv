package weather

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const na = "mq"

// Measure as defined in https://donneespubliques.meteofrance.fr/client/document/doc_parametres_synop_168.pdf
// Names are in english for code mapping
// Code list is available at https://library.wmo.int/pmb_ged/wmo_306-v1_1-2012_fr.pdf
type Measure struct {
	StationID                           string    // numer_sta
	Date                                time.Time // date
	SeaPressure                         *int      // pmer, in Pa
	PressureVariation                   *int      // tend, in Pa
	BarometricTrend                     *int      // cod_tend, code 0200
	WindDirection                       *int      // dd, in degrees for 10 min
	WindSpeed                           *float64  // ff, in m/s for 10 min
	Temperature                         *float64  // t, in K
	DewPoint                            *float64  // td, in
	Humidity                            *int      // u, in %
	HorizontalVisibility                *float64  // vv, in m
	PresentTime                         *int      // ww, code 4677
	PastTime1                           *int      // w1, code 4561
	PastTime2                           *int      // w2, code 4561
	TotalNebulosity                     *float64  // n, in %
	LowerLevelCloudNebulosity           *int      // nbas, in octa
	LowerLevelCloudHeight               *int      // hbas, in m
	LowerLevelCloudType                 *int      // cl, code 0513
	MiddleLevelCloudType                *int      // cm, code 0515
	HigherLevelCloudType                *int      // ch, code 0509
	PressureStation                     *int      // pres, in Pa
	BarometricLevel                     *int      // niv_bar, in Pa
	Geopotential                        *int      // geop, in m2/s2
	PressureVariation24Hours            *int      // tend24, in Pa
	MinimalTemperatureOverLast12Hours   *float64  // tn12, in K
	MinimalTemperatureOverLast24Hours   *float64  // tn24, in K
	MaximalTemperatureOverLast12Hours   *float64  // tx12, in K
	MaximalTemperatureOverLast24Hours   *float64  // tx24, in K
	MinimalGroundTemperatureOver12Hours *float64  // tminsol, in K
	TwMeasureMethod                     *int      // sw, code 3855
	WetBulbTemperature                  *float64  // tw, in K
	Last10MinutesGust                   *float64  // raf10, in m/s
	GustOverPeriod                      *float64  // rafper, in m/s
	GustPeriod                          *float64  // per, min
	GroundState                         *int      // etat_sol, code 0901
	SnowHeight                          *float64  // ht_neige, in m
	FreshSnowHeight                     *float64  // ssfrai, in m
	FreshSnowPeriod                     *float64  // perssfrai, in 1/10 hour
	PrecipitationOverLastHour           *float64  // rr1, in mm
	PrecipitationOverLast3Hours         *float64  // rr3, in mm
	PrecipitationOverLast6Hours         *float64  // rr6, in mm
	PrecipitationOverLast12Hours        *float64  // rr12, in mm
	PrecipitationOverLast24Hours        *float64  // rr24, in mm
	SpecialPhenomenon1                  *string   // phenspe1, code 3778
	SpecialPhenomenon2                  *string   // phenspe2, code 3778
	SpecialPhenomenon3                  *string   // phenspe3, code 3778
	SpecialPhenomenon4                  *string   // phenspe4, code 3778
	LevelCloudNebulosity1               *int      // nnuage1, in octa
	LevelCloudNebulosity2               *int      // nnuage2, in octa
	LevelCloudNebulosity3               *int      // nnuage3, in octa
	LevelCloudNebulosity4               *int      // nnuage4, in octa
	LevelCloudType1                     *int      // ctype1, code 0500
	LevelCloudType2                     *int      // ctype2, code 0500
	LevelCloudType3                     *int      // ctype3, code 0500
	LevelCloudType4                     *int      // ctype4, code 0500
	LevelBaseHeight1                    *int      // hnuage1, in m
	LevelBaseHeight2                    *int      // hnuage2, in m
	LevelBaseHeight3                    *int      // hnuage3, in m
	LevelBaseHeight4                    *int      // hnuage4, in m

}

// FetchMeasureCSV retrieve past measures in CSV format
// https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.${DATE}.csv.gz
// with ${DATE} a string as YYYYMM
// or https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.${DATE}.csv
// with ${DATE} a string as YYYYMMDDHH
func FetchMeasureCSV(date string) (measures []Measure, err error) {
	baseURL := "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/"
	url := ""
	switch len(date) {
	case 6:
		url = baseURL + fmt.Sprintf("Archive/synop.%v.csv.gz", date)
	case 10:
		url = baseURL + fmt.Sprintf("synop.%v.csv", date)
	default:
		return nil, errors.Errorf("wrong date size: %v", date)
	}
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
	measures, err = parseMeasureCSV(response.Body)
	if err != nil {
		return nil, err
	}
	return measures, nil
}

func isCodeValid(v int, code string) bool {
	return true
	/* TODO: Find how to validate codes correctly
	switch code {
	case "0200":
		return v >= 0 && v <= 8
	case "4677":
		return v >= 0 && v <= 99
	case "4561":
		return v >= 0 && v <= 9
	case "0513": // What about "/" value?
		// TODO: in csv file, values are like 30, 34, ... why??
		// return true at the moment because this columns is not meaningful for me now
		return true
		//		return v >= 0 && v <= 9
	case "0515": // What about "/" value?
		// See above
		return true
		//		return v >= 0 && v <= 9
	case "0509": // What about "/" value?
		// See above
		return true
		//		return v >= 0 && v <= 9
	case "3855":
		return v >= 0 && v <= 7
	case "0901":
		// See above
		return true
		//		return v >= 0 && v <= 9
	case "3778": // Do not know how to check
		return true
	default:
		return false
	}*/
}

type parser struct {
	err error
}

func (p *parser) parseFloat(s string) *float64 {
	if p.err != nil || s == na {
		return nil
	}
	val, err := strconv.ParseFloat(s, 64)
	p.err = errors.WithStack(err)
	return &val
}

func (p *parser) parseInt(s string) *int {
	if p.err != nil || s == na {
		return nil
	}
	val, err := strconv.Atoi(s)
	p.err = errors.WithStack(err)
	return &val
}

func (p *parser) parseCode(s string, code string) *int {
	if p.err != nil || s == na {
		return nil
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		p.err = errors.Wrapf(err, "error reading %v for code %v", s, code)
		return nil
	}

	if !isCodeValid(val, code) {
		p.err = errors.Errorf("Invalid code: %v for %v", val, code)
		return nil
	}
	return &val
}

func (p *parser) parseDate(s string) time.Time {
	var t time.Time
	if p.err != nil {
		return t
	}
	t, err := time.Parse("20060102150405", s)
	if err != nil {
		p.err = errors.WithStack(err)
	}
	return t
}

func (p *parser) parseString(s string) *string {
	if p.err != nil || s == na {
		return nil
	}
	return &s
}

func parseMeasureCSV(in io.Reader) ([]Measure, error) {
	csvVals, err := parseCSV(in)
	if err != nil {
		return nil, err
	}

	measures := make([]Measure, 0)
	for _, row := range csvVals.rows {
		measure := Measure{StationID: row["numer_sta"]}

		p := parser{}
		measure.Date = p.parseDate(row["date"])
		measure.SeaPressure = p.parseInt(row["pmer"])
		measure.PressureVariation = p.parseInt(row["tend"])
		measure.BarometricTrend = p.parseCode(row["cod_tend"], "0200")
		measure.WindDirection = p.parseInt(row["dd"])
		measure.WindSpeed = p.parseFloat(row["ff"])
		measure.Temperature = p.parseFloat(row["t"])
		measure.DewPoint = p.parseFloat(row["td"])
		measure.Humidity = p.parseInt(row["u"])
		measure.HorizontalVisibility = p.parseFloat(row["vv"])
		measure.PresentTime = p.parseInt(row["ww"])
		measure.PastTime1 = p.parseInt(row["w1"])
		measure.PastTime2 = p.parseInt(row["w2"])
		measure.TotalNebulosity = p.parseFloat(row["n"])
		measure.LowerLevelCloudNebulosity = p.parseInt(row["nbas"])
		measure.LowerLevelCloudHeight = p.parseInt(row["hbas"])
		measure.LowerLevelCloudType = p.parseCode(row["cl"], "0513")
		measure.MiddleLevelCloudType = p.parseCode(row["cm"], "0515")
		measure.HigherLevelCloudType = p.parseCode(row["ch"], "0509")
		measure.PressureStation = p.parseInt(row["pres"])
		measure.BarometricLevel = p.parseInt(row["niv_bar"])
		measure.Geopotential = p.parseInt(row["geop"])
		measure.PressureVariation24Hours = p.parseInt(row["tend24"])
		measure.MinimalTemperatureOverLast12Hours = p.parseFloat(row["tn12"])
		measure.MinimalTemperatureOverLast24Hours = p.parseFloat(row["tn24"])
		measure.MaximalTemperatureOverLast12Hours = p.parseFloat(row["tx12"])
		measure.MaximalTemperatureOverLast24Hours = p.parseFloat(row["tx24"])

		measure.MinimalGroundTemperatureOver12Hours = p.parseFloat(row["tminsol"])
		measure.TwMeasureMethod = p.parseInt(row["sw"])
		measure.WetBulbTemperature = p.parseFloat(row["tw"])
		measure.Last10MinutesGust = p.parseFloat(row["raf10"])
		measure.GustOverPeriod = p.parseFloat(row["rafper"])
		measure.GustPeriod = p.parseFloat(row["per"])
		measure.GroundState = p.parseCode(row["etat_sol"], "0901")
		measure.SnowHeight = p.parseFloat(row["ht_neige"])
		measure.FreshSnowHeight = p.parseFloat(row["ssfrai"])
		measure.FreshSnowPeriod = p.parseFloat(row["perssfrai"])
		measure.PrecipitationOverLastHour = p.parseFloat(row["rr1"])
		measure.PrecipitationOverLast3Hours = p.parseFloat(row["rr3"])
		measure.PrecipitationOverLast6Hours = p.parseFloat(row["rr6"])
		measure.PrecipitationOverLast12Hours = p.parseFloat(row["rr12"])
		measure.PrecipitationOverLast24Hours = p.parseFloat(row["rr24"])
		measure.SpecialPhenomenon1 = p.parseString(row["phenspe1"])
		measure.SpecialPhenomenon2 = p.parseString(row["phenspe2"])
		measure.SpecialPhenomenon3 = p.parseString(row["phenspe3"])
		measure.SpecialPhenomenon4 = p.parseString(row["phenspe4"])
		measure.LevelCloudNebulosity1 = p.parseInt(row["nnuage1"])
		measure.LevelCloudNebulosity2 = p.parseInt(row["nnuage2"])
		measure.LevelCloudNebulosity3 = p.parseInt(row["nnuage3"])
		measure.LevelCloudNebulosity4 = p.parseInt(row["nnuage4"])
		measure.LevelCloudType1 = p.parseCode(row["ctype1"], "0500")
		measure.LevelCloudType2 = p.parseCode(row["ctype2"], "0500")
		measure.LevelCloudType3 = p.parseCode(row["ctype3"], "0500")
		measure.LevelCloudType4 = p.parseCode(row["ctype4"], "0500")
		measure.LevelBaseHeight1 = p.parseInt(row["hnuage1"])
		measure.LevelBaseHeight2 = p.parseInt(row["hnuage2"])
		measure.LevelBaseHeight3 = p.parseInt(row["hnuage3"])
		measure.LevelBaseHeight4 = p.parseInt(row["hnuage4"])

		if p.err != nil {
			return nil, p.err
		}
		measures = append(measures, measure)
	}

	return measures, nil
}
