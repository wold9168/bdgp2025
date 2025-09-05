package db_interface

import (
	"log"
	"math"

	"github.com/apache/iotdb-client-go/v2/client"
)

type StatisticsResult struct {
	Cnt               int
	Sum               []float64
	Average           []float64
	Variance          []float64
	StandardDeviation []float64
}

func FetchMetadata(session client.Session, deviceId string, timeout int64) (columnNames []string, columnTypes []string, errRnt error) {
	var sql string = "select * from " + deviceId
	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		columnNames = ds.GetColumnNames()
		columnTypes = ds.GetColumnTypes()
		ds.Close()
	} else {
		log.Fatal(err)
		errRnt = err
	}
	return
}
func fetchDataByColumnType(ds *client.SessionDataSet, columnType string, index int32) (float64, error) {
	var data float64
	if columnType == "DOUBLE" {
		dataRaw, errGet := ds.GetDoubleByIndex(index)
		if errGet != nil {
			return 0, errGet
		}
		data = dataRaw
	} else if columnType == "INT64" {
		dataRaw, errGet := ds.GetLongByIndex(index)
		if errGet != nil {
			return 0, errGet
		}
		data = float64(dataRaw)
	}
	return data, nil
}

func GetStatisticsResult(session client.Session, deviceId string, timeout int64) (result StatisticsResult, errRnt error) {
	columnNames, columnTypes, errMetadata := FetchMetadata(session, deviceId, timeout)
	if errMetadata != nil {
		log.Fatal(errMetadata)
		return StatisticsResult{}, errMetadata
	}

	columnLength := int32(len(columnNames))
	result = StatisticsResult{
		Cnt:               0,
		Sum:               make([]float64, columnLength),
		Average:           make([]float64, columnLength),
		Variance:          make([]float64, columnLength),
		StandardDeviation: make([]float64, columnLength),
	}
	sql := "select * from " + deviceId

	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		var welfordMean []float64 = make([]float64, columnLength)
		var welfordM2 []float64 = make([]float64, columnLength)
		for next, err := ds.Next(); err == nil && next; next, err = ds.Next() {
			result.Cnt++
			var i int32 = 1
			for ; i < columnLength; i++ {
				var index int32 = i + 1 //For Get***ByIndex(), index 1 is timestamp.
				columnType := columnTypes[i]
				data, err := fetchDataByColumnType(ds, columnType, index)
				if err != nil {
					log.Fatal(err)
				}
				result.Sum[i] += data
				result.Average[i] += data
				welfordDiff := data - welfordMean[i]
				welfordMean[i] = welfordDiff / float64(result.Cnt)
				welfordDiff2 := data - welfordMean[i]
				welfordM2[i] = welfordDiff2 * welfordDiff
			}
		}
		var i int32 = 1
		for ; i < columnLength; i++ {
			result.Average[i] /= float64(result.Cnt)
			result.Variance[i] = welfordM2[i] / float64(result.Cnt-1)
			result.StandardDeviation[i] = math.Sqrt(result.Variance[i])
		}
	} else {
		log.Fatal(err)
		errRnt = err
	}
	return
}
