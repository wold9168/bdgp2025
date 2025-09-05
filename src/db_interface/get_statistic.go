package db_interface

import (
	"log"

	"github.com/apache/iotdb-client-go/v2/client"
)

type StatisticsResult struct {
	cnt               int
	sum               []float64
	average           []float64
	variance          []float64
	standardDeviation []float64
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
		cnt:               0,
		sum:               make([]float64, columnLength),
		average:           make([]float64, columnLength),
		variance:          make([]float64, columnLength),
		standardDeviation: make([]float64, columnLength),
	}
	sql := "select * from " + deviceId

	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		for next, err := ds.Next(); err == nil && next; next, err = ds.Next() {
			result.cnt++
			var i int32 = 1
			for ; i < columnLength; i++ {
				var index int32 = i + 1 //For Get***ByIndex(), index 1 is timestamp.
				columnType := columnTypes[i]
				data, err := fetchDataByColumnType(ds, columnType, index)
				if err != nil {
					log.Fatal(err)
				}
				result.sum[i] += data
				result.average[i] += data
			}
		}
		var i int32 = 1
		for ; i < columnLength; i++ {
			result.average[i] /= float64(result.cnt)
		}
	} else {
		log.Fatal(err)
		errRnt = err
	}
	return
}
