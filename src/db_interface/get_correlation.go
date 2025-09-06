package db_interface

import (
	"log"
	"math"

	"github.com/apache/iotdb-client-go/v2/client"
)

type CorrelationResult struct {
	PearsonCorrelation [][]float64
}

func GetCorrelationResult(session client.Session, deviceId string, timeout int64) (result CorrelationResult, errRnt error) {
	columnNames, columnTypes, errMetadata := FetchMetadata(session, deviceId, timeout)
	if errMetadata != nil {
		log.Fatal(errMetadata)
		return CorrelationResult{}, errMetadata
	}

	columnLength := int32(len(columnNames))
	n := columnLength - 1 // Exclude timestamp column

	result.PearsonCorrelation = make([][]float64, n)
	for i := range result.PearsonCorrelation {
		result.PearsonCorrelation[i] = make([]float64, n)
	}

	sql := "select * from " + deviceId
	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		defer ds.Close()

		// Initialize accumulators for each column pair
		sumX := make([]float64, n)
		sumY := make([]float64, n)
		sumXY := make([][]float64, n)
		sumX2 := make([]float64, n)
		sumY2 := make([]float64, n)
		count := 0

		for i := range sumXY {
			sumXY[i] = make([]float64, n)
		}

		// Process each row
		for next, err := ds.Next(); err == nil && next; next, err = ds.Next() {
			count++
			values := make([]float64, n)

			// Read values for all columns (excluding timestamp)
			for i := int32(1); i < columnLength; i++ {
				index := i + 1
				columnType := columnTypes[i]
				data, err := fetchDataByColumnType(ds, columnType, index)
				if err != nil {
					log.Fatal(err)
				}
				values[i-1] = data
			}

			// Update accumulators for all column pairs
			for i := 0; i < int(n); i++ {
				x := values[i]
				sumX[i] += x
				sumX2[i] += x * x

				for j := 0; j < int(n); j++ {
					if i == j {
						continue
					}
					y := values[j]
					sumXY[i][j] += x * y
				}
			}

			// Update sumY and sumY2 (same as sumX and sumX2 for symmetric matrix)
			for j := 0; j < int(n); j++ {
				y := values[j]
				sumY[j] += y
				sumY2[j] += y * y
			}
		}

		// Calculate Pearson correlation coefficients
		for i := 0; i < int(n); i++ {
			for j := 0; j < int(n); j++ {
				if i == j {
					result.PearsonCorrelation[i][j] = 1.0
					continue
				}

				numerator := float64(count)*sumXY[i][j] - sumX[i]*sumY[j]
				denomX := math.Sqrt(float64(count)*sumX2[i] - sumX[i]*sumX[i])
				denomY := math.Sqrt(float64(count)*sumY2[j] - sumY[j]*sumY[j])

				if denomX == 0 || denomY == 0 {
					result.PearsonCorrelation[i][j] = 0
				} else {
					result.PearsonCorrelation[i][j] = numerator / (denomX * denomY)
				}
			}
		}

	} else {
		log.Fatal(err)
		errRnt = err
	}
	return
}
