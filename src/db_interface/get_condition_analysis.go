package db_interface

import (
	"log"
	"math"
	"sort"

	"github.com/apache/iotdb-client-go/v2/client"
)

type DetailedStatisticsResult struct {
	Cnt      int
	Sum      []float64
	Mean     []float64
	Variance []float64
	StdDev   []float64
	Min      []float64
	Max      []float64
	Median   []float64
	Q1       []float64 // 25%分位数
	Q3       []float64 // 75%分位数
	IQR      []float64 // 四分位距=Q3-Q1
	Skewness []float64 // 偏度
	Kurtosis []float64 // 峰度
}

type ConditionAnalysisResult struct {
	ConditionValues []int64
	Statistics      map[int64]DetailedStatisticsResult
}

func GetConditionAnalysisResult(session client.Session, deviceId string, timeout int64) (result ConditionAnalysisResult, errRnt error) {
	columnNames, columnTypes, errMetadata := FetchMetadata(session, deviceId, timeout)
	if errMetadata != nil {
		log.Fatal(errMetadata)
		return ConditionAnalysisResult{}, errMetadata
	}

	columnLength := int32(len(columnNames))

	// Find the engine_condition column index
	engineConditionIndex := int32(-1)
	for i, name := range columnNames {
		if name == deviceId+".engine_condition" {
			engineConditionIndex = int32(i)
			break
		}
	}

	if engineConditionIndex == -1 {
		log.Fatal("engine_condition column not found")
		return ConditionAnalysisResult{}, errMetadata
	}

	// Initialize map to store statistics for each condition value
	conditionStats := make(map[int64]*struct {
		Cnt    int
		Sum    []float64
		Mean   []float64
		M2     []float64 // For variance calculation
		M3     []float64 // For skewness calculation
		M4     []float64 // For kurtosis calculation
		Min    []float64
		Max    []float64
		Values [][]float64 // Store all values for median, quartiles, etc.
	})
	conditionValues := make([]int64, 0)

	sql := "select * from " + deviceId

	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		for next, err := ds.Next(); err == nil && next; next, err = ds.Next() {
			// Get engine condition value
			index := engineConditionIndex + 1 // For Get***ByIndex(), index 1 is timestamp
			conditionValueRaw, errGet := ds.GetLongByIndex(index)
			if errGet != nil {
				log.Fatal(errGet)
				continue
			}
			conditionValue := conditionValueRaw

			// Initialize statistics for this condition value if not already done
			if _, exists := conditionStats[conditionValue]; !exists {
				conditionStats[conditionValue] = &struct {
					Cnt    int
					Sum    []float64
					Mean   []float64
					M2     []float64
					M3     []float64
					M4     []float64
					Min    []float64
					Max    []float64
					Values [][]float64
				}{
					Cnt:    0,
					Sum:    make([]float64, columnLength),
					Mean:   make([]float64, columnLength),
					M2:     make([]float64, columnLength),
					M3:     make([]float64, columnLength),
					M4:     make([]float64, columnLength),
					Min:    make([]float64, columnLength),
					Max:    make([]float64, columnLength),
					Values: make([][]float64, columnLength),
				}
				conditionValues = append(conditionValues, conditionValue)

				// Initialize min/max with infinity values
				for i := int32(0); i < columnLength; i++ {
					conditionStats[conditionValue].Min[i] = math.Inf(1)
					conditionStats[conditionValue].Max[i] = math.Inf(-1)
				}
			}

			stats := conditionStats[conditionValue]
			stats.Cnt++

			// Process all columns (excluding timestamp)
			for i := int32(1); i < columnLength; i++ {
				index := i + 1 // For Get***ByIndex(), index 1 is timestamp
				columnType := columnTypes[i]
				data, err := fetchDataByColumnType(ds, columnType, index)
				if err != nil {
					log.Fatal(err)
				}

				// Store value for later calculations
				stats.Values[i] = append(stats.Values[i], data)

				// Update sum
				stats.Sum[i] += data

				// Update min/max
				if data < stats.Min[i] {
					stats.Min[i] = data
				}
				if data > stats.Max[i] {
					stats.Max[i] = data
				}

				// Welford's online algorithm for variance, skewness, and kurtosis
				delta := data - stats.Mean[i]
				deltaN := delta / float64(stats.Cnt)
				deltaN2 := deltaN * deltaN
				term1 := delta * deltaN * float64(stats.Cnt-1)
				// Use extended Welford Algorithm to calculate M2, M3 and M4
				// 使用扩展的 Welford 算法来计算高阶中心距
				stats.Mean[i] += deltaN
				stats.M4[i] += term1*deltaN2*float64(stats.Cnt*stats.Cnt-3*stats.Cnt+3) +
					6*deltaN2*stats.M2[i] - 4*deltaN*stats.M3[i]
				stats.M3[i] += term1*deltaN*float64(stats.Cnt-2) - 3*deltaN*stats.M2[i]
				stats.M2[i] += term1
			}
		}

		// Convert to final result format
		result.ConditionValues = conditionValues
		result.Statistics = make(map[int64]DetailedStatisticsResult)

		for conditionValue, stats := range conditionStats {
			finalStats := DetailedStatisticsResult{
				Cnt:      stats.Cnt,
				Sum:      make([]float64, columnLength),
				Mean:     make([]float64, columnLength),
				Variance: make([]float64, columnLength),
				StdDev:   make([]float64, columnLength),
				Min:      make([]float64, columnLength),
				Max:      make([]float64, columnLength),
				Median:   make([]float64, columnLength),
				Q1:       make([]float64, columnLength),
				Q3:       make([]float64, columnLength),
				IQR:      make([]float64, columnLength),
				Skewness: make([]float64, columnLength),
				Kurtosis: make([]float64, columnLength),
			}

			for i := int32(1); i < columnLength; i++ {
				finalStats.Sum[i] = stats.Sum[i]
				finalStats.Mean[i] = stats.Mean[i]
				finalStats.Min[i] = stats.Min[i]
				finalStats.Max[i] = stats.Max[i]

				// Calculate variance and standard deviation
				if stats.Cnt > 1 {
					finalStats.Variance[i] = stats.M2[i] / float64(stats.Cnt-1)
				} else {
					finalStats.Variance[i] = 0
				}
				finalStats.StdDev[i] = math.Sqrt(finalStats.Variance[i])

				// Calculate skewness and kurtosis
				if stats.Cnt > 2 && finalStats.Variance[i] != 0 {
					// Skewness: sqrt(n) * M3 / (M2^(3/2))
					// Kurtosis: n * M4 / M2^2 - 3
					n := float64(stats.Cnt)
					m2 := stats.M2[i]
					m3 := stats.M3[i]
					m4 := stats.M4[i]

					finalStats.Skewness[i] = (math.Sqrt(n) * m3) / math.Pow(m2, 1.5)
					finalStats.Kurtosis[i] = (n*m4)/(m2*m2) - 3.0
				} else {
					finalStats.Skewness[i] = 0
					finalStats.Kurtosis[i] = 0
				}

				// Calculate median and quartiles
				if len(stats.Values[i]) > 0 {
					// Sort the values
					sortedValues := make([]float64, len(stats.Values[i]))
					copy(sortedValues, stats.Values[i])
					sort.Float64s(sortedValues)

					// Calculate median
					n := len(sortedValues)
					if n%2 == 0 {
						finalStats.Median[i] = (sortedValues[n/2-1] + sortedValues[n/2]) / 2.0
					} else {
						finalStats.Median[i] = sortedValues[n/2]
					}

					// Calculate quartiles
					// TODO: This task cannot be streamed.
					// More efforts are needed to make it compatible with large-scale data that
					// cannot be loaded into memory, such as writing excess data back to disk instead
					// of keeping it in an array waiting to be counted.
					q1Index := n / 4
					q3Index := 3 * n / 4
					finalStats.Q1[i] = sortedValues[q1Index]
					finalStats.Q3[i] = sortedValues[q3Index]
					finalStats.IQR[i] = finalStats.Q3[i] - finalStats.Q1[i]
				}
			}

			result.Statistics[conditionValue] = finalStats
		}
	} else {
		log.Fatal(err)
		errRnt = err
		return
	}

	return
}
