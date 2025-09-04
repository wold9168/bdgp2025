package db_interface

import (
	"fmt"

	"github.com/apache/iotdb-client-go/v2/client"
)

// SumResults holds the calculated sums for all engine parameters
type SumResults struct {
	EngineRpmSum       int64
	LubOilPressureSum  float64
	FuelPressureSum    float64
	CoolantPressureSum float64
	LubOilTempSum      float64
	CoolantTempSum     float64
	EngineConditionSum int64
}

// getSums calculates and returns the sums of all engine parameters from the SessionDataSet
func GetSums(sds *client.SessionDataSet) (SumResults, int) {
	var results SumResults
	cnt := 0

	// Get column mapping to handle dynamic column order
	columnMapping := GetSimpleColumnMapping(sds)
	fmt.Print(columnMapping)
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		// Get values using column names instead of hardcoded indices
		// Use GetObjectByIndex to handle actual data types, not just metadata types

		if coolantPressureInfo, exists := columnMapping["coolant_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(coolantPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.CoolantPressureSum += floatValue
				} else if intValue, ok := value.(int64); ok {
					results.CoolantPressureSum += float64(intValue)
				}
			}
		}

		if lubOilPressureInfo, exists := columnMapping["lub_oil_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(lubOilPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.LubOilPressureSum += floatValue
				} else if intValue, ok := value.(int64); ok {
					results.LubOilPressureSum += float64(intValue)
				}
			}
		}

		if engineRpmInfo, exists := columnMapping["engine_rpm"]; exists {
			if value, err := sds.GetObjectByIndex(engineRpmInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.EngineRpmSum += int64(floatValue)
				} else if intValue, ok := value.(int64); ok {
					results.EngineRpmSum += intValue
				}
			}
		}

		// lub_oil_pressure already handled in debug section above

		if fuelPressureInfo, exists := columnMapping["fuel_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(fuelPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.FuelPressureSum += floatValue
				}
			}
		}

		// coolant_pressure already handled in debug section above

		if lubOilTempInfo, exists := columnMapping["luboil_temp"]; exists {
			if value, err := sds.GetObjectByIndex(lubOilTempInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.LubOilTempSum += floatValue
				}
			}
		}

		if coolantTempInfo, exists := columnMapping["coolant_temp"]; exists {
			if value, err := sds.GetObjectByIndex(coolantTempInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.CoolantTempSum += floatValue
				}
			}
		}

		if engineConditionInfo, exists := columnMapping["engine_condition"]; exists {
			if value, err := sds.GetObjectByIndex(engineConditionInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					results.EngineConditionSum += int64(floatValue)
				} else if intValue, ok := value.(int64); ok {
					results.EngineConditionSum += intValue
				}
			}
		}

		cnt++
	}

	return results, cnt
}

// printSums prints the calculated sums in a formatted table
func PrintSums(sds *client.SessionDataSet, results SumResults) {
	fmt.Println("Print Sums")
	// Print column headers
	for _, columnName := range sds.GetColumnNames() {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()
	const whitespace string = "\t\t"
	const placeholder string = "------"
	fmt.Printf("%s%s", placeholder, whitespace)
	fmt.Printf("%v%s", results.EngineRpmSum, whitespace)
	fmt.Printf("%v%s", results.LubOilPressureSum, whitespace)
	fmt.Printf("%v%s", results.FuelPressureSum, whitespace)
	fmt.Printf("%v%s", results.CoolantPressureSum, whitespace)
	fmt.Printf("%v%s", results.LubOilTempSum, whitespace)
	fmt.Printf("%v%s", results.CoolantTempSum, whitespace)
	fmt.Printf("%v%s", results.EngineConditionSum, whitespace)
	fmt.Println()
}
