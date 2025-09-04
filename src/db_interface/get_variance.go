package db_interface

import (
	"fmt"
	"math"

	"github.com/apache/iotdb-client-go/v2/client"
)

// VarianceResults holds the calculated variances for all engine parameters
type VarianceResults struct {
	EngineRpmVariance       float64
	LubOilPressureVariance  float64
	FuelPressureVariance    float64
	CoolantPressureVariance float64
	LubOilTempVariance      float64
	CoolantTempVariance     float64
	EngineConditionVariance float64
}

// StdDevResults holds the calculated standard deviations for all engine parameters
type StdDevResults struct {
	EngineRpmStdDev       float64
	LubOilPressureStdDev  float64
	FuelPressureStdDev    float64
	CoolantPressureStdDev float64
	LubOilTempStdDev      float64
	CoolantTempStdDev     float64
	EngineConditionStdDev float64
}

// GetVariance calculates and returns the variance of all engine parameters from the SessionDataSet
func GetVariance(sds *client.SessionDataSet) VarianceResults {
	// Variables to store sums and squared sums
	var (
		count                     int
		engineRpmSum              int64
		engineRpmSquaredSum       int64
		lubOilPressureSum         float64
		lubOilPressureSquaredSum  float64
		fuelPressureSum           float64
		fuelPressureSquaredSum    float64
		coolantPressureSum        float64
		coolantPressureSquaredSum float64
		lubOilTempSum             float64
		lubOilTempSquaredSum      float64
		coolantTempSum            float64
		coolantTempSquaredSum     float64
		engineConditionSum        int64
		engineConditionSquaredSum int64
	)

	// Get column mapping to handle dynamic column order
	columnMapping := GetSimpleColumnMapping(sds)

	// Iterate through the dataset and calculate sums
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		// Get values using GetObjectByIndex to handle actual data types
		if engineRpmInfo, exists := columnMapping["engine_rpm"]; exists {
			if value, err := sds.GetObjectByIndex(engineRpmInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					intValue := int64(floatValue)
					engineRpmSum += intValue
					engineRpmSquaredSum += intValue * intValue
				} else if intValue, ok := value.(int64); ok {
					engineRpmSum += intValue
					engineRpmSquaredSum += intValue * intValue
				}
			}
		}

		if lubOilPressureInfo, exists := columnMapping["lub_oil_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(lubOilPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					lubOilPressureSum += floatValue
					lubOilPressureSquaredSum += floatValue * floatValue
				}
			}
		}

		if fuelPressureInfo, exists := columnMapping["fuel_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(fuelPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					fuelPressureSum += floatValue
					fuelPressureSquaredSum += floatValue * floatValue
				}
			}
		}

		if coolantPressureInfo, exists := columnMapping["coolant_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(coolantPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					coolantPressureSum += floatValue
					coolantPressureSquaredSum += floatValue * floatValue
				}
			}
		}

		if lubOilTempInfo, exists := columnMapping["luboil_temp"]; exists {
			if value, err := sds.GetObjectByIndex(lubOilTempInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					lubOilTempSum += floatValue
					lubOilTempSquaredSum += floatValue * floatValue
				}
			}
		}

		if coolantTempInfo, exists := columnMapping["coolant_temp"]; exists {
			if value, err := sds.GetObjectByIndex(coolantTempInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					coolantTempSum += floatValue
					coolantTempSquaredSum += floatValue * floatValue
				}
			}
		}

		if engineConditionInfo, exists := columnMapping["engine_condition"]; exists {
			if value, err := sds.GetObjectByIndex(engineConditionInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					intValue := int64(floatValue)
					engineConditionSum += intValue
					engineConditionSquaredSum += intValue * intValue
				} else if intValue, ok := value.(int64); ok {
					engineConditionSum += intValue
					engineConditionSquaredSum += intValue * intValue
				}
			}
		}

		count++
	}

	// Return empty results if count is insufficient
	if count <= 1 {
		return VarianceResults{}
	}

	// Calculate and return variances
	return VarianceResults{
		EngineRpmVariance:       CalculateVarianceInt64(engineRpmSum, engineRpmSquaredSum, count),
		LubOilPressureVariance:  CalculateVarianceFloat64(lubOilPressureSum, lubOilPressureSquaredSum, count),
		FuelPressureVariance:    CalculateVarianceFloat64(fuelPressureSum, fuelPressureSquaredSum, count),
		CoolantPressureVariance: CalculateVarianceFloat64(coolantPressureSum, coolantPressureSquaredSum, count),
		LubOilTempVariance:      CalculateVarianceFloat64(lubOilTempSum, lubOilTempSquaredSum, count),
		CoolantTempVariance:     CalculateVarianceFloat64(coolantTempSum, coolantTempSquaredSum, count),
		EngineConditionVariance: CalculateVarianceInt64(engineConditionSum, engineConditionSquaredSum, count),
	}
}

// GetVarianceFromAverage calculates variance using pre-calculated average values
func GetVarianceFromAverage(sds *client.SessionDataSet, avg AverageResults) VarianceResults {
	var (
		count                   int
		engineRpmVariance       float64
		lubOilPressureVariance  float64
		fuelPressureVariance    float64
		coolantPressureVariance float64
		lubOilTempVariance      float64
		coolantTempVariance     float64
		engineConditionVariance float64
	)

	// Get column mapping to handle dynamic column order
	columnMapping := GetSimpleColumnMapping(sds)

	// Iterate through the dataset and calculate variances based on averages
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		// Get values using GetObjectByIndex to handle actual data types
		if engineRpmInfo, exists := columnMapping["engine_rpm"]; exists {
			if value, err := sds.GetObjectByIndex(engineRpmInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					engineRpmVariance += math.Pow(float64(int64(floatValue))-float64(avg.EngineRpmAverage), 2)
				} else if intValue, ok := value.(int64); ok {
					engineRpmVariance += math.Pow(float64(intValue)-float64(avg.EngineRpmAverage), 2)
				}
			}
		}

		if lubOilPressureInfo, exists := columnMapping["lub_oil_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(lubOilPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					lubOilPressureVariance += math.Pow(floatValue-avg.LubOilPressureAverage, 2)
				}
			}
		}

		if fuelPressureInfo, exists := columnMapping["fuel_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(fuelPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					fuelPressureVariance += math.Pow(floatValue-avg.FuelPressureAverage, 2)
				}
			}
		}

		if coolantPressureInfo, exists := columnMapping["coolant_pressure"]; exists {
			if value, err := sds.GetObjectByIndex(coolantPressureInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					coolantPressureVariance += math.Pow(floatValue-avg.CoolantPressureAverage, 2)
				}
			}
		}

		if lubOilTempInfo, exists := columnMapping["luboil_temp"]; exists {
			if value, err := sds.GetObjectByIndex(lubOilTempInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					lubOilTempVariance += math.Pow(floatValue-avg.LubOilTempAverage, 2)
				}
			}
		}

		if coolantTempInfo, exists := columnMapping["coolant_temp"]; exists {
			if value, err := sds.GetObjectByIndex(coolantTempInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					coolantTempVariance += math.Pow(floatValue-avg.CoolantTempAverage, 2)
				}
			}
		}

		if engineConditionInfo, exists := columnMapping["engine_condition"]; exists {
			if value, err := sds.GetObjectByIndex(engineConditionInfo.Index); err == nil {
				if floatValue, ok := value.(float64); ok {
					engineConditionVariance += math.Pow(float64(int64(floatValue))-float64(avg.EngineConditionAverage), 2)
				} else if intValue, ok := value.(int64); ok {
					engineConditionVariance += math.Pow(float64(intValue)-float64(avg.EngineConditionAverage), 2)
				}
			}
		}

		count++
	}

	// Return empty results if count is insufficient
	if count <= 1 {
		return VarianceResults{}
	}

	// Normalize by (n-1)
	n := float64(count)
	engineRpmVariance /= n - 1
	lubOilPressureVariance /= n - 1
	fuelPressureVariance /= n - 1
	coolantPressureVariance /= n - 1
	lubOilTempVariance /= n - 1
	coolantTempVariance /= n - 1
	engineConditionVariance /= n - 1

	return VarianceResults{
		EngineRpmVariance:       engineRpmVariance,
		LubOilPressureVariance:  lubOilPressureVariance,
		FuelPressureVariance:    fuelPressureVariance,
		CoolantPressureVariance: coolantPressureVariance,
		LubOilTempVariance:      lubOilTempVariance,
		CoolantTempVariance:     coolantTempVariance,
		EngineConditionVariance: engineConditionVariance,
	}
}

// Helper functions for variance calculation
func CalculateVarianceInt64(sum, squaredSum int64, count int) float64 {
	if count <= 1 {
		return 0
	}
	n := float64(count)
	return (float64(squaredSum) - float64(sum)*float64(sum)/n) / (n - 1)
}

func CalculateVarianceFloat64(sum, squaredSum float64, count int) float64 {
	if count <= 1 {
		return 0
	}
	n := float64(count)
	return (squaredSum - sum*sum/n) / (n - 1)
}

// PrintVariances prints the calculated variances in a formatted table
func PrintVariances(sds *client.SessionDataSet, results VarianceResults) {
	// Print column headers
	fmt.Println("Print Variances")
	for _, columnName := range sds.GetColumnNames() {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()
	const whitespace string = "\t\t"
	const placeholder string = "------"
	fmt.Printf("%s%s", placeholder, whitespace)
	fmt.Printf("%.2f%s", results.EngineRpmVariance, whitespace)
	fmt.Printf("%.2f%s", results.LubOilPressureVariance, whitespace)
	fmt.Printf("%.2f%s", results.FuelPressureVariance, whitespace)
	fmt.Printf("%.2f%s", results.CoolantPressureVariance, whitespace)
	fmt.Printf("%.2f%s", results.LubOilTempVariance, whitespace)
	fmt.Printf("%.2f%s", results.CoolantTempVariance, whitespace)
	fmt.Printf("%.2f%s", results.EngineConditionVariance, whitespace)
	fmt.Println()
}

// GetStdDev calculates and returns the standard deviation of all engine parameters from the SessionDataSet
func GetStdDev(sds *client.SessionDataSet) StdDevResults {
	variance := GetVariance(sds)
	return StdDevResults{
		EngineRpmStdDev:       math.Sqrt(variance.EngineRpmVariance),
		LubOilPressureStdDev:  math.Sqrt(variance.LubOilPressureVariance),
		FuelPressureStdDev:    math.Sqrt(variance.FuelPressureVariance),
		CoolantPressureStdDev: math.Sqrt(variance.CoolantPressureVariance),
		LubOilTempStdDev:      math.Sqrt(variance.LubOilTempVariance),
		CoolantTempStdDev:     math.Sqrt(variance.CoolantTempVariance),
		EngineConditionStdDev: math.Sqrt(variance.EngineConditionVariance),
	}
}

// GetStdDevFromVariance calculates standard deviation from pre-calculated variance
func GetStdDevFromVariance(variance VarianceResults) StdDevResults {
	return StdDevResults{
		EngineRpmStdDev:       math.Sqrt(variance.EngineRpmVariance),
		LubOilPressureStdDev:  math.Sqrt(variance.LubOilPressureVariance),
		FuelPressureStdDev:    math.Sqrt(variance.FuelPressureVariance),
		CoolantPressureStdDev: math.Sqrt(variance.CoolantPressureVariance),
		LubOilTempStdDev:      math.Sqrt(variance.LubOilTempVariance),
		CoolantTempStdDev:     math.Sqrt(variance.CoolantTempVariance),
		EngineConditionStdDev: math.Sqrt(variance.EngineConditionVariance),
	}
}

// PrintStdDev prints the calculated standard deviations in a formatted table
func PrintStdDev(sds *client.SessionDataSet, results StdDevResults) {
	fmt.Println("Print Standard Deviations")
	// Print column headers
	for _, columnName := range sds.GetColumnNames() {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()
	const whitespace string = "\t\t"
	const placeholder string = "------"
	fmt.Printf("%s%s", placeholder, whitespace)
	fmt.Printf("%.2f%s", results.EngineRpmStdDev, whitespace)
	fmt.Printf("%.2f%s", results.LubOilPressureStdDev, whitespace)
	fmt.Printf("%.2f%s", results.FuelPressureStdDev, whitespace)
	fmt.Printf("%.2f%s", results.CoolantPressureStdDev, whitespace)
	fmt.Printf("%.2f%s", results.LubOilTempStdDev, whitespace)
	fmt.Printf("%.2f%s", results.CoolantTempStdDev, whitespace)
	fmt.Printf("%.2f%s", results.EngineConditionStdDev, whitespace)
	fmt.Println()
}
