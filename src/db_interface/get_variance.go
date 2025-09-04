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

	// Iterate through the dataset and calculate sums
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		engineRpm, _ := sds.GetLongByIndex(2)
		lubOilPressure, _ := sds.GetDoubleByIndex(3)
		fuelPressure, _ := sds.GetDoubleByIndex(4)
		coolantPressure, _ := sds.GetDoubleByIndex(5)
		lubOilTemp, _ := sds.GetDoubleByIndex(6)
		coolantTemp, _ := sds.GetDoubleByIndex(7)
		engineCondition, _ := sds.GetLongByIndex(8)

		count++
		engineRpmSum += engineRpm
		engineRpmSquaredSum += engineRpm * engineRpm
		lubOilPressureSum += lubOilPressure
		lubOilPressureSquaredSum += lubOilPressure * lubOilPressure
		fuelPressureSum += fuelPressure
		fuelPressureSquaredSum += fuelPressure * fuelPressure
		coolantPressureSum += coolantPressure
		coolantPressureSquaredSum += coolantPressure * coolantPressure
		lubOilTempSum += lubOilTemp
		lubOilTempSquaredSum += lubOilTemp * lubOilTemp
		coolantTempSum += coolantTemp
		coolantTempSquaredSum += coolantTemp * coolantTemp
		engineConditionSum += engineCondition
		engineConditionSquaredSum += engineCondition * engineCondition
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

	// Iterate through the dataset and calculate variances based on averages
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		engineRpm, _ := sds.GetLongByIndex(2)
		lubOilPressure, _ := sds.GetDoubleByIndex(3)
		fuelPressure, _ := sds.GetDoubleByIndex(4)
		coolantPressure, _ := sds.GetDoubleByIndex(5)
		lubOilTemp, _ := sds.GetDoubleByIndex(6)
		coolantTemp, _ := sds.GetDoubleByIndex(7)
		engineCondition, _ := sds.GetLongByIndex(8)

		count++
		engineRpmVariance += math.Pow(float64(engineRpm)-float64(avg.EngineRpmAverage), 2)
		lubOilPressureVariance += math.Pow(lubOilPressure-avg.LubOilPressureAverage, 2)
		fuelPressureVariance += math.Pow(fuelPressure-avg.FuelPressureAverage, 2)
		coolantPressureVariance += math.Pow(coolantPressure-avg.CoolantPressureAverage, 2)
		lubOilTempVariance += math.Pow(lubOilTemp-avg.LubOilTempAverage, 2)
		coolantTempVariance += math.Pow(coolantTemp-avg.CoolantTempAverage, 2)
		engineConditionVariance += math.Pow(float64(engineCondition)-float64(avg.EngineConditionAverage), 2)
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
