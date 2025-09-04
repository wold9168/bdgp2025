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
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		engineRpm, _ := sds.GetLongByIndex(2)
		lubOilPressure, _ := sds.GetDoubleByIndex(3)
		fuelPressure, _ := sds.GetDoubleByIndex(4)
		coolantPressure, _ := sds.GetDoubleByIndex(5)
		lubOilTemp, _ := sds.GetDoubleByIndex(6)
		coolantTemp, _ := sds.GetDoubleByIndex(7)
		engineCondition, _ := sds.GetLongByIndex(8)

		results.EngineRpmSum += engineRpm
		results.LubOilPressureSum += lubOilPressure
		results.FuelPressureSum += fuelPressure
		results.CoolantPressureSum += coolantPressure
		results.LubOilTempSum += lubOilTemp
		results.CoolantTempSum += coolantTemp
		results.EngineConditionSum += engineCondition
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
