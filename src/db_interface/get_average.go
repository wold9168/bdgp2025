package db_interface

import (
	"fmt"

	"github.com/apache/iotdb-client-go/v2/client"
)

type AverageResults struct {
	EngineRpmAverage       int64
	LubOilPressureAverage  float64
	FuelPressureAverage    float64
	CoolantPressureAverage float64
	LubOilTempAverage      float64
	CoolantTempAverage     float64
	EngineConditionAverage int64
}

func GetAverageFromSumResults(sumResults SumResults, cnt int) AverageResults {
	return AverageResults{
		EngineRpmAverage:       sumResults.EngineRpmSum / int64(cnt),
		LubOilPressureAverage:  sumResults.LubOilPressureSum / float64(cnt),
		FuelPressureAverage:    sumResults.FuelPressureSum / float64(cnt),
		CoolantPressureAverage: sumResults.CoolantPressureSum / float64(cnt),
		LubOilTempAverage:      sumResults.LubOilTempSum / float64(cnt),
		CoolantTempAverage:     sumResults.CoolantTempSum / float64(cnt),
		EngineConditionAverage: sumResults.EngineConditionSum / int64(cnt),
	}
}

func GetAverage(sds *client.SessionDataSet) AverageResults {
	return GetAverageFromSumResults(GetSums(sds))
}

// printSums prints the calculated sums in a formatted table
func PrintAverages(sds *client.SessionDataSet, results AverageResults) {
	fmt.Println("Print Averages")
	// Print column headers
	for _, columnName := range sds.GetColumnNames() {
		fmt.Printf("%s\t", columnName)
	}
	fmt.Println()
	const whitespace string = "\t\t"
	const placeholder string = "------"
	fmt.Printf("%s%s", placeholder, whitespace)
	fmt.Printf("%v%s", results.EngineRpmAverage, whitespace)
	fmt.Printf("%v%s", results.LubOilPressureAverage, whitespace)
	fmt.Printf("%v%s", results.FuelPressureAverage, whitespace)
	fmt.Printf("%v%s", results.CoolantPressureAverage, whitespace)
	fmt.Printf("%v%s", results.LubOilTempAverage, whitespace)
	fmt.Printf("%v%s", results.CoolantTempAverage, whitespace)
	fmt.Printf("%v%s", results.EngineConditionAverage, whitespace)
	fmt.Println()
}
