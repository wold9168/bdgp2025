package test

import (
	"testing"

	"bdgp2025/src/db_interface"
)

func TestCalculateVarianceFunctions(t *testing.T) {
	// Test CalculateVarianceInt64 with known values
	varianceInt := db_interface.CalculateVarianceInt64(6000, 14000000, 3)
	expectedIntVariance := 1000000.0
	if varianceInt != expectedIntVariance {
		t.Errorf("Expected CalculateVarianceInt64 result %f, got %f", expectedIntVariance, varianceInt)
	}

	// Test CalculateVarianceFloat64 with known values
	varianceFloat := db_interface.CalculateVarianceFloat64(180.0, 11000.0, 3)
	expectedFloatVariance := 100.0
	if varianceFloat != expectedFloatVariance {
		t.Errorf("Expected CalculateVarianceFloat64 result %f, got %f", expectedFloatVariance, varianceFloat)
	}
}

func TestGetStdDevFromVariance(t *testing.T) {
	variance := db_interface.VarianceResults{
		EngineRpmVariance:      100.0,
		LubOilPressureVariance: 25.0,
		FuelPressureVariance:   16.0,
	}

	results := db_interface.GetStdDevFromVariance(variance)

	// Standard deviation should be square root of variance
	if results.EngineRpmStdDev != 10.0 {
		t.Errorf("Expected EngineRpmStdDev 10.0, got %f", results.EngineRpmStdDev)
	}

	if results.LubOilPressureStdDev != 5.0 {
		t.Errorf("Expected LubOilPressureStdDev 5.0, got %f", results.LubOilPressureStdDev)
	}

	if results.FuelPressureStdDev != 4.0 {
		t.Errorf("Expected FuelPressureStdDev 4.0, got %f", results.FuelPressureStdDev)
	}
}

func TestStructInitialization(t *testing.T) {
	// Test that we can initialize the structs without issues
	var varianceResults db_interface.VarianceResults
	var stdDevResults db_interface.StdDevResults
	var averageResults db_interface.AverageResults

	// Just make sure they can be created
	_ = varianceResults
	_ = stdDevResults
	_ = averageResults

	t.Log("Successfully initialized all data structures")
}
