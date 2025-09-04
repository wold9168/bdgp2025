package test

import (
	"testing"

	utils "bdgp2025/src/utils"
)

func TestReadCSVFile(t *testing.T) {
	// Test reading the engine_data.csv file
	records, err := utils.ReadCSVFileFirst5Rows("../data/engine_data.csv")
	if err != nil {
		t.Fatalf("Failed to read CSV file: %v", err)
	}

	// Check that we got exactly 5 records
	if len(records) != 5 {
		t.Errorf("Expected 5 records, got %d", len(records))
	}

	// Check that the first record has the expected values from the sample data
	expectedFirstRecord := utils.CSVRecord{
		EngineRPM:       int64(700),
		LubOilPressure:  float64(2.493591821),
		FuelPressure:    float64(11.79092738),
		CoolantPressure: float64(3.178980794),
		LubOilTemp:      float64(84.14416293),
		CoolantTemp:     float64(81.6321865),
		EngineCondition: int64(1),
	}

	firstRecord := records[0]
	if firstRecord != expectedFirstRecord {
		t.Errorf("First record does not match expected values.\nGot: %+v\nExpected: %+v", firstRecord, expectedFirstRecord)
	}
}

func TestPrintCSVRecords(t *testing.T) {
	// This is a simple test to ensure the function doesn't panic
	records := []utils.CSVRecord{
		{
			EngineRPM:       int64(700),
			LubOilPressure:  float64(2.493591821),
			FuelPressure:    float64(11.79092738),
			CoolantPressure: float64(3.178980794),
			LubOilTemp:      float64(84.14416293),
			CoolantTemp:     float64(81.6321865),
			EngineCondition: int64(1),
		},
	}

	// This test just ensures the function runs without panicking
	// We can't easily test the output since it goes to stdout
	utils.PrintCSVRecords(records)
}
