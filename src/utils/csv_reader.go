package utils

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// CSVRecord represents a single row from the CSV file
type CSVRecord struct {
	EngineRPM       string
	LubOilPressure  string
	FuelPressure    string
	CoolantPressure string
	LubOilTemp      string
	CoolantTemp     string
	EngineCondition string
}

// ReadCSVFile reads the first 5 rows from a CSV file at the given path
func ReadCSVFile(filePath string) ([]CSVRecord, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read the header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}

	// Validate the header
	expectedHeader := []string{"Engine rpm", "Lub oil pressure", "Fuel pressure", "Coolant pressure", "lub oil temp", "Coolant temp", "Engine Condition"}
	if len(header) != len(expectedHeader) {
		return nil, fmt.Errorf("unexpected number of columns in header")
	}

	// Read up to 5 data rows
	records := make([]CSVRecord, 0, 5)
	for i := 0; i < 5; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row %d: %v", i+1, err)
		}

		// Validate the row has the correct number of columns
		if len(row) != len(expectedHeader) {
			return nil, fmt.Errorf("row %d has incorrect number of columns", i+1)
		}

		// Create a CSVRecord from the row data
		record := CSVRecord{
			EngineRPM:       row[0],
			LubOilPressure:  row[1],
			FuelPressure:    row[2],
			CoolantPressure: row[3],
			LubOilTemp:      row[4],
			CoolantTemp:     row[5],
			EngineCondition: row[6],
		}

		records = append(records, record)
	}

	return records, nil
}

// PrintCSVRecords prints the CSV records to stdout
func PrintCSVRecords(records []CSVRecord) {
	fmt.Println("Engine RPM\tLub Oil Pressure\tFuel Pressure\tCoolant Pressure\tLub Oil Temp\tCoolant Temp\tEngine Condition")
	for _, record := range records {
		fmt.Printf("%s\t\t%s\t\t\t%s\t\t%s\t\t\t%s\t\t%s\t\t%s\n",
			record.EngineRPM,
			record.LubOilPressure,
			record.FuelPressure,
			record.CoolantPressure,
			record.LubOilTemp,
			record.CoolantTemp,
			record.EngineCondition)
	}
}
