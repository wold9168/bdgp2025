package utils

import (
	"bdgp2025/src/db_interface"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

func ReadinCSVOneByOne(args ...interface{}) error {
	var data CSVRecord = args[0].(CSVRecord)
	var session client.Session = args[1].(client.Session)
	var deviceId string = args[2].(string)
	var ts int64 = args[3].(int64)
	// Create dataMatrix from the CSVRecord data
	dataMatrix := [][]interface{}{
		{
			data.EngineRPM,
			data.LubOilPressure,
			data.FuelPressure,
			data.CoolantPressure,
			data.LubOilTemp,
			data.CoolantTemp,
			data.EngineCondition,
		},
	}

	db_interface.InsertRecordsOfOneDevice(session, deviceId, dataMatrix, ts)
	return nil
}

// CSVRecord represents a single row from the CSV file
type CSVRecord struct {
	EngineRPM       int64
	LubOilPressure  float64
	FuelPressure    float64
	CoolantPressure float64
	LubOilTemp      float64
	CoolantTemp     float64
	EngineCondition int64
}

// ImportCSVFile reads the entire CSV file and applies the provided function to each record
// The file is processed row by row to handle large files efficiently
func ImportCSVFile(filePath string, processFunc func(...interface{}) error, args ...interface{}) error {
	var session client.Session = args[0].(client.Session)
	var deviceId string = args[1].(string)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read the header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	// Validate the header
	expectedHeader := []string{"Engine rpm", "Lub oil pressure", "Fuel pressure", "Coolant pressure", "lub oil temp", "Coolant temp", "Engine Condition"}
	if len(header) != len(expectedHeader) {
		return fmt.Errorf("unexpected number of columns in header")
	}

	// Process each row
	cnt := 0
	var ts int64 = time.Now().UTC().UnixNano() / 1000000
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row: %v", err)
		}

		// Validate the row has the correct number of columns
		if len(row) != len(expectedHeader) {
			return fmt.Errorf("row has incorrect number of columns")
		}

		// Convert string values to appropriate types
		engineRPMInt, err := strconv.ParseInt(row[0], 0, 64)
		if err != nil {
			return fmt.Errorf("failed to parse EngineRPM: %v", err)
		}

		lubOilPressure, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return fmt.Errorf("failed to parse LubOilPressure: %v", err)
		}

		fuelPressure, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return fmt.Errorf("failed to parse FuelPressure: %v", err)
		}

		coolantPressure, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			return fmt.Errorf("failed to parse CoolantPressure: %v", err)
		}

		lubOilTemp, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return fmt.Errorf("failed to parse LubOilTemp: %v", err)
		}

		coolantTemp, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			return fmt.Errorf("failed to parse CoolantTemp: %v", err)
		}

		engineConditionBool, err := strconv.ParseInt(row[6], 0, 64)
		if err != nil {
			return fmt.Errorf("failed to parse EngineCondition: %v", err)
		}

		// Create a CSVRecord from the row data
		record := CSVRecord{
			EngineRPM:       engineRPMInt,
			LubOilPressure:  lubOilPressure,
			FuelPressure:    fuelPressure,
			CoolantPressure: coolantPressure,
			LubOilTemp:      lubOilTemp,
			CoolantTemp:     coolantTemp,
			EngineCondition: engineConditionBool,
		}

		// Apply the provided function to the record
		ts++ // Avoid data overwriting
		if err := processFunc(record, session, deviceId, ts); err != nil {
			return fmt.Errorf("error processing record: %v", err)
		}
		cnt++
	}
	fmt.Println("Processed", cnt, "items.")
	return nil
}

// ReadCSVFileFirst5Rows reads the first 5 rows from a CSV file at the given path
func ReadCSVFileFirst5Rows(filePath string) ([]CSVRecord, error) {
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

		// Convert string values to appropriate types
		engineRPMInt, err := strconv.ParseInt(row[0], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse EngineRPM: %v", err)
		}

		lubOilPressure, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse LubOilPressure: %v", err)
		}

		fuelPressure, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse FuelPressure: %v", err)
		}

		coolantPressure, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse CoolantPressure: %v", err)
		}

		lubOilTemp, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse LubOilTemp: %v", err)
		}

		coolantTemp, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse CoolantTemp: %v", err)
		}

		engineConditionBool, err := strconv.ParseInt(row[6], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse EngineCondition: %v", err)
		}

		// Create a CSVRecord from the row data
		record := CSVRecord{
			EngineRPM:       engineRPMInt,
			LubOilPressure:  lubOilPressure,
			FuelPressure:    fuelPressure,
			CoolantPressure: coolantPressure,
			LubOilTemp:      lubOilTemp,
			CoolantTemp:     coolantTemp,
			EngineCondition: engineConditionBool,
		}

		records = append(records, record)
	}

	return records, nil
}

// PrintCSVRecords prints the CSV records to stdout
// This function is just designed for debug.
func PrintCSVRecords(records []CSVRecord) {
	fmt.Println("Engine RPM\tLub Oil Pressure\tFuel Pressure\tCoolant Pressure\tLub Oil Temp\tCoolant Temp\tEngine Condition")
	for _, record := range records {
		fmt.Printf("%d\t\t%.2f\t\t\t%.2f\t\t%.2f\t\t\t%.2f\t\t%.2f\t\t%t\n",
			record.EngineRPM,
			record.LubOilPressure,
			record.FuelPressure,
			record.CoolantPressure,
			record.LubOilTemp,
			record.CoolantTemp,
			record.EngineCondition)
	}
}
