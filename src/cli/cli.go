package cli

import (
	"bdgp2025/src/db_interface"
	"bdgp2025/src/utils"
	"flag"
	"log"
	"path/filepath"
	"strings"

	"github.com/apache/iotdb-client-go/v2/client"
)

func Main() {
	// Define the import-csv flag
	importCSV := flag.String("i", "", "Import data from CSV file (shorthand)")
	importCSVLong := flag.String("import-csv", "", "Import data from CSV file")
	deviceId := flag.String("device-id", "root.example.exampledev", "Device ID for IoTDB")
	calculateStats := flag.Bool("calculate-statistics", false, "Calculate statistics for the device")

	flag.Parse()

	// Load configuration with proper precedence
	configWithSources, err := utils.LoadIoTDBConfig()
	if err != nil {
		log.Fatal(err)
	}

	// Convert to IoTDB config
	iotdbConfig := configWithSources.ToIoTDBConfig()

	config := &client.Config{
		Host:     iotdbConfig.Host,
		Port:     iotdbConfig.Port,
		UserName: iotdbConfig.User,
		Password: iotdbConfig.Password,
	}
	timeout := iotdbConfig.Timeout

	session := client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Check if import-csv flag is provided
	if *importCSV != "" || *importCSVLong != "" {
		csvFile := *importCSV
		if csvFile == "" {
			csvFile = *importCSVLong
		}

		// Check if the file has a .csv extension
		ext := strings.ToLower(filepath.Ext(csvFile))
		if ext != ".csv" {
			log.Fatalf("Error: File '%s' is not a CSV file (extension: %s)", csvFile, ext)
		}

		handleCSVImport(csvFile, session, *deviceId)
	} else if *calculateStats {
		// Calculate statistics if the flag is set
		handleCalculateStatistics(session, *deviceId, timeout)
	}
}

// handleCSVImport is a placeholder function for CSV import functionality
// This function should be implemented according to specific requirements
func handleCSVImport(csvFile string, session client.Session, deviceId string) {
	log.Printf("Importing data from CSV file: %s", csvFile)
	utils.ImportCSVFile(csvFile, utils.ReadinCSVOneByOne, session, deviceId)
}

func handleCalculateStatistics(session client.Session, deviceId string, timeout int64) {
	log.Printf("Calculate statistics for the device, deviceId: %s", deviceId)
	var sql string = "select * from " + deviceId
	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		sumResults, cnt := db_interface.GetSums(ds)
		db_interface.PrintSums(ds, sumResults)
		averageResults := db_interface.GetAverageFromSumResults(sumResults, cnt)
		db_interface.PrintAverages(ds, averageResults)
		ds.Close()
	} else {
		log.Fatal(err)
	}
	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		varianceResults := db_interface.GetVariance(ds)
		db_interface.PrintVariances(ds, varianceResults)
		ds.Close()
	} else {
		log.Fatal(err)
	}
}
