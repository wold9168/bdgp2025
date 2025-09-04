package cli

import (
	"bdgp2025/src/handler"
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
	deviceId := flag.String("device-id", "root.ln.dev9", "Device ID for IoTDB")

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
	}
}

// handleCSVImport is a placeholder function for CSV import functionality
// This function should be implemented according to specific requirements
func handleCSVImport(csvFile string, session client.Session, deviceId string) {
	log.Printf("Importing data from CSV file: %s", csvFile)
	utils.ImportCSVFile(csvFile, handler.ReadinCSVOneByOne, session, deviceId)
}
