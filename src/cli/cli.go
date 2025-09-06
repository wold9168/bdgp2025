package cli

import (
	"bdgp2025/src/handlers"
	"bdgp2025/src/utils"
	"flag"
	"fmt"
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
	statisticCalc := flag.Bool("stat", false, "Calculate statistics (shorthand)")
	statisticGraph := flag.Bool("graph", false, "Generate statistic graph (shorthand)")
	correlationCalc := flag.Bool("corr", false, "Calculate correlation coefficients (shorthand)")
	conditionAnalysis := flag.Bool("condition", false, "Analyze engine conditions (shorthand)")

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
	} else if *statisticCalc {
		// Execute statistic calculation
		handleStatisticCalc(session, *deviceId, timeout)
	} else if *statisticGraph {
		// Execute statistic graph generation
		handleStatisticGraph(session, *deviceId, timeout)
	} else if *correlationCalc {
		// Execute correlation calculation
		handleCorrelationCalc(session, *deviceId, timeout)
	} else if *conditionAnalysis {
		// Execute condition analysis
		handleConditionAnalysis(session, *deviceId, timeout)
	}
}

// handleCSVImport 处理CSV文件导入功能
func handleCSVImport(csvFile string, session client.Session, deviceId string) {
	err := handlers.HandleCSVImport(csvFile, session, deviceId)
	if err != nil {
		log.Fatal(err)
	}
}

func handleStatisticCalc(session client.Session, deviceId string, timeout int64) {
	result, err := handlers.HandleStatisticCalc(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(result)
}

func handleCorrelationCalc(session client.Session, deviceId string, timeout int64) {
	result, err := handlers.HandleCorrelationCalc(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(result)
}

func handleStatisticGraph(session client.Session, deviceId string, timeout int64) {
	result, err := handlers.HandleStatisticGraph(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(result)
}

func handleConditionAnalysis(session client.Session, deviceId string, timeout int64) {
	result, err := handlers.HandleConditionAnalysis(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(result)
}
