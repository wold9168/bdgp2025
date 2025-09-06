package cli

import (
	"bdgp2025/src/db_interface"
	"bdgp2025/src/utils"
	"bdgp2025/src/utils/histogram"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"strconv"
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

// handleCSVImport is a placeholder function for CSV import functionality
// This function should be implemented according to specific requirements
func handleCSVImport(csvFile string, session client.Session, deviceId string) {
	log.Printf("Importing data from CSV file: %s", csvFile)
	utils.ImportCSVFile(csvFile, utils.ReadinCSVOneByOne, session, deviceId)
}

func handleStatisticCalc(session client.Session, deviceId string, timeout int64) {
	result, err := db_interface.GetStatisticsResult(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	v := reflect.ValueOf(result)
	t := reflect.TypeOf(result)
	columeNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	for _, columeName := range columeNames {
		fmt.Printf("%s\t", columeName)
	}
	for i := 0; i < t.NumField(); i++ {
		fmt.Printf("%s: %v\n", t.Field(i).Name, v.Field(i).Interface())
	}
}

func handleCorrelationCalc(session client.Session, deviceId string, timeout int64) {
	result, err := db_interface.GetCorrelationResult(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}
	columnNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}

	// Print header row
	fmt.Print("\t")
	for i := 1; i < len(columnNames); i++ {
		fmt.Printf("%s\t", columnNames[i])
	}
	fmt.Println()

	// Print correlation matrix
	for i := 0; i < len(result.PearsonCorrelation); i++ {
		fmt.Printf("%s\t", columnNames[i+1])
		for j := 0; j < len(result.PearsonCorrelation[i]); j++ {
			fmt.Printf("%.4f\t", result.PearsonCorrelation[i][j])
		}
		fmt.Println()
	}
}

func handleStatisticGraph(session client.Session, deviceId string, timeout int64) {
	columnNames, _, errMetadata := db_interface.FetchMetadata(session, deviceId, timeout)
	if errMetadata != nil {
		log.Fatal(errMetadata)
		return
	}
	columnLength := int32(len(columnNames))
	hists := make([]*histogram.StreamingHistogram, columnLength)
	for i := 1; i < len(hists); i++ {
		hists[i] = histogram.NewStreamingHistogram(histogram.DefaultConfig())
		db_interface.TraverseWithProcess(session, deviceId, timeout, hists[i].AddValue, int32(i))
		result := hists[i].Finalize()
		result.SaveAsHTML("output" + strconv.Itoa(i) + " " + columnNames[i] + ".html")
	}

}

func handleConditionAnalysis(session client.Session, deviceId string, timeout int64) {
	result, err := db_interface.GetConditionAnalysisResult(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
	}

	// Print header
	fmt.Printf("Engine Condition Analysis\n")
	fmt.Printf("========================\n\n")

	// For each condition value, print statistics
	for _, conditionValue := range result.ConditionValues {
		stats := result.Statistics[conditionValue]
		fmt.Printf("Condition %d:\n", conditionValue)
		fmt.Printf("  Count: %d\n", stats.Cnt)

		// Get column names for meaningful output
		columnNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("  Column Statistics:\n")
		for i := 1; i < len(columnNames); i++ {
			fmt.Printf("    %s:\n", columnNames[i])
			fmt.Printf("      Sum: %.2f\n", stats.Sum[i])
			fmt.Printf("      Mean: %.2f\n", stats.Mean[i])
			fmt.Printf("      Variance: %.2f\n", stats.Variance[i])
			fmt.Printf("      StdDev: %.2f\n", stats.StdDev[i])
			fmt.Printf("      Min: %.2f\n", stats.Min[i])
			fmt.Printf("      Max: %.2f\n", stats.Max[i])
			fmt.Printf("      Median: %.2f\n", stats.Median[i])
			fmt.Printf("      Q1: %.2f\n", stats.Q1[i])
			fmt.Printf("      Q3: %.2f\n", stats.Q3[i])
			fmt.Printf("      IQR: %.2f\n", stats.IQR[i])
			fmt.Printf("      Skewness: %.2f\n", stats.Skewness[i])
			fmt.Printf("      Kurtosis: %.2f\n", stats.Kurtosis[i])
		}
		fmt.Printf("\n")
	}
}
