package server

import (
	"fmt"
	"log"
	"net/http"

	config "bdgp2025/src/utils"

	"github.com/apache/iotdb-client-go/v2/client"
)

func Main() {
	// Load configuration with proper precedence
	configWithSources, err := config.LoadIoTDBConfig()
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

	// Register the /import endpoint
	http.HandleFunc("/import", func(w http.ResponseWriter, r *http.Request) {
		// Call the unimplemented function here
		// TODO: Implement the actual import functionality
		result := callImportFunction()

		fmt.Fprintf(w, "Import API called. Result: %s", result)
	})

	fmt.Println("Server starting on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

// callImportFunction is a placeholder for the actual import functionality
func callImportFunction() string {
	// TODO: Implement the actual import logic
	return "Not implemented yet"
}
