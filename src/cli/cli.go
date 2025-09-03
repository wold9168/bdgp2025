package cli

import (
	config "bdgp2025/src/utils"
	"log"

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

}
