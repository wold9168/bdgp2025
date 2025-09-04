package utils

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
)

// IoTDBConfig represents the configuration for IoTDB connection
type IoTDBConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Timeout  int    `json:"timeout"`
}

// Source indicates where a configuration value came from
type Source string

const (
	DefaultValue Source = "default"
	FileValue    Source = "file"
	FlagValue    Source = "flag"
)

// ConfigWithSource wraps a config value with its source information
type ConfigWithSource struct {
	Value  string
	Source Source
}

// IoTDBConfigWithSources represents the configuration with source information
type IoTDBConfigWithSources struct {
	Host     ConfigWithSource
	Port     ConfigWithSource
	User     ConfigWithSource
	Password ConfigWithSource
	Timeout  ConfigWithSource
}

// LoadIoTDBConfig loads IoTDB configuration with proper precedence:
// flag values > file values > default values
func LoadIoTDBConfig() (*IoTDBConfigWithSources, error) {
	// Define flag variables
	var (
		flagHost     string
		flagPort     string
		flagUser     string
		flagPassword string
		flagTimeout  int
		configFile   string
		showConfig   bool
	)

	// Setup flags
	flag.StringVar(&flagHost, "host", "", "IoTDB host")
	flag.StringVar(&flagPort, "port", "", "IoTDB port")
	flag.StringVar(&flagUser, "user", "", "IoTDB user")
	flag.StringVar(&flagPassword, "password", "", "IoTDB password")
	flag.IntVar(&flagTimeout, "timeout", 0, "Timeout in milliseconds")
	flag.StringVar(&configFile, "config", "config/iotdb.json", "Path to config file")
	flag.BoolVar(&showConfig, "show-config", false, "Show configuration sources")

	// Parse flags
	flag.Parse()

	// Load config from file
	fileConfig, err := loadConfigFromFile(configFile)
	if err != nil {
		// If file doesn't exist, use default values
		fileConfig = &IoTDBConfig{
			Host:     "127.0.0.1",
			Port:     "6667",
			User:     "root",
			Password: "root",
			Timeout:  1000,
		}
	}

	// Determine the source of each configuration value
	configWithSources := &IoTDBConfigWithSources{}

	// Host
	if flagHost != "" {
		configWithSources.Host = ConfigWithSource{Value: flagHost, Source: FlagValue}
	} else if fileConfig.Host != "" {
		configWithSources.Host = ConfigWithSource{Value: fileConfig.Host, Source: FileValue}
	} else {
		configWithSources.Host = ConfigWithSource{Value: "127.0.0.1", Source: DefaultValue}
	}

	// Port
	if flagPort != "" {
		configWithSources.Port = ConfigWithSource{Value: flagPort, Source: FlagValue}
	} else if fileConfig.Port != "" {
		configWithSources.Port = ConfigWithSource{Value: fileConfig.Port, Source: FileValue}
	} else {
		configWithSources.Port = ConfigWithSource{Value: "6667", Source: DefaultValue}
	}

	// User
	if flagUser != "" {
		configWithSources.User = ConfigWithSource{Value: flagUser, Source: FlagValue}
	} else if fileConfig.User != "" {
		configWithSources.User = ConfigWithSource{Value: fileConfig.User, Source: FileValue}
	} else {
		configWithSources.User = ConfigWithSource{Value: "root", Source: DefaultValue}
	}

	// Password
	if flagPassword != "" {
		configWithSources.Password = ConfigWithSource{Value: flagPassword, Source: FlagValue}
	} else if fileConfig.Password != "" {
		configWithSources.Password = ConfigWithSource{Value: fileConfig.Password, Source: FileValue}
	} else {
		configWithSources.Password = ConfigWithSource{Value: "root", Source: DefaultValue}
	}

	// Timeout
	if flagTimeout > 0 {
		configWithSources.Timeout = ConfigWithSource{Value: fmt.Sprintf("%d", flagTimeout), Source: FlagValue}
	} else if fileConfig.Timeout > 0 {
		configWithSources.Timeout = ConfigWithSource{Value: fmt.Sprintf("%d", fileConfig.Timeout), Source: FileValue}
	} else {
		configWithSources.Timeout = ConfigWithSource{Value: "1000", Source: DefaultValue}
	}

	// Show configuration sources if requested
	if showConfig {
		fmt.Println("Configuration sources:")
		fmt.Printf("Host: %s (%s)\n", configWithSources.Host.Value, configWithSources.Host.Source)
		fmt.Printf("Port: %s (%s)\n", configWithSources.Port.Value, configWithSources.Port.Source)
		fmt.Printf("User: %s (%s)\n", configWithSources.User.Value, configWithSources.User.Source)
		fmt.Printf("Password: %s (%s)\n", configWithSources.Password.Value, configWithSources.Password.Source)
		fmt.Printf("Timeout: %s (%s)\n", configWithSources.Timeout.Value, configWithSources.Timeout.Source)
	}

	return configWithSources, nil
}

// ToIoTDBConfig converts IoTDBConfigWithSources to IoTDBConfig
func (c *IoTDBConfigWithSources) ToIoTDBConfig() *IoTDBConfig {
	timeout, _ := strconv.Atoi(c.Timeout.Value)
	return &IoTDBConfig{
		Host:     c.Host.Value,
		Port:     c.Port.Value,
		User:     c.User.Value,
		Password: c.Password.Value,
		Timeout:  timeout,
	}
}

// loadConfigFromFile loads configuration from a JSON file
func loadConfigFromFile(filePath string) (*IoTDBConfig, error) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, err
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Parse JSON
	var config IoTDBConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
