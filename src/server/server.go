package server

import (
	"bdgp2025/src/handlers"
	"fmt"
	"log"
	"net/http"
	"time"

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
	timeout := iotdbConfig.Timeout

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

	// Log server startup
	log.Println("Server started, listening on port 8084")

	// Register the /import endpoint
	http.HandleFunc("/import", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		if r.Method != http.MethodPost {
			log.Printf("Import API: Method not allowed %s\n", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// 获取查询参数
		csvFile := r.URL.Query().Get("csvFile")
		deviceId := r.URL.Query().Get("deviceId")
		if deviceId == "" {
			deviceId = "root.example.exampledev" // 默认设备ID
		}

		log.Printf("Import API: Starting CSV file import, Device ID: %s, File: %s\n", deviceId, csvFile)

		if csvFile == "" {
			log.Println("Import API: Missing csvFile parameter")
			http.Error(w, "csvFile parameter is required", http.StatusBadRequest)
			return
		}

		// 调用处理函数
		err := handlers.HandleCSVImport(csvFile, session, deviceId)
		if err != nil {
			log.Printf("Import API: Processing failed, Error: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		duration := time.Since(startTime)
		log.Printf("Import API: Successfully completed CSV import, Device ID: %s, Duration: %v\n", deviceId, duration)
		fmt.Fprintf(w, "CSV import successful for device %s", deviceId)
	})

	// 注册统计计算端点
	http.HandleFunc("/statistic", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		if r.Method != http.MethodGet {
			log.Printf("Statistic API: Method not allowed %s\n", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		deviceId := r.URL.Query().Get("deviceId")
		if deviceId == "" {
			deviceId = "root.example.exampledev" // 默认设备ID
		}

		log.Printf("Statistic API: Starting statistical data calculation, Device ID: %s\n", deviceId)

		result, err := handlers.HandleStatisticCalc(session, deviceId, timeout)
		if err != nil {
			log.Printf("Statistic API: Calculation failed, Error: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		duration := time.Since(startTime)
		log.Printf("Statistic API: Successfully completed statistical calculation, Device ID: %s, Duration: %v\n", deviceId, duration)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, result)
	})

	// 注册相关性计算端点
	http.HandleFunc("/correlation", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		if r.Method != http.MethodGet {
			log.Printf("Correlation API: Method not allowed %s\n", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		deviceId := r.URL.Query().Get("deviceId")
		if deviceId == "" {
			deviceId = "root.example.exampledev" // 默认设备ID
		}

		log.Printf("Correlation API: Starting correlation data calculation, Device ID: %s\n", deviceId)

		result, err := handlers.HandleCorrelationCalc(session, deviceId, timeout)
		if err != nil {
			log.Printf("Correlation API: Calculation failed, Error: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		duration := time.Since(startTime)
		log.Printf("Correlation API: Successfully completed correlation calculation, Device ID: %s, Duration: %v\n", deviceId, duration)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, result)
	})

	// 注册统计图表生成端点
	http.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		if r.Method != http.MethodGet {
			log.Printf("Graph API: Method not allowed %s\n", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		deviceId := r.URL.Query().Get("deviceId")
		if deviceId == "" {
			deviceId = "root.example.exampledev" // 默认设备ID
		}

		log.Printf("Graph API: Starting statistical chart generation, Device ID: %s\n", deviceId)

		result, err := handlers.HandleStatisticGraph(session, deviceId, timeout)
		if err != nil {
			log.Printf("Graph API: Generation failed, Error: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		duration := time.Since(startTime)
		log.Printf("Graph API: Successfully completed chart generation, Device ID: %s, Duration: %v\n", deviceId, duration)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, result)
	})

	// 注册条件分析端点
	http.HandleFunc("/condition", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		if r.Method != http.MethodGet {
			log.Printf("Condition Analysis API: Method not allowed %s\n", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		deviceId := r.URL.Query().Get("deviceId")
		if deviceId == "" {
			deviceId = "root.example.exampledev" // 默认设备ID
		}

		log.Printf("Condition Analysis API: Starting condition analysis, Device ID: %s\n", deviceId)

		result, err := handlers.HandleConditionAnalysis(session, deviceId, timeout)
		if err != nil {
			log.Printf("Condition Analysis API: Analysis failed, Error: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		duration := time.Since(startTime)
		log.Printf("Condition Analysis API: Successfully completed condition analysis, Device ID: %s, Duration: %v\n", deviceId, duration)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, result)
	})

	fmt.Println("Server starting on :8084...")
	if err := http.ListenAndServe(":8084", nil); err != nil {
		log.Fatal(err)
	}
}
