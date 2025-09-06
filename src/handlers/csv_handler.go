package handlers

import (
	"bdgp2025/src/utils"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/apache/iotdb-client-go/v2/client"
)

// HandleCSVImport 处理CSV文件导入功能
func HandleCSVImport(csvFile string, session client.Session, deviceId string) error {
	// 检查文件扩展名
	ext := strings.ToLower(filepath.Ext(csvFile))
	if ext != ".csv" {
		return fmt.Errorf("file '%s' is not a CSV file (extension: %s)", csvFile, ext)
	}

	log.Printf("Importing data from CSV file: %s", csvFile)
	utils.ImportCSVFile(csvFile, utils.ReadinCSVOneByOne, session, deviceId)
	return nil
}
