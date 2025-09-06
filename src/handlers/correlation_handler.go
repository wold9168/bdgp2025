package handlers

import (
	"bdgp2025/src/db_interface"
	"fmt"

	"github.com/apache/iotdb-client-go/v2/client"
)

// HandleCorrelationCalc 处理相关性计算功能
func HandleCorrelationCalc(session client.Session, deviceId string, timeout int64) (string, error) {
	result, err := db_interface.GetCorrelationResult(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	columnNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	var output string

	// 添加标题行
	output += "\t"
	for i := 1; i < len(columnNames); i++ {
		output += fmt.Sprintf("%s\t", columnNames[i])
	}
	output += "\n"

	// 添加相关性矩阵
	for i := 0; i < len(result.PearsonCorrelation); i++ {
		output += fmt.Sprintf("%s\t", columnNames[i+1])
		for j := 0; j < len(result.PearsonCorrelation[i]); j++ {
			output += fmt.Sprintf("%.4f\t", result.PearsonCorrelation[i][j])
		}
		output += "\n"
	}

	return output, nil
}
