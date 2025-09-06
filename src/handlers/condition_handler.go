package handlers

import (
	"bdgp2025/src/db_interface"
	"fmt"

	"github.com/apache/iotdb-client-go/v2/client"
)

// HandleConditionAnalysis 处理条件分析功能
func HandleConditionAnalysis(session client.Session, deviceId string, timeout int64) (string, error) {
	result, err := db_interface.GetConditionAnalysisResult(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	columnNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	var output string

	// 添加标题
	output += "Engine Condition Analysis\n"
	output += "========================\n\n"

	// 为每个条件值打印统计信息
	for _, conditionValue := range result.ConditionValues {
		stats := result.Statistics[conditionValue]
		output += fmt.Sprintf("Condition %d:\n", conditionValue)
		output += fmt.Sprintf("  Count: %d\n", stats.Cnt)

		output += "  Column Statistics:\n"
		for i := 1; i < len(columnNames); i++ {
			output += fmt.Sprintf("    %s:\n", columnNames[i])
			output += fmt.Sprintf("      Sum: %.2f\n", stats.Sum[i])
			output += fmt.Sprintf("      Mean: %.2f\n", stats.Mean[i])
			output += fmt.Sprintf("      Variance: %.2f\n", stats.Variance[i])
			output += fmt.Sprintf("      StdDev: %.2f\n", stats.StdDev[i])
			output += fmt.Sprintf("      Min: %.2f\n", stats.Min[i])
			output += fmt.Sprintf("      Max: %.2f\n", stats.Max[i])
			output += fmt.Sprintf("      Median: %.2f\n", stats.Median[i])
			output += fmt.Sprintf("      Q1: %.2f\n", stats.Q1[i])
			output += fmt.Sprintf("      Q3: %.2f\n", stats.Q3[i])
			output += fmt.Sprintf("      IQR: %.2f\n", stats.IQR[i])
			output += fmt.Sprintf("      Skewness: %.2f\n", stats.Skewness[i])
			output += fmt.Sprintf("      Kurtosis: %.2f\n", stats.Kurtosis[i])
		}
		output += "\n"
	}

	return output, nil
}
