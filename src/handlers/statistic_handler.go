package handlers

import (
	"bdgp2025/src/db_interface"
	"fmt"
	"reflect"

	"github.com/apache/iotdb-client-go/v2/client"
)

// HandleStatisticCalc 处理统计计算功能
func HandleStatisticCalc(session client.Session, deviceId string, timeout int64) (string, error) {
	result, err := db_interface.GetStatisticsResult(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	v := reflect.ValueOf(result)
	t := reflect.TypeOf(result)
	columnNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	var output string
	// 添加列名
	for _, columnName := range columnNames {
		output += fmt.Sprintf("%s\t", columnName)
	}
	output += "\n"

	// 添加统计结果
	for i := 0; i < t.NumField(); i++ {
		output += fmt.Sprintf("%s: %v\n", t.Field(i).Name, v.Field(i).Interface())
	}

	return output, nil
}
