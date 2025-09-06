package handlers

import (
	"bdgp2025/src/db_interface"
	"bdgp2025/src/utils/histogram"
	"fmt"
	"strconv"

	"github.com/apache/iotdb-client-go/v2/client"
)

// HandleStatisticGraph 处理统计图表生成功能
func HandleStatisticGraph(session client.Session, deviceId string, timeout int64) (string, error) {
	columnNames, _, err := db_interface.FetchMetadata(session, deviceId, timeout)
	if err != nil {
		return "", err
	}

	columnLength := int32(len(columnNames))
	hists := make([]*histogram.StreamingHistogram, columnLength)

	var output string

	for i := 1; i < len(hists); i++ {
		hists[i] = histogram.NewStreamingHistogram(histogram.DefaultConfig())
		err := db_interface.TraverseWithProcess(session, deviceId, timeout, hists[i].AddValue, int32(i))
		if err != nil {
			return "", err
		}

		result := hists[i].Finalize()
		filename := "output" + strconv.Itoa(i) + " " + columnNames[i] + ".html"
		err = result.SaveAsHTML(filename)
		if err != nil {
			return "", err
		}

		output += fmt.Sprintf("Generated graph for %s: %s\n", columnNames[i], filename)
	}

	return output, nil
}
