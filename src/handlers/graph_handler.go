package handlers

import (
	"bdgp2025/src/db_interface"
	"bdgp2025/src/utils/histogram"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	var fileList []string

	for i := 1; i < len(hists); i++ {
		hists[i] = histogram.NewStreamingHistogram(histogram.DefaultConfig())
		err := db_interface.TraverseWithProcess(session, deviceId, timeout, hists[i].AddValue, int32(i))
		if err != nil {
			return "", err
		}

		result := hists[i].Finalize()
		filename := "output" + strconv.Itoa(i) + " " + columnNames[i] + ".html"
		// 生成到frontend/static/graphs目录下
		graphPath := filepath.Join("frontend", "static", "graphs", filename)
		err = result.SaveAsHTML(graphPath)
		if err != nil {
			return "", err
		}

		output += fmt.Sprintf("Generated graph for %s: %s\n", columnNames[i], filename)
		fileList = append(fileList, filename)
	}

	// 生成索引文件
	err = generateIndexFile(fileList)
	if err != nil {
		return "", err
	}

	return output, nil
}

// generateIndexFile 生成图表文件的索引页面
func generateIndexFile(fileList []string) error {
	indexPath := filepath.Join("frontend", "static", "graphs", "index.html")

	// 创建文件列表的HTML项
	var fileItems strings.Builder
	for _, file := range fileList {
		fileItems.WriteString(fmt.Sprintf("<li><a href=\"%s\" target=\"_blank\">%s</a></li>\n", file, file))
	}

	// 生成完整的HTML内容
	indexContent := fmt.Sprintf(`<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>生成的图表文件列表</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            text-align: center;
        }
        ul {
            list-style-type: none;
            padding: 0;
        }
        li {
            margin: 10px 0;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 4px;
            border: 1px solid #dee2e6;
        }
        a {
            color: #007bff;
            text-decoration: none;
            font-weight: bold;
        }
        a:hover {
            text-decoration: underline;
        }
        .back-link {
            display: block;
            text-align: center;
            margin-top: 20px;
            color: #007bff;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>生成的图表文件列表</h1>
        <ul>
            %s
        </ul>
        <a href="/" class="back-link">返回主页面</a>
    </div>
</body>
</html>`, fileItems.String())

	return os.WriteFile(indexPath, []byte(indexContent), 0644)
}
