package db_interface

import (
	"strings"

	"github.com/apache/iotdb-client-go/v2/client"
)

// ColumnInfo holds information about a column including its index and data type
type ColumnInfo struct {
	Index    int32
	DataType client.TSDataType
}

// GetColumnMapping creates a mapping from column names to their index and data type
func GetColumnMapping(sds *client.SessionDataSet) map[string]ColumnInfo {
	columnNames := sds.GetColumnNames()
	columnTypes := sds.GetColumnTypes()

	mapping := make(map[string]ColumnInfo)
	for i, columnName := range columnNames {
		dataType, _ := client.GetDataTypeByStr(columnTypes[i])
		mapping[columnName] = ColumnInfo{
			Index:    int32(i),
			DataType: dataType,
		}
	}
	return mapping
}

// GetSimpleColumnMapping creates a mapping from simple column names (without device prefix) to ColumnInfo
func GetSimpleColumnMapping(sds *client.SessionDataSet) map[string]ColumnInfo {
	columnNames := sds.GetColumnNames()
	columnTypes := sds.GetColumnTypes()

	mapping := make(map[string]ColumnInfo)
	for i, fullColumnName := range columnNames {
		// Extract base column name (after last dot)
		parts := strings.Split(fullColumnName, ".")
		baseName := parts[len(parts)-1]

		dataType, _ := client.GetDataTypeByStr(columnTypes[i])
		mapping[baseName] = ColumnInfo{
			Index:    int32(i) + 1,
			DataType: dataType,
		}
	}
	return mapping
}
