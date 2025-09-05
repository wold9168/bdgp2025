package db_interface

import (
	"log"

	"github.com/apache/iotdb-client-go/v2/client"
)

func TraverseWithProcess(session client.Session, deviceId string, timeout int64, processFunc func(float64)) error {
	sql := "select * from " + deviceId

	columnNames, columnTypes, errMetadata := FetchMetadata(session, deviceId, timeout)
	if errMetadata != nil {
		log.Fatal(errMetadata)
		return errMetadata
	}

	columnLength := int32(len(columnNames))

	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		for next, err := ds.Next(); err == nil && next; next, err = ds.Next() {
			var i int32 = 1
			for ; i < columnLength; i++ {
				var index int32 = i + 1 //For Get***ByIndex(), index 1 is timestamp.
				columnType := columnTypes[i]
				data, err := fetchDataByColumnType(ds, columnType, index)
				if err != nil {
					log.Fatal(err)
				}
				processFunc(data)
			}
		}
	} else {
		log.Fatal(err)
		return err
	}
	return nil
}
