package db_interface

import (
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

func InsertRecordsOfOneDevice(session client.Session, deviceId string, dataMatrix [][]interface{}, ts int64) {
	if ts == 0 {
		ts = time.Now().UTC().UnixNano() / 1000000
	}
	var (
		measurementsSlice = [][]string{
			{"engine_rpm", "lub_oil_pressure", "fuel_pressure", "coolant_pressure", "luboil_temp", "coolant_temp", "engine_condition"},
		}
		dataTypes = [][]client.TSDataType{
			{client.INT64, client.DOUBLE, client.DOUBLE, client.DOUBLE, client.DOUBLE, client.DOUBLE, client.INT64},
		}
		values     = dataMatrix
		timestamps = []int64{ts}
	)
	CheckError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
}
