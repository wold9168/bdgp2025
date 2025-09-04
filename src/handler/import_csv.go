package handler

import (
	"bdgp2025/src/utils"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

func ReadinCSVOneByOne(args ...interface{}) error {
	var data utils.CSVRecord = args[0].(utils.CSVRecord)
	var session client.Session = args[1].(client.Session)
	var deviceId string = args[2].(string)

	// Create dataMatrix from the CSVRecord data
	dataMatrix := [][]interface{}{
		{
			data.EngineRPM,
			data.LubOilPressure,
			data.FuelPressure,
			data.CoolantPressure,
			data.LubOilTemp,
			data.CoolantTemp,
			data.EngineCondition,
		},
	}

	InsertRecordsOfOneDevice(session, deviceId, dataMatrix)
	return nil
}

func InsertRecordsOfOneDevice(session client.Session, deviceId string, dataMatrix [][]interface{}) {
	ts := time.Now().UTC().UnixNano() / 1000000
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
	utils.CheckError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
}
