package test

import (
	"bdgp2025/src/db_interface"
	"fmt"
	"log"
	"testing"

	"github.com/apache/iotdb-client-go/v2/client"
)

func TestAlwaysPasses(t *testing.T) {
	config := &client.Config{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
	}
	session := client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	var timeout int64 = 1000
	const deviceId string = "root.ln.dev7"
	var sql string = "select * from " + deviceId
	if ds, err := session.ExecuteQueryStatement(sql, &timeout); err == nil {
		fmt.Println(ds.GetColumnNames())
		fmt.Println(ds.GetColumnTypes())
		fmt.Println(len(ds.GetColumnNames()))

		ds.Next()
		fmt.Println(ds.GetStringByIndex(1)) //For GetStringByIndex(), index 1 is timestamp.
		ds.Close()
	} else {
		log.Fatal(err)
	}
	result, err := db_interface.GetStatisticsResult(session, deviceId, timeout)
	if err != nil {
		log.Fatal(err)
		t.Fail()
	}
	fmt.Print(result)

}
