package utils

import (
	"log"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
)

func CheckError(status *common.TSStatus, err error) {
	if err != nil {
		log.Fatal(err)
	}

	if status != nil {
		if err = client.VerifySuccess(status); err != nil {
			log.Println(err)
		}
	}
}
