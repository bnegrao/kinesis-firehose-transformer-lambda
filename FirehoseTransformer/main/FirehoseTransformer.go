package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/aws/aws-lambda-go/events"

	"github.com/aws/aws-lambda-go/lambda"
)

type Event struct {
	Message string `json:"message"`
}

func main() {
	lambda.Start(handler)
}

func handler(evnt events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	var response events.KinesisFirehoseResponse

	for _, record := range evnt.Records {
		fmt.Printf("Processando record de id: %s\n", record.RecordID)

		var responseRecord events.KinesisFirehoseResponseRecord

		processedData, err := process(record.Data)
		if err != nil {
			fmt.Printf("ERROR: The event record of id %s could not be processed. The error was: %s\n", record.RecordID, err)
			responseRecord = buildResponseRecord(record.RecordID, events.KinesisFirehoseTransformedStateProcessingFailed, record.Data)
		} else {
			responseRecord = buildResponseRecord(record.RecordID, events.KinesisFirehoseTransformedStateOk, processedData)
		}

		response.Records = append(response.Records, responseRecord)
	}

	return response, nil
}

func buildResponseRecord(recordId string, transformedState string, data []byte) events.KinesisFirehoseResponseRecord {
	transformedRecord := events.KinesisFirehoseResponseRecord{
		RecordID: recordId,
		Result:   transformedState,
		Data:     data,
	}

	return transformedRecord
}

func process(dataBytes []byte) ([]byte, error) {
	// IOT added a '\n' to the end of the data payload. We need to remove
	// that newline before decompressing. The '\n' is the last byte in the payload
	var dataBytesWithoutNewline []byte = dataBytes[0 : len(dataBytes)-1]

	decompressedMessage, err := decompress(dataBytesWithoutNewline)
	if err != nil {
		return nil, fmt.Errorf("Failed to decompress the message, error: %s", err)
	}

	fmt.Printf("decompressed message:\n%s\n", string(decompressedMessage))

	return decompressedMessage, err
}

func decompress(message []byte) ([]byte, error) {

	buffer := bytes.NewBuffer(message)
	reader, err := gzip.NewReader(buffer)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	_, err = io.Copy(&out, reader)
	if err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}
