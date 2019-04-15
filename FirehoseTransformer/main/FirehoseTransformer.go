package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"

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

		transformedRecord, err := buildResponseRecord(record)
		if err != nil {
			log.Fatalln(err)
		}

		response.Records = append(response.Records, transformedRecord)
	}

	fmt.Printf("%+v\n", response)

	return response, nil
}

func buildResponseRecord(record events.KinesisFirehoseEventRecord) (events.KinesisFirehoseResponseRecord, error) {
	var transformedRecord events.KinesisFirehoseResponseRecord

	data, err := transform(record.Data)
	if err != nil {
		return transformedRecord, err
	}

	transformedRecord.RecordID = record.RecordID
	transformedRecord.Result = events.KinesisFirehoseTransformedStateOk
	transformedRecord.Data = []byte(string(data))

	return transformedRecord, nil
}

func transform(dataBytes []byte) ([]byte, error) {

	decompressedMessage, err := decompress1(dataBytes)
	if err != nil {
		return nil, err
	}

	fmt.Printf("decompressed message:\n%s\n", string(decompressedMessage))

	var pdvEvent map[string]interface{}
	err = json.Unmarshal(decompressedMessage, &pdvEvent)
	if err != nil {
		return nil, err
	}

	validationError := validate(pdvEvent)
	if len(validationError) > 0 {
		pdvEvent["ingesterValidationError"] = validationError
	}

	result, err := json.Marshal(pdvEvent)
	return result, err
}

// validate retorna um erro de validação
// em caso de mensagem valida a string retornada será vazia
func validate(pdvEvent map[string]interface{}) string {
	if _, contains := pdvEvent["eventType"]; contains {
		return ""
	} else {
		return "O json não contém o elemento de nome 'eventType'."
	}
}

func decompress1(message []byte) ([]byte, error) {

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
