package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/streadway/amqp"
)

func Consume(queueName, folderName string) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("CONNECTION FAILED CONSUMER")
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("COULD NOT CONSUME")
		return err
	}

	compressedFolderName := "compressedImage"
	if _, err := os.Stat(compressedFolderName); os.IsNotExist(err) {
		err := os.Mkdir(compressedFolderName, 0755)
		if err != nil {
			return err
		}
	}
	for msg := range msgs {
		imageData := msg.Body

		compressedData, err := compress(imageData)
		if err != nil {
			fmt.Println("ERROR COMPRESSING IMAGE: ", err)
			continue
		}

		imageName := strings.TrimSuffix(msg.MessageId, ".jpeg")
		err = storeCommpressedImage(compressedData, compressedFolderName, imageName)
		if err != nil {
			fmt.Println("ERROR STORING COMPRESSED IMAGE:", err)
		} else {
			fmt.Printf("IMAGE %s COMPRESSED AND STORED\n", imageName)
		}
	}
	return nil
}

func compress(data []byte) ([]byte, error) {
	var compressedData bytes.Buffer
	writer := gzip.NewWriter(&compressedData)

	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}
	return compressedData.Bytes(), nil
}

func storeCommpressedImage(data []byte, folderName, imageName string) error {
	imagePath := filepath.Join(folderName, imageName+".jpeg")
	err := os.WriteFile(imagePath, data, 0644)
	return err
}
