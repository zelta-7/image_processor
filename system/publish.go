package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/streadway/amqp"
)

func QueueImage(folderName, queueName string) error {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("CONNECTION FAILED: %v\n", err)
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("CHANNEL FAILED: %v\n", err)
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
		fmt.Printf("QUEUE CREATION FAILED: %v\n", err)
		return err
	}

	files, err := os.ReadDir(folderName)
	if err != nil {
		fmt.Printf("ERROR OPENING DIRECTORY: %v\n", err)
		return err
	}

	for _, file := range files {
		filepath := filepath.Join(folderName, file.Name())

		imageData, err := os.ReadFile(filepath)
		if err != nil {
			fmt.Printf("ERROR READING IMAGE %s: %v\n", file.Name(), err)
			continue
		}
		err = ch.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "image/jpeg",
				Body:        imageData,
				MessageId:   file.Name(),
			},
		)
		if err != nil {
			fmt.Printf("ERROR QUEUING IMAGE %s: %v\n", file.Name(), err)
		} else {
			fmt.Printf("IMAGE %s QUEUED SUCESSFULLY\n", file.Name())
		}
	}
	return nil
}
