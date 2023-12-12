package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	fileName := "urls.txt"
	content, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	urls := strings.Split(string(content), "\n")

	folderName := "images"

	_, err = os.Stat(folderName)
	if os.IsNotExist(err) {
		err = os.Mkdir(folderName, 0755)
		if err != nil {
			fmt.Println("ERROR CREATING FOLDER: ", err)
			return
		}
	}

	for i, url := range urls {
		if url == "" {
			continue
		}
		filename := strconv.Itoa(i) + ".jpeg"
		filepath := filepath.Join(folderName, filename)

		err := downloadImage(url, filepath)
		if err != nil {
			fmt.Printf("ERROR DOWNLOADING IMAGE %s: %v\n", filename, err)
		} else {
			fmt.Printf("IMAGE %s DOWNLOADED AT %s\n", filename, filepath)
		}
	}
	queueName := "imageQueue"
	err = QueueImage(folderName, queueName)
	if err != nil {
		fmt.Println("ERROR QUEUING IMAGE TO RABBITMQ: ", err)
	} else {
		fmt.Printf("ALL IMAGES DOWNLOADED AT %s AND QUEUED AT \n", folderName)
	}

	err = Consume(queueName, folderName)
	if err != nil {
		fmt.Println("ERROR IN CONSUMER:", err)
	}

	time.Sleep(5 * time.Second)
}

func downloadImage(url, filepath string) error {
	response, err := http.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, response.Body)
	if err != nil {
		return err
	}
	return nil
}
