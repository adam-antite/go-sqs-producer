package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/tidwall/limiter"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

var wg sync.WaitGroup
var sess *session.Session
var queue *sqs.SQS
var currentTime string
var l *limiter.Limiter

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	defer track("main")()

	totalMessages, err := strconv.ParseFloat(os.Args[2], 64)
	if err != nil {
		panic(err)
	}
	numBatches := int(math.Ceil(totalMessages / float64(10)))
	currentTime = time.Now().UTC().Format(time.RFC3339)

	LogFile := fmt.Sprintf("./logs/log_%s.log", currentTime)
	logFile, err := os.OpenFile(LogFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			log.Printf("Error closing log file: " + err.Error())
		}
	}(logFile)
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	switch environment := os.Args[1]; environment {
	case "local":
		sess = session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("not", "empty", ""),
			DisableSSL:  aws.Bool(true),
			Region:      aws.String(endpoints.UsEast1RegionID),
			Endpoint:    aws.String("http://localhost:4566"),
		}))
	case "prod":
		sess = session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY"), os.Getenv("AWS_SECRET_KEY"), ""),
			Region:      aws.String(endpoints.UsEast1RegionID),
		}))
	}

	// FOR LOCAL DEV ONLY

	queue = sqs.New(sess)
	l = limiter.New(100)
	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go sendBatch(i + 1)
	}

	wg.Wait()
}

func sendBatch(batchNumber int) {
	l.Begin()
	var batch []*sqs.SendMessageBatchRequestEntry

	defer wg.Done()
	defer l.End()
	defer trackBatch(batchNumber)()

	for i := 0; i < 10; i++ {
		id := uuid.New().String()
		messageBody := os.Getenv("UID")
		messageRequest := sqs.SendMessageBatchRequestEntry{
			Id:          &id,
			MessageBody: &messageBody,
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"bungieMembershipId": {
					DataType:    aws.String("String"),
					StringValue: aws.String("16650411"),
				},
			},
		}
		batch = append(batch, &messageRequest)
	}

	result, err := queue.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueUrl: aws.String(os.Getenv("QUEUE_URL")),
		Entries:  batch,
	})
	_ = result

	if err != nil {
		log.Println("Could not read from queue", err)
		return
	}
}

func track(name string) func() {
	start := time.Now()
	return func() {
		log.Printf("%s: execution time: %s\n", name, time.Since(start))
	}
}

func trackBatch(batchNumber int) func() {
	start := time.Now()
	return func() {
		log.Printf("Batch #%d finished in: %s\n", batchNumber, time.Since(start))
	}
}
