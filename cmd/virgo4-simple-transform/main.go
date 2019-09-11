package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//
// main entry point
//
func main() {

	//log.Printf("===> V4 batch ingest service staring up <===")

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	sess, err := session.NewSession( )
	if err != nil {
		log.Fatal( err )
	}

	svc := sqs.New(sess)

	// get the queue URL's from the name
	result, err := svc.GetQueueUrl( &sqs.GetQueueUrlInput{
		QueueName: aws.String( cfg.InQueueName ),
	})

	if err != nil {
		log.Fatal( err )
	}

	inQueueUrl := result.QueueUrl

	result, err = svc.GetQueueUrl( &sqs.GetQueueUrlInput{
		QueueName: aws.String( cfg.OutQueueName ),
	})

	if err != nil {
		log.Fatal( err )
	}

	outQueueUrl := result.QueueUrl

	for {

		log.Printf("Waiting for messages...")

		result, err := svc.ReceiveMessage( &sqs.ReceiveMessageInput{
			//AttributeNames: []*string{
			//	aws.String( sqs.QueueAttributeNameAll ),
			//},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll ),
			},
			QueueUrl:            inQueueUrl,
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64( cfg.PollTimeOut ),
		})

		if err != nil {
			log.Fatal( err )
		}

		// print and then delete
		if len( result.Messages ) != 0 {

			log.Printf("Received %d messages", len( result.Messages ) )

			for _, m := range result.Messages {

				// apply our transform
				transformed := transform( cfg.TransformName, *m.Body )

				_, err := svc.SendMessage( &sqs.SendMessageInput{
					MessageAttributes: map[string]*sqs.MessageAttributeValue{
						"op": &sqs.MessageAttributeValue{
							DataType:    aws.String("String"),
							StringValue: aws.String("add"),
						},
						"src": &sqs.MessageAttributeValue{
							DataType:    aws.String("String"),
							StringValue: aws.String( cfg.InQueueName ),
						},
						"dst": &sqs.MessageAttributeValue{
							DataType:    aws.String("String"),
							StringValue: aws.String( cfg.OutQueueName ),
						},
						//"type": &sqs.MessageAttributeValue{
						//	DataType:    aws.String("String"),
						//	StringValue: aws.String( "text" ),
						//},
					},
					MessageBody: aws.String( transformed ),
					QueueUrl:    outQueueUrl,
				})

				if err != nil {
					log.Fatal( err )
				}

				_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      inQueueUrl,
					ReceiptHandle: m.ReceiptHandle,
				})

				if err != nil {
					log.Fatal( err )
				}
			}

			log.Printf("Transformed and sent %d messages", len( result.Messages ) )

		} else {
			log.Printf("No messages received...")
		}
	}
}

func transform( transformName string, body string ) string {

	return body
}