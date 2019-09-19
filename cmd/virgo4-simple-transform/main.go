package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"github.com/antchfx/xmlquery"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle( cfg.InQueueName )
	if err != nil {
		log.Fatal( err )
	}

	outQueueHandle, err := aws.QueueHandle( cfg.OutQueueName )
	if err != nil {
		log.Fatal( err )
	}

	for {

		//log.Printf("Waiting for messages...")
		start := time.Now()

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet( inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, time.Duration( cfg.PollTimeOut ) * time.Second )
		if err != nil {
			log.Fatal( err )
		}

		// did we receive any?
		sz := len( messages )
		if sz != 0 {

			//log.Printf("Received %d messages", len( result.Messages ) )

			for _, m := range messages {

				// apply our transform
				transformed, err := transform( cfg.TransformName, m.Payload )
				if err == nil {
                   m.Payload = transformed
				} else {
					log.Printf("WARNING: transform error (%s), message unchanged", err )
				}
			}

			opStatus, err := aws.BatchMessagePut( outQueueHandle, messages )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to send to outbound queue", ix )
				}
			}

			// delete them all anyway
			opStatus, err = aws.BatchMessageDelete( inQueueHandle, messages )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to delete", ix )
				}
			}

			duration := time.Since(start)
			log.Printf("Transformed and sent %d records (%0.2f tps)", sz, float64( sz ) / duration.Seconds() )

		} else {
			log.Printf("No messages received...")
		}
	}
}

func transform( transformName string, body awssqs.Payload ) ( awssqs.Payload, error ) {

	// parse the XML
	_, err := xmlquery.Parse( strings.NewReader( string( body ) ) )
	return body, err
}

//
// end of file
//