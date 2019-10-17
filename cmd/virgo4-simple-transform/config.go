package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName   string
	OutQueueName  string
	TransformName string
	PollTimeOut   int64
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")
	var cfg ServiceConfig
	flag.StringVar(&cfg.InQueueName, "inqueue", "", "Inbound queue name")
	flag.StringVar(&cfg.OutQueueName, "outqueue", "", "Outbound queue name")
	flag.StringVar(&cfg.TransformName, "xform", "", "The transform to apply")
	flag.Int64Var(&cfg.PollTimeOut, "pollwait", 15, "Poll wait time (in seconds)")

	flag.Parse()

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName)
	log.Printf("[CONFIG] TransformName        = [%s]", cfg.TransformName)
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut)

	return &cfg
}

//
// end of file
//
