package main

import (
	"flag"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/lfsdong/conybymulticonpblack/amqp"
	"github.com/lfsdong/conybymulticonpblack/cony"
	"github.com/lfsdong/conybymulticonpblack/mcpa"
)

var (
	uri = []string{*flag.String("uri_1", "amqp://user:passwd@127.0.0.1:5672/", "AMQP URI"),
		*flag.String("uri_2", "amqp://user:passwd@127.0.0.1:5673/", "AMQP URI"),
		*flag.String("uri_3", "amqp://user:passwd@127.0.0.1:5674/", "AMQP URI")}
	exchangeName = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	body         = flag.String("body", "foobar", "Body of message")
)

func pblCallback(sendBuf []byte, confi amqp.Confirmation) {
	log.Printf("pblCallback sendBuf:%s \t confi %v \n\n", string(sendBuf), confi)
}

func producer(myMqClient *mcpa.MqClient) {
	exc := cony.Exchange{
		Name:       *exchangeName,
		Kind:       *exchangeType,
		AutoDelete: false,
		Durable:    true,
	}

	pblFuncId, err := myMqClient.AddPublisher(exc, "", 10)
	log.Printf("producer pblFuncId:%d \t err:%v", pblFuncId, err)
	time.Sleep(time.Second * 3)
	if err != nil {
		log.Printf("producer AddPublisher err:%v", err)
	} else {
		for num := 1; num <= 10; num++ {
			go func() {
				for cnt := 1; cnt <= 10000000; cnt++ {
					sendBuf := *body + "_" + strconv.Itoa(cnt)
					if err := myMqClient.Publish([]byte(sendBuf), pblFuncId, pblCallback); err != nil {
						log.Printf("producer Publish err %v", err)
					}
				}
			}()
		}
	}
}

func cnsCallback(msg amqp.Delivery, err error) {
	log.Printf("cnsCallback msg %s \t err %v", string(msg.Body), err)
	// msg.Ack(false) msg.Reject(true)
	msg.Ack(false)
}

func consumer(myMqClient *mcpa.MqClient) {
	que := cony.Queue{
		Name: "myRabbitMq",
	}

	exc := cony.Exchange{
		Name:       *exchangeName,
		Kind:       *exchangeType,
		AutoDelete: false,
		Durable:    true,
	}

	cnsFuncId, err := myMqClient.AddConsumer(que, exc, "", 10, cnsCallback)
	log.Printf("consumer cnsFuncId:%d \t err:%v", cnsFuncId, err)
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	flag.Parse()

	myMqClient := mcpa.NewMqClient(uri)

	go func() {
		select {
		case err := <-myMqClient.Errors():
			log.Printf("main err:%v", err)
		case blocked := <-myMqClient.Blocking():
			log.Printf("main blocked:%v", blocked)
		}
	}()

	go producer(myMqClient)
	for num := 1; num <= 1; num++ {
		go consumer(myMqClient)
	}

	wg.Wait()
}
