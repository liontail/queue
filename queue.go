package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/streadway/amqp"
)

// MessageQueue is a struct that can be create by NewConnectionWithQueue
type MessageQueue struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Consumers  []Consumer
	Prefetch   int
}

// Consumer is a struct that represents consumer of the queue
type Consumer struct {
	ConsumeFromQ string
	FailExchange string
	FailQueue    string
	Work         func([]byte) error
	ID           int
}

var messageQueue *MessageQueue

// GetMessageQueue is a function that get singleton queue
func GetMessageQueue() *MessageQueue {
	return messageQueue
}

// SetSingletonMessageQueue is a function that set queue to singleton
func (mq *MessageQueue) SetSingletonMessageQueue() *MessageQueue {
	messageQueue = mq
	return messageQueue
}

// DeclareExchange is a function that create a exchange name with kind
// if there is empty kind the default will be topic
func (mq *MessageQueue) DeclareExchange(exchangeName, kind string) error {
	if kind == "" {
		kind = "topic"
	}
	ch, _ := mq.NewChannel()
	defer ch.Close()
	return ch.ExchangeDeclare(
		exchangeName,
		kind,
		true,
		false,
		false,
		false,
		nil,
	)
}

// BindExchangeQueue is a function that bind a exchange and queue with routing key
// exchangeName represents exchange name, routingKey represents routing key, and queueName represents name of the queue
func (mq *MessageQueue) BindExchangeQueue(exchangeName, routingKey, queueName string) error {
	ch, _ := mq.NewChannel()
	defer ch.Close()
	return ch.QueueBind(queueName, routingKey, exchangeName, false, nil)
}

// NewConnectionWithQueue will create a connection with queue and prefetch
// connectionStr represents connection url ex. guest:guest@localhost ( it will automatic concat protocal amqp:// ),
// queueName represents name of the queue that wants to declare,
// prefetch represents number of prefetch from queue
func NewConnectionWithQueue(connectionStr, queueName string, prefetch int) (*MessageQueue, error) {
	if strings.Index(connectionStr, "amqp://") != 0 {
		connectionStr = fmt.Sprintf("amqp://%s", connectionStr)
	}
	conn, err := amqp.Dial(connectionStr)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	ch.Qos(prefetch, 0, false)
	if queueName == "" {
		return nil, errors.New("queue name is empty")
	}
	if _, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	); err != nil {
		return nil, err
	}

	return &MessageQueue{Connection: conn, Channel: ch, Prefetch: prefetch}, nil
}

func DeclareQueue(queueName string) error {
	ch := messageQueue.Channel
	if _, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	); err != nil {
		return err
	}
	return nil
}

// Copy is a function that return a copy of MessageQueue
func (mq *MessageQueue) Copy() *MessageQueue {
	newConnection := MessageQueue{
		Connection: mq.Connection,
		Channel:    mq.Channel,
	}
	return &newConnection
}

// Close will close both channel and connection
func (mq *MessageQueue) Close() error {
	mq.Channel.Close()
	return mq.Connection.Close()
}

// NewChannel will return a new channel with the prefetch
func (mq *MessageQueue) NewChannel() (*amqp.Channel, error) {
	ch, err := mq.Connection.Channel()
	if err != nil {
		return nil, err
	}
	ch.Qos(mq.Prefetch, 0, false)
	return ch, nil
}

// SetQueue is a function that Declare a queue by queueName
func (mq *MessageQueue) SetQueue(queueName string) error {
	if mq.Channel == nil {
		newCH, err := mq.NewChannel()
		if err != nil {
			return err
		}
		mq.Channel = newCH
	}
	mq.Channel.Qos(mq.Prefetch, 0, false)
	if _, err := mq.Channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	); err != nil {
		return err
	}

	return nil
}

// Get the byte array for the interface
func getBytes(key interface{}) ([]byte, error) {
	return json.Marshal(&key)
}

// Publish messages to queue
// routingKey represents routing key, can use routingKey as a queue name
// exchange represents exchange name, can left empty if there is none
// message represents data that wants to put into queue
// contentType represents type of data in the queue
func (mq *MessageQueue) Publish(routingKey, exchange string, message interface{}, contentType string) error {
	var body []byte
	if reflect.TypeOf(message) == reflect.TypeOf([]byte{}) {
		body = message.([]byte)
	} else {
		newBody, err := getBytes(message)
		if err != nil {
			return err
		}
		body = newBody

	}
	ch := mq.Channel
	if ch == nil {
		newCh, err := mq.NewChannel()
		if err != nil {
			return err
		}
		ch = newCh
	}
	return ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		})

}

// Consume is a function that start to process consuming
// by using Consumer in MessageQueue
func (mq *MessageQueue) Consume() {
	defer mq.Close()
	for _, cons := range mq.Consumers {
		ch, _ := mq.NewChannel()
		con := cons
		go func(con *Consumer) {
			msgs, err := ch.Consume(
				con.ConsumeFromQ,          // queue
				fmt.Sprintf("%d", con.ID), // consumer
				false,                     // auto-ack
				false,                     // exclusive
				false,                     // no-local
				false,                     // no-wait
				nil,                       // args
			)
			if err != nil {
				log.Println(err)
				return
			}
			for d := range msgs {
				if err := con.Work(d.Body); err != nil {
					body := make(map[string]interface{})
					if err := json.Unmarshal(d.Body, &body); err != nil {
						log.Println(err)
						if err := mq.Publish(con.FailQueue, con.FailExchange, d.Body, "application/json"); err != nil {
							log.Println(err)
						}
					} else {
						body["error_message"] = err.Error()
						if err := mq.Publish(con.FailQueue, con.FailExchange, body, "application/json"); err != nil {
							log.Println(err)
						}
					}
					d.Ack(false)
				}
				d.Ack(false)
			}
			<-make(chan bool)
		}(&con)
	}
	<-make(chan bool)
}

// NewConsumer is a function that return Consumer
// id represents tag of comsumer, consumerQ represents name of the queue that wants to consume,
// failQ represents name of the queue that wants to send unprocessable data to,
// work represents function that wants to excute on consuming
func (mq *MessageQueue) NewConsumer(id int, consumeQ, failEx, failQ string, work func([]byte) error) error {
	mq.Consumers = append(mq.Consumers, Consumer{
		ID:           id,
		ConsumeFromQ: consumeQ,
		FailExchange: failEx,
		FailQueue:    failQ,
		Work:         work,
	})
	return nil
}
