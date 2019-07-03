package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/streadway/amqp"
)

type MessageQueue struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Consumers  []Consumer
	Prefetch   int
}

type Consumer struct {
	ConsumeFromQ string
	FailQueue    string
	Work         func([]byte) error
	ID           int
}

var messageQueue *MessageQueue

func GetMessageQueue() *MessageQueue {
	return messageQueue
}

func (mq *MessageQueue) SetMessageQueue() *MessageQueue {
	messageQueue = mq
	return messageQueue
}

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

func (mq *MessageQueue) Copy() *MessageQueue {
	newConnection := MessageQueue{
		Connection: mq.Connection,
		Channel:    mq.Channel,
	}
	return &newConnection
}

func (mq *MessageQueue) Close() error {
	mq.Channel.Close()
	return mq.Connection.Close()
}

func (mq *MessageQueue) NewChannel() (*amqp.Channel, error) {
	ch, err := mq.Connection.Channel()
	if err != nil {
		return nil, err
	}
	ch.Qos(mq.Prefetch, 0, false)
	return ch, nil
}

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

// Publish messages
func (mq *MessageQueue) Publish(routingKey, exchange string, message interface{}) error {
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
			ContentType: "application/json",
			Body:        body,
		})

}

func (mq *MessageQueue) Consume() {
	defer mq.Close()
	for _, cons := range mq.Consumers {
		msgs, err := mq.Channel.Consume(
			cons.ConsumeFromQ,          // queue
			fmt.Sprintf("%d", cons.ID), // consumer
			false,                      // auto-ack
			false,                      // exclusive
			false,                      // no-local
			false,                      // no-wait
			nil,                        // args
		)
		if err != nil {
			log.Println(err)
			return
		}
		go func(cons Consumer) {
			for d := range msgs {
				if err := cons.Work(d.Body); err != nil {
					body := make(map[string]interface{})
					if err := json.Unmarshal(d.Body, &body); err != nil {
						log.Println(err)
						if err := mq.Publish(cons.FailQueue, "", d.Body); err != nil {
							log.Println(err)
						}
						d.Ack(false)
					}
					body["error_message"] = err.Error()
					if err := mq.Publish(cons.FailQueue, "", body); err != nil {
						log.Println(err)
					}
				}
				d.Ack(false)
			}
		}(cons)
	}
	forever := make(chan bool)
	<-forever
}

func (mq *MessageQueue) NewConsumer(id int, consumeQ, failQ string, work func([]byte) error) {
	mq.Consumers = append(mq.Consumers, Consumer{
		ID:           id,
		ConsumeFromQ: consumeQ,
		FailQueue:    failQ,
		Work:         work,
	})
}
