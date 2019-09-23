package rpc_client

import (
	"encoding/json"
	"github.com/getmorebrasil/amqp-connection/connection"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
)

var conn *amqp.Connection
var ch *amqp.Channel

func ConnectRabbitMQ(connString string) {
	newConn, newChannel := amqp_connection.Connect(connString)
	conn = newConn
	ch = newChannel
}

func CreateQueue(name string, durable bool, del_when_unused bool, exclusive bool, noWait bool, arguments amqp.Table, channel *amqp.Channel) (q amqp.Queue) {
	q = amqp_connection.CreateQueue(name, durable, del_when_unused, exclusive, noWait, arguments, ch)
	return q
}

func ConsumeQueue(name string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table, ch *amqp.Channel) (messages <-chan amqp.Delivery) {
	messages = amqp_connection.ConsumeQueue(name, autoAck, exclusive, noLocal, noWait, args, ch)
	return messages
}

func PublishQueue(exchange string, routingKey string, mandatory bool, immediate bool, options amqp.Publishing, ch *amqp.Channel) {
	amqp_connection.PublishQueue(exchange, routingKey, mandatory, immediate, options, ch)
}

type callParams struct {
	Target string      `json:"target"`
	Params interface{} `json:"params"`
}
type autocompleteParams struct {
	Word string `json:"word"`
}

func RPCCall(Target string, params interface{}, exchange string, routingKey string, mandatory bool, immediate bool) []byte {
	ch, err := conn.Channel()

	msgToSent := callParams{
		Target: Target,
		Params: params,
	}
	data, err := json.Marshal(msgToSent)

	failOnError(err, "")
	q := CreateQueue("", false, false, true, false, nil, ch)
	msgs := ConsumeQueue(q.Name, false, false, false, false, nil, ch)

	corrId := randomString(32)
	var options amqp.Publishing
	options.CorrelationId = corrId
	options.Body = data
	options.ReplyTo = q.Name
	PublishQueue(exchange, routingKey, mandatory, immediate, options, ch)
	var response []byte
	for d := range msgs {
		if corrId == d.CorrelationId {
			response = d.Body
			break
		}
	}
	return response
}

func failOnError(err error, msg string) {

	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
