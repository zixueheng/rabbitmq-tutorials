package main

import (
	"log"
	"strconv"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// 斐波那契数列
func fib(n int) int {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	} else {
		return fib(n-1) + fib(n-2)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// rpc_queue 队列 存储客服端请求参数
	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 设置预取计数值为1，告诉RabbitMQ一次只向一个worker发送一条消息。换句话说，在处理并确认前一个消息之前，不要向工作进程发送新消息。
	err = ch.Qos(
		1,     // prefetch count 当预取计数大于零时，服务器将在收到确认之前将这些消息传递给消费者。当使用者使用noAck启动时，服务器忽略此选项，因为不需要或不发送确认。
		0,     // prefetch size 当预取大小大于零时，服务器将尝试在接收来自消费者的确认之前，将至少保持多个字节的传递刷新到网络。当消费者使用noAck启动时，将忽略此选项。
		false, // global 当global为true时，这些Qos设置将应用于同一连接上所有通道上的所有现有和未来使用者。如果为false，则Channel.Qos设置将应用于此频道上的所有现有和将来的使用者。
	)
	failOnError(err, "Failed to set QoS")

	// rpc_queue 队列 的消费者
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack 不自动确认消息
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			n, err := strconv.Atoi(string(d.Body)) // 客服端发过来的请求参数
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [.] fib(%d)", n)
			response := fib(n) // 模拟计算得出结果

			// 将计算结果响应给 d.ReplyTo 即客户端创建的回调队列
			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId, // 带上客户端请求的ID
					Body:          []byte(strconv.Itoa(response)),
				})
			failOnError(err, "Failed to publish a message")

			d.Ack(false) // 手动确认消息
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}
