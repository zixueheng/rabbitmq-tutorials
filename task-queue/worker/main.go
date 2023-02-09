package main

import (
	"bytes"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// 消费者 处理任务，可以开启多个消费者同时等待一个队列
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable 队列持久化
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Qos控制服务器在接收传递确认之前，将尝试在网络上为消费者保留多少消息或多少字节。Qos的目的是确保服务器和客户端之间的网络缓冲区保持满。
	// 要在不同连接上从同一队列消费的消费者之间获得循环行为，请将预取计数设置为1，服务器上的下一条可用消息将传递给下一个可用的使用者。
	// 设置预取计数值为1，告诉RabbitMQ一次只向一个worker发送一条消息。换句话说，在处理并确认前一个消息之前，不要向工作进程发送新消息。
	err = ch.Qos(
		1,     // prefetch count 当预取计数大于零时，服务器将在收到确认之前将这些消息传递给消费者。当使用者使用noAck启动时，服务器忽略此选项，因为不需要或不发送确认。
		0,     // prefetch size 当预取大小大于零时，服务器将尝试在接收来自消费者的确认之前，将至少保持多个字节的传递刷新到网络。当消费者使用noAck启动时，将忽略此选项。
		false, // global 当global为true时，这些Qos设置将应用于同一连接上所有通道上的所有现有和未来使用者。如果为false，则Channel.Qos设置将应用于此通道上的所有现有和将来的使用者。
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack // 这里设置不自动确认 需要消费者处理完消息后 手动发送确认给MQ
		false,  // exclusive 是否排他，这里多个消费者会共享一个队列
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount) // 消息体中每一个点号（.）模拟1秒钟的操作
			time.Sleep(t * time.Second)
			log.Printf("Done")
			d.Ack(false) // 任务完成，使用d.Ack（false）向RabbitMQ服务器发送消费完成的确认（这个确认消息是单次传递的）
			// 杀掉了一个工作者（worker）进程，消息也不会丢失。当工作者（worker）挂掉这后，所有没有响应的消息都会重新发送。
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
