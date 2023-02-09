package main

import (
	"fmt"
	"log"
	"rabbitmq-test/rabbitmqlib/lib2"
	"time"
)

// 消费者
func main() {
	rabbit := lib2.NewRabbitMQ("yoyo_exchange", "yoyo_route", "yoyo_queue")
	defer rabbit.Close()

	for i := 1; i <= 3; i++ {
		go func(i int) {
			rabbit.SendMessage(lib2.Message{Body: fmt.Sprintf("普通消息%d", i)})
			rabbit.SendDelayMessage(lib2.Message{Body: fmt.Sprintf("延时5秒的消息%d", i), DelayTime: 5})
		}(i)
	}

	time.Sleep(time.Second * 3)
	log.Println("3秒后")

	rabbit.SendMessage(lib2.Message{Body: "普通消息4"})
	rabbit.SendDelayMessage(lib2.Message{Body: "延时5秒的消息4", DelayTime: 5})

}
