package main

import (
	"fmt"
	"rabbitmq-test/rabbitmqlib"
)

// 网页浏览 http://localhost:15672/

// TestPro ...
type TestPro struct {
	msgContent string
}

// MsgContent 实现发送者
func (t *TestPro) MsgContent() string {
	return t.msgContent
}

// Consumer 实现接收者
func (t *TestPro) Consumer(dataByte []byte) error {
	fmt.Println(string(dataByte))
	return nil
}

// 报错，没解决
func main() {
	msg := fmt.Sprintf("这是测试任务")
	t := &TestPro{
		msg,
	}

	queueExchange := &rabbitmqlib.QueueExchange{
		"test.rabbit",
		"rabbit.key",
		"test.rabbit.mq",
		"direct",
	}
	mq := rabbitmqlib.New(queueExchange)
	mq.RegisterProducer(t)
	mq.RegisterReceiver(t)
	mq.Start()
}
