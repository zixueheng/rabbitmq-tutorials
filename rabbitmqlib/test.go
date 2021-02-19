package rabbitmqlib

import (
	"fmt"
)

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

func test() {
	msg := fmt.Sprintf("这是测试任务")
	t := &TestPro{
		msg,
	}
	queueExchange := &QueueExchange{
		"test.rabbit",
		"rabbit.key",
		"test.rabbit.mq",
		"direct",
	}
	mq := New(queueExchange)
	mq.RegisterProducer(t)
	mq.RegisterReceiver(t)
	mq.Start()
}
