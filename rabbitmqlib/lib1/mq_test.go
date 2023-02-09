/*
 * @Description: The program is written by the author, if modified at your own risk.
 * @Author: heyongliang
 * @Email: 356126067@qq.com
 * @Phone: 15215657185
 * @Date: 2020-11-20 15:50:08
 * @LastEditTime: 2023-02-09 09:44:55
 */
package lib1

import (
	"fmt"
	"testing"
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

func TestAbc(t *testing.T) {
	msg := fmt.Sprintf("这是测试任务")
	testPro := &TestPro{
		msg,
	}
	queueExchange := &QueueExchange{
		"test.queue",
		"test.key",
		"test.exchange",
		"direct",
	}
	var forever = make(chan bool)
	mq := New(queueExchange)
	mq.RegisterProducer(testPro)
	mq.RegisterReceiver(testPro)
	mq.Start()
	<-forever
}
