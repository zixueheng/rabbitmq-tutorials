## 实现延时队列的基本要素

1. 存在一个倒计时机制:Time To Live(TTL)
2. 当到达时间点的时候会触发一个发送消息的事件:Dead Letter Exchanges（DLX）
    - 基于第一点,我利用的是消息存在过期时间这一特性, 消息一旦过期就会变成dead letter,可以让单独的消息过期,也可以设置整个队列消息的过期时间 而rabbitmq会有限取两个值的最小
    - 基于第二点,是用到了rabbitmq的过期消息处理机制: . x-dead-letter-exchange 将过期的消息发送到指定的 exchange 中 . x-dead-letter-routing-key 将过期的消息发送到自定的 route当中

在这里例子当中,我使用的是 过期消息+转发指定exchange