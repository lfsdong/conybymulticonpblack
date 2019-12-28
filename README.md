#项目概述

* 本项目用于rabbitMQ client，基于cony、amqp实现，cony项目参考：<https://github.com/assembla/cony>；amqp项目参考：<https://github.com/streadway/amqp>

* 项目实现了multi-mq连接，定时检测连接，针对连接断开等情况实现告警并自动重连；

* 生产者、消费者 msg ack机制基于callback回调实现，生产者、消费者初始化时可指定callback协程个数；

* 项目使用demo可参考 mcpa/examples目录的示例代码；
