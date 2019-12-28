package mcpa

import (
	"sync/atomic"
	"time"

	"github.com/lfsdong/conybymulticonpblack/amqp"
	"github.com/lfsdong/conybymulticonpblack/cony"
)

//新建client
func NewMqClient(addrs []string) *MqClient {
	mqCli := &MqClient{
		index:     0,
		cliSize:   uint32(len(addrs)),
		curClient: nil,
		errs:      make(chan MqError, 100*len(addrs)),
		blocking:  make(chan MqBlocking, 10*len(addrs)),
	}

	for _, addr := range addrs {
		curClient := &client{
			isUsable:   false,
			addr:       addr,
			client:     nil,
			publishers: make(map[int64]*publishInfo),
			consumers:  make(map[int64]*cony.Consumer),
		}

		curClient.client = cony.NewClient(
			cony.URL(addr),
			cony.Backoff(cony.DefaultBackoff),
		)

		mqCli.curClient = append(mqCli.curClient, curClient)

		//启动协程循环检测连接是否可用，并自动重连
		go func(mqCli *MqClient, curClient *client) {
			for curClient.client.Loop() {
				if curClient.client.IsUsable() {
					curClient.isUsable = true
				}

				select {
				case err := <-curClient.client.Errors():
					curClient.isUsable = false
					mqCli.errs <- MqError{
						Addr:  curClient.addr,
						Error: err,
					}
				case blocked := <-curClient.client.Blocking():
					mqCli.blocking <- MqBlocking{
						Addr:   curClient.addr,
						Active: blocked.Active,
						Reason: blocked.Reason,
					}
				}
			}
		}(mqCli, curClient)
	}
	return mqCli
}

func (mqCli *MqClient) Errors() <-chan MqError {
	return mqCli.errs
}

func (mqCli *MqClient) Blocking() <-chan MqBlocking {
	return mqCli.blocking
}

//添加生产者，（不支持动态添加）
func (mqCli *MqClient) AddPublisher(exc cony.Exchange, key string, nums int) (funcId int64, err error) {
	if nums <= 0 {
		return 0, NOT_RIGHT_NUMS_COROUTINE
	} else if funcId, err = genFuncId(); err != nil {
		return funcId, err
	}

	for _, cli := range mqCli.curClient {
		cli.mutex.Lock()
		defer cli.mutex.Unlock()

		cli.client.Declare([]cony.Declaration{
			cony.DeclareExchange(exc),
		})

		pbl := cony.NewPublisher(exc.Name, key, cony.SetReliable())
		cli.client.Publish(pbl)

		pblInfo := &publishInfo{
			pbl:       pbl,
			sequencer: make(map[uint64]callback),
		}
		cli.publishers[funcId] = pblInfo
	}

	for _, cli := range mqCli.curClient {
		for num := 1; num <= nums; num++ {
			go func(curClient *client, funcId int64) {
				for !curClient.publishers[funcId].pbl.GetReliable() || curClient.publishers[funcId].pbl.Confirms() == nil {
					time.Sleep(time.Second * 1)
					continue
				}

				for {
					if confi, ok := <-curClient.publishers[funcId].pbl.Confirms(); !ok {
						time.Sleep(time.Microsecond * 1)
						continue
					} else {
						curClient.publishers[funcId].mutex.Lock()
						cal, found := curClient.publishers[funcId].sequencer[confi.DeliveryTag]
						delete(curClient.publishers[funcId].sequencer, confi.DeliveryTag)
						curClient.publishers[funcId].mutex.Unlock()
						if found {
							cal.pblCallback(cal.sendBuf, confi)
						}
					}
				}
			}(cli, funcId)
		}
	}

	return funcId, err
}

//通过funcId推送消息，指定消息确认处理函数
func (mqCli *MqClient) Publish(sendBuf []byte, funcId int64, pblCallback PblCalllback) (err error) {
	nowIndex := atomic.AddUint32(&mqCli.index, 1) % mqCli.cliSize
	for uiNum := nowIndex; mqCli.cliSize >= 1 && ((uiNum%mqCli.cliSize) != nowIndex || uiNum < mqCli.cliSize); uiNum++ {
		if mqCli.curClient[(uiNum % mqCli.cliSize)].isUsable {
			if _, ok := mqCli.curClient[(uiNum % mqCli.cliSize)].publishers[funcId]; !ok {
				return ILLEGAL_FUNCTION_ID
			}

			if published, err := mqCli.curClient[(uiNum % mqCli.cliSize)].publishers[funcId].pbl.Publish(amqp.Publishing{
				Body:         sendBuf,
				DeliveryMode: amqp.Persistent,
			}); err == nil {
				if mqCli.curClient[(uiNum % mqCli.cliSize)].publishers[funcId].pbl.GetReliable() {
					mqCli.curClient[(uiNum % mqCli.cliSize)].publishers[funcId].mutex.Lock()
					mqCli.curClient[(uiNum % mqCli.cliSize)].publishers[funcId].sequencer[published] = callback{
						sendBuf:     sendBuf,
						pblCallback: pblCallback,
					}
					mqCli.curClient[(uiNum % mqCli.cliSize)].publishers[funcId].mutex.Unlock()
				}
			}
			return err
		}
	}

	return NO_CONNECTION_AVAILABLE
}

//添加消费者，指定消息处理函数（不支持动态添加）
func (mqCli *MqClient) AddConsumer(que cony.Queue, exc cony.Exchange, key string, nums int, cnsCallback CnsCallback) (funcId int64, err error) {
	if nums <= 0 {
		return 0, NOT_RIGHT_NUMS_COROUTINE
	} else if funcId, err = genFuncId(); err != nil {
		return funcId, err
	}

	for _, cli := range mqCli.curClient {
		cli.mutex.Lock()
		defer cli.mutex.Unlock()

		bind := cony.Binding{
			Queue:    &que,
			Exchange: exc,
			Key:      key,
		}

		cli.client.Declare([]cony.Declaration{
			cony.DeclareQueue(&que),
			cony.DeclareExchange(exc),
			cony.DeclareBinding(bind),
		})

		cns := cony.NewConsumer(&que)
		cli.client.Consume(cns)

		cli.consumers[funcId] = cns
	}

	for _, cli := range mqCli.curClient {
		for num := 1; num <= nums; num++ {
			go func(cns *cony.Consumer, cnsCallback CnsCallback) {
				for {
					select {
					case msg := <-cns.Deliveries():
						cnsCallback(msg, nil)
					case err := <-cns.Errors():
						cnsCallback(amqp.Delivery{}, err)
					}
				}
			}(cli.consumers[funcId], cnsCallback)
		}
	}

	return funcId, nil
}
