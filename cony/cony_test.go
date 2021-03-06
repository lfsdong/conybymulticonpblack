package cony

import "github.com/lfsdong/conybymulticonpblack/amqp"

type mqDeleterTest struct {
	_deletePublisher func(*Publisher)
	_deleteConsumer  func(*Consumer)
}

func (m *mqDeleterTest) deletePublisher(p *Publisher) {
	m._deletePublisher(p)
}

func (m *mqDeleterTest) deleteConsumer(c *Consumer) {
	m._deleteConsumer(c)
}

type mqChannelTest struct {
	_Close         func() error
	_Consume       func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	_NotifyClose   func(chan *amqp.Error) chan *amqp.Error
	_Confirm       func(noWait bool) error
	_NotifyPublish func(chan amqp.Confirmation) chan amqp.Confirmation
	_Publish       func(string, string, bool, bool, amqp.Publishing) (uint64, error)
	_Qos           func(int, int, bool) error
}

func (m *mqChannelTest) Close() error {
	return m._Close()
}

func (m *mqChannelTest) Consume(name string, tag string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return m._Consume(name, tag, autoAck, exclusive, noLocal, noWait, args)
}

func (m *mqChannelTest) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return m._NotifyClose(c)
}

func (m *mqChannelTest) Confirm(noWait bool) error {
	return m._Confirm(noWait)
}

func (m *mqChannelTest) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	return m._NotifyPublish(c)
}

func (m *mqChannelTest) Publish(exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) (uint64, error) {
	return m._Publish(exchange, key, mandatory, immediate, msg)
}

func (m *mqChannelTest) Qos(prefetchCount int, prefetchSize int, global bool) error {
	return m._Qos(prefetchCount, prefetchSize, global)
}
