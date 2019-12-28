package cony

import (
	"errors"
	"sync"

	"github.com/lfsdong/conybymulticonpblack/amqp"
)

// ErrPublisherDead indicates that publisher was canceled, could be returned
// from Write() and Publish() methods
var ErrPublisherDead = errors.New("Publisher is dead")

// PublisherOpt is a functional option type for Publisher
type PublisherOpt func(*Publisher)

type publishMaybeErr struct {
	pub  chan amqp.Publishing
	info chan struct {
		published uint64
		err       error
	}
	key string
}

// Publisher hold definition for AMQP publishing
type Publisher struct {
	exchange string
	key      string
	tmpl     amqp.Publishing
	pubChan  chan publishMaybeErr
	stop     chan struct{}
	dead     bool
	m        sync.Mutex

	//add by lfsdong
	isReliable     bool
	reallyReliable bool
	confirms       chan amqp.Confirmation
}

//获取生产者 确认消息，add by lfswang
func (p *Publisher) Confirms() <-chan amqp.Confirmation {
	return p.confirms
}

// Template will be used, input buffer will be added as Publishing.Body.
// return int will always be len(b)
//
// Implements io.Writer
//
// WARNING: this is blocking call, it will not return until connection is
// available. The only way to stop it is to use Cancel() method.
func (p *Publisher) Write(b []byte) (int, error) {
	pub := p.tmpl
	pub.Body = b
	_, err := p.Publish(pub)
	return len(b), err
}

// PublishWithRoutingKey used to publish custom amqp.Publishing and routing key
//
// WARNING: this is blocking call, it will not return until connection is
// available. The only way to stop it is to use Cancel() method.
func (p *Publisher) PublishWithRoutingKey(pub amqp.Publishing, key string) (uint64, error) {
	reqRepl := publishMaybeErr{
		pub: make(chan amqp.Publishing, 2),
		info: make(chan struct {
			published uint64
			err       error
		}, 2),
		key: key,
	}

	reqRepl.pub <- pub

	select {
	case <-p.stop:
		// received stop signal
		return 0, ErrPublisherDead
	case p.pubChan <- reqRepl:
	}

	info := <-reqRepl.info

	close(reqRepl.pub)
	close(reqRepl.info)

	return info.published, info.err
}

// Publish used to publish custom amqp.Publishing
//
// WARNING: this is blocking call, it will not return until connection is
// available. The only way to stop it is to use Cancel() method.
func (p *Publisher) Publish(pub amqp.Publishing) (uint64, error) {
	return p.PublishWithRoutingKey(pub, p.key)
}

// Cancel this publisher
func (p *Publisher) Cancel() {
	p.m.Lock()
	defer p.m.Unlock()

	if !p.dead {
		close(p.stop)
		p.dead = true
	}
}

//判断生产者 确认机制 是否可用，add by lfswang
func (p *Publisher) GetReliable() bool {
	return p.reallyReliable
}

func (p *Publisher) serve(client mqDeleter, ch mqChannel) {
	chanErrs := make(chan *amqp.Error)
	ch.NotifyClose(chanErrs)

	//add by lfsdong
	if p.isReliable {
		if err := ch.Confirm(false); err != nil {
			p.reallyReliable = false
		} else {
			p.reallyReliable = true
			p.confirms = make(chan amqp.Confirmation)
			ch.NotifyPublish(p.confirms)
		}
	}

	for {
		select {
		case <-p.stop:
			client.deletePublisher(p)
			ch.Close()
			return
		case <-chanErrs:
			return
		case envelop := <-p.pubChan:
			msg := <-envelop.pub
			published, err := ch.Publish(
				p.exchange,  // exchange
				envelop.key, // key
				false,       // mandatory
				false,       // immediate
				msg,         // msg amqp.Publishing
			)
			envelop.info <- struct {
				published uint64
				err       error
			}{published: published, err: err}
		}
	}
}

// NewPublisher is a Publisher constructor
func NewPublisher(exchange string, key string, opts ...PublisherOpt) *Publisher {
	p := &Publisher{
		exchange:       exchange,
		key:            key,
		pubChan:        make(chan publishMaybeErr),
		stop:           make(chan struct{}),
		isReliable:     false,
		reallyReliable: false,
		confirms:       nil,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// PublishingTemplate Publisher's functional option. Provide template
// amqp.Publishing and save typing.
func PublishingTemplate(t amqp.Publishing) PublisherOpt {
	return func(p *Publisher) {
		p.tmpl = t
	}
}

//是否采用生产者 确认机制， add by lfsdong
func SetReliable() PublisherOpt {
	return func(p *Publisher) {
		p.isReliable = true
	}
}
