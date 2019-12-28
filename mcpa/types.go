package mcpa

import (
	"errors"
	"sync"

	"github.com/lfsdong/conybymulticonpblack/amqp"
	"github.com/lfsdong/conybymulticonpblack/cony"
)

var (
	NOT_RIGHT_NUMS_COROUTINE = errors.New("coroutine nums must gt 1")
	CLOCK_CALLBACK_ERROR     = errors.New("clock callback error")
	NO_CONNECTION_AVAILABLE  = errors.New("no connection available")
	ILLEGAL_FUNCTION_ID      = errors.New("illegal function id")
)

type PblCalllback func(sendBuf []byte, confi amqp.Confirmation)

type CnsCallback func(msg amqp.Delivery, err error)

type MqError struct {
	Addr  string //client addr
	Error error  //connection error
}

type MqBlocking struct {
	Addr   string //client addr
	Active bool   // TCP pushback active/inactive on server
	Reason string // Server reason for activation
}

type callback struct {
	sendBuf     []byte
	pblCallback PblCalllback
}

type publishInfo struct {
	pbl       *cony.Publisher
	mutex     sync.Mutex
	sequencer map[uint64]callback
}

type client struct {
	mutex      sync.Mutex
	isUsable   bool
	addr       string
	client     *cony.Client
	publishers map[int64]*publishInfo
	consumers  map[int64]*cony.Consumer
}

type MqClient struct {
	index     uint32
	cliSize   uint32
	curClient []*client
	errs      chan MqError
	blocking  chan MqBlocking
}
