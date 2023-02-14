package mgrabbit

import (
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/maczh/jazz"
	"github.com/sadlil/gologger"
	"strings"
)

type rabbitmq struct {
	rabbit    *jazz.Connection //当前连接
	exchange  string           //当前交换机
	conf      *koanf.Koanf
	confUrl   string
	multi     bool                        //是否多库连接
	rabbits   map[string]*jazz.Connection //多连接
	exchanges map[string]string           //多连接交换机
	dsns      map[string]string           //连接地址
	tags      []string                    //多连接的连接名
}

var Rabbit = &rabbitmq{}
var logger = gologger.GetLogger()

func (r *rabbitmq) Init(rabbitConfigUrl string) {
	if rabbitConfigUrl != "" {
		r.confUrl = rabbitConfigUrl
	}
	if r.confUrl == "" {
		logger.Error("rabbit配置Url为空")
		return
	}
	var err error
	if r.rabbit == nil && len(r.rabbits) == 0 {
		if r.conf == nil {
			resp, err := grequests.Get(rabbitConfigUrl, nil)
			if err != nil {
				logger.Error("RabbitMQ配置下载失败! " + err.Error())
				return
			}
			r.conf = koanf.New(".")
			err = r.conf.Load(rawbytes.Provider([]byte(resp.String())), yaml.Parser())
			if err != nil {
				logger.Error("MongoDB配置解析错误:" + err.Error())
				r.conf = nil
				return
			}
		}
		r.rabbits = make(map[string]*jazz.Connection)
		r.exchanges = make(map[string]string)
		r.dsns = make(map[string]string)
		r.tags = make([]string, 0)
		r.multi = r.conf.Bool("go.rabbitmq.multi")
		if r.multi {
			r.tags = strings.Split(r.conf.String("go.rabbitmq.conns"), ",")
			for _, tag := range r.tags {
				r.dsns[tag] = r.conf.String(fmt.Sprintf("go.rabbitmq.%s.uri", tag))
				r.exchanges[tag] = r.conf.String(fmt.Sprintf("go.rabbitmq.%s.exchange", tag))
				r.rabbits[tag], err = jazz.Connect(r.dsns[tag])
				if err != nil {
					logger.Error(tag + " RabbitMQ connection failed: " + err.Error())
				} else {
					logger.Info(tag + " RabbitMQ connection succeeded")
				}
			}
			r.rabbit = r.rabbits[r.tags[0]]
			r.exchange = r.exchanges[r.tags[0]]
		} else {
			dsn := r.conf.String("go.rabbitmq.uri")
			r.rabbit, err = jazz.Connect(dsn)
			if err != nil {
				logger.Error("RabbitMQ连接错误:" + err.Error())
			} else {
				r.exchange = r.conf.String("go.rabbitmq.exchange")
			}
			r.rabbits["0"] = r.rabbit
			r.exchanges["0"] = r.exchange
			r.dsns["0"] = dsn
		}
	}
}

// GetConnection 多连接时获取指定标签的连接
// 说明: 仅在多连接时使用，切换当前连接为指定标签的连接，在下次GetConnection之前一直不变
func (r *rabbitmq) GetConnection(tag string) *rabbitmq {
	if !r.multi {
		return r
	}
	if _, ok := r.rabbits[tag]; !ok {
		logger.Error("RabbitMQ connection tag " + tag + " invalid")
		return r
	}
	r.rabbit = r.rabbits[tag]
	r.exchange = r.exchanges[tag]
	return r
}

func (r *rabbitmq) Close() {
	if r.rabbit != nil {
		r.rabbit.Close()
		r.rabbit = nil
	}
}

func (r *rabbitmq) RabbitSendMessage(queueName string, msg string) {
	err := r.rabbit.SendMessage(r.exchange, queueName, msg)
	if err != nil {
		logger.Error("RabbitMQ发送消息错误:" + err.Error())
	}
}

func (r *rabbitmq) RabbitMessageListener(queueName string, listener func(msg []byte)) {
	//侦听之前先创建队列
	r.RabbitCreateNewQueue(queueName)
	//启动侦听消息处理线程
	go r.rabbit.ProcessQueue(queueName, listener)
}

func (r *rabbitmq) RabbitCreateNewQueue(queueName string) {
	queues := make(map[string]jazz.QueueSpec)
	binding := &jazz.Binding{
		Exchange: r.exchange,
		Key:      queueName,
	}
	queueSpec := &jazz.QueueSpec{
		Durable:  true,
		Bindings: []jazz.Binding{*binding},
		Args:     nil,
	}
	queues[queueName] = *queueSpec
	setting := &jazz.Settings{
		Queues: queues,
	}
	err := r.rabbit.CreateScheme(*setting)
	if err != nil {
		logger.Error("RabbitMQ创建队列失败:" + err.Error())
	}
}

func (r *rabbitmq) RabbitCreateDeadLetterQueue(queueName, toQueueName string, ttl int) {
	queues := make(map[string]jazz.QueueSpec)
	binding := &jazz.Binding{
		Exchange: r.exchange,
		Key:      queueName,
	}
	queueSpec := &jazz.QueueSpec{
		Durable:  true,
		Bindings: []jazz.Binding{*binding},
		Args:     jazz.DeadLetterArgs(ttl, r.exchange, toQueueName),
	}
	queues[queueName] = *queueSpec
	setting := &jazz.Settings{
		Queues: queues,
	}
	err := r.rabbit.CreateScheme(*setting)
	if err != nil {
		logger.Error("RabbitMQ创建死信队列失败:" + err.Error())
	}
}
