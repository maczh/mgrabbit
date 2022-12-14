package mgrabbit

import (
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/maczh/jazz"
	"github.com/sadlil/gologger"
)

type rabbitmq struct {
	rabbit   *jazz.Connection
	exchange string
	conf       *koanf.Koanf
	confUrl    string
}

var Rabbit = &rabbitmq{}
var logger = gologger.GetLogger()

func (r *rabbitmq)Init(rabbitConfigUrl string) {
	if rabbitConfigUrl != "" {
		r.confUrl = rabbitConfigUrl
	}
	if r.confUrl == "" {
		logger.Error("rabbit配置Url为空")
		return
	}
	var err error
	if r.rabbit == nil {
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
		dsn := r.conf.String("go.rabbitmq.uri")
		r.rabbit, err = jazz.Connect(dsn)
		if err != nil {
			logger.Error("RabbitMQ连接错误:" + err.Error())
		} else {
			r.exchange = r.conf.String("go.rabbitmq.exchange")
		}
	}
}

func (r *rabbitmq)Close() {
	if r.rabbit != nil {
		r.rabbit.Close()
		r.rabbit = nil
	}
}

func (r *rabbitmq)RabbitSendMessage(queueName string, msg string) {
	err := r.rabbit.SendMessage(r.exchange, queueName, msg)
	if err != nil {
		logger.Error("RabbitMQ发送消息错误:" + err.Error())
	}
}

func (r *rabbitmq)RabbitMessageListener(queueName string, listener func(msg []byte)) {
	//侦听之前先创建队列
	r.RabbitCreateNewQueue(queueName)
	//启动侦听消息处理线程
	go r.rabbit.ProcessQueue(queueName, listener)
}

func (r *rabbitmq)RabbitCreateNewQueue(queueName string) {
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

func (r *rabbitmq)RabbitCreateDeadLetterQueue(queueName, toQueueName string, ttl int) {
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
