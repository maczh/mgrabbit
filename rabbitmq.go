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
	conf        *koanf.Koanf
	confUrl     string
	multi       bool                   //是否多库连接
	connections map[string]*connection //多连接
	tags        []string               //多连接的连接名
}

type connection struct {
	conn     *jazz.Connection
	exchange string
	dsn      string
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
	if r.connections == nil || len(r.connections) == 0 {
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
		r.connections = make(map[string]*connection)
		r.tags = make([]string, 0)
		r.multi = r.conf.Bool("go.rabbitmq.multi")
		if r.multi {
			tags := strings.Split(r.conf.String("go.rabbitmq.conns"), ",")
			for _, tag := range tags {
				dsn := r.conf.String(fmt.Sprintf("go.rabbitmq.%s.uri", tag))
				conn, err := jazz.Connect(dsn)
				if err != nil {
					logger.Error(tag + " RabbitMQ connection failed: " + err.Error())
				} else {
					logger.Info(tag + " RabbitMQ connection succeeded")
					r.connections[tag] = &connection{
						conn:     conn,
						exchange: r.conf.String(fmt.Sprintf("go.rabbitmq.%s.exchange", tag)),
						dsn:      dsn,
					}
					r.tags = append(r.tags, tag)
				}
			}
		} else {
			dsn := r.conf.String("go.rabbitmq.uri")
			conn, err := jazz.Connect(dsn)
			if err != nil {
				logger.Error("RabbitMQ连接错误:" + err.Error())
			} else {
				r.connections["0"] = &connection{
					conn:     conn,
					exchange: r.conf.String("go.rabbitmq.exchange"),
					dsn:      dsn,
				}
			}
		}
	}
}

// GetConnection 多连接时获取指定标签的连接
func (r *rabbitmq) GetConnection(tag ...string) (*connection, error) {
	if !r.multi {
		return r.connections["0"], nil
	}
	if len(tag) == 0 || tag[0] == "" {
		return nil, fmt.Errorf("RabbitMQ Multi connection need a tag to get connection")
	}
	if _, ok := r.connections[tag[0]]; !ok {
		logger.Error("RabbitMQ connection tag " + tag[0] + " invalid")
		return nil, fmt.Errorf("RabbitMQ connection tag %s invalid", tag[0])
	}
	return r.connections[tag[0]], nil
}

func (r *rabbitmq) Close() {
	if !r.multi {
		r.connections["0"].conn.Close()
		delete(r.connections, "0")
	} else {
		for tag, _ := range r.connections {
			r.connections[tag].conn.Close()
			delete(r.connections, tag)
		}
	}

}

// RabbitSendMessage 向指定队列发送消息
func (r *connection) RabbitSendMessage(queueName string, msg string) {
	err := r.conn.SendMessage(r.exchange, queueName, msg)
	if err != nil {
		logger.Error("RabbitMQ发送消息错误:" + err.Error())
	}
}

// RabbitMessageListener 侦听指定队列消息，内部自建侦听协程
func (r *connection) RabbitMessageListener(queueName string, listener func(msg []byte)) {
	//侦听之前先创建队列
	r.RabbitCreateNewQueue(queueName)
	//启动侦听消息处理线程
	go r.conn.ProcessQueue(queueName, listener)
}

// RabbitCreateNewQueue 创建队列
func (r *connection) RabbitCreateNewQueue(queueName string) {
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
	err := r.conn.CreateScheme(*setting)
	if err != nil {
		logger.Error("RabbitMQ创建队列失败:" + err.Error())
	}
}

// RabbitCreateDeadLetterQueue 创建死信队列
func (r *connection) RabbitCreateDeadLetterQueue(queueName, toQueueName string, ttl int) {
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
	err := r.conn.CreateScheme(*setting)
	if err != nil {
		logger.Error("RabbitMQ创建死信队列失败:" + err.Error())
	}
}
