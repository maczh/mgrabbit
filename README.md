# MGin rabbitmq注册插件

## 安装
```bash
go get -u github.com/maczh/mgrabbit
```

## 使用
在MGin微服务模块的main.go中,在mgin.Init()之后，加入一行

```go
	//加载RabbitMQ消息队列
	mgin.MGin.Use("rabbitmq",mgrabbit.Rabbit.Init,mgrabbit.Rabbit.Close,nil)
```

## yml配置
### 在MGin微服务模块本地配置文件中
```yaml
go:
  config:
    used: rabbitmq
    prefix:
      rabbitmq: rabbitmq-example
```

### 配置中心的rabbitmq-example-test.yml配置
```yaml
go:
  rabbitmq:
    uri: amqp://test:********@xxx.xxx.xxx.xxx:5672/%2ftest
    exchange: e1 
```

### 配置中心的rabbitmq-example-test.yml多连接配置
```yaml
go:
  rabbitmq:
    multi: true
    conns: c1,c2
    c1:
      uri: amqp://test1:********@xxx.xxx.xxx.xxx:5672/%2ftest1
      exchange: e1 
    c2:
      uri: amqp://test2:********@xxx.xxx.xxx.xxx:5672/%2ftest2
      exchange: e2 
```


## 变更记录
- v0.0.4 多连接优化处理
- v0.0.3 多连接支持