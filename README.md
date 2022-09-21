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
