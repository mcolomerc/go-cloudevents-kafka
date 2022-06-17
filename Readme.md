# CloudEvents Kafka  

Example of a Kafka Cloud Events Protocol to produce/consume CloudEvents with Kafka.

## Cloud Events 

- [CloudEvents](https://cloudevents.io/)

- [CloudEvents spec](https://github.com/cloudevents/spec)

- SDK GO: [github.com/cloudevents/sdk-go](github.com/cloudevents/sdk-go/v2)  

- [Protocol Binding](https://github.com/cloudevents/spec/blob/v1.0/spec.md#protocol-binding) 

## Kafka Golang

The CloudEvents SDK GO uses Sarama as Kafka protocol binding implementation, this example uses [Confluent Kafka GO](https://github.com/confluentinc/confluent-kafka-go) instead. 

New Protocol Binding using Confluent Kafka GO was implemented.

## Confluent Cloud 

Kafka cluster: The example uses [Confluent Cloud](https://confluent.cloud) fully managed, cloud-native service. 

## Configuration 

.env file: 

```properties
BOOTSTRAP_SERVERS=<bootstrap-servers>
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
SASL_USERNAME=<user-name> # Confluent Cloud API_KEY
SASL_PASSWORD=<user-password> # Confluent Cloud API_SECRET

ACKS=all # all,0,1 

TOPIC = "cevents"
```
 
### Events 

The example generates a random number of events and sends them to the Kafka topic.

Event example: 

```json
{
  "hostname": "hostname_value",
  "allocmemory": "24921944",
  "totalalloc": "23",
  "system": "34753296",
  "numgc": "1",
  "memusage": "51",
  "cpu": "7",
  "Created": "2022-06-14T14:07:49.05547+02:00"
}
```
 
Kafka headers: 

```json
[
  {
    "key": "ce_id",
    "stringValue": "7fdba707-390e-47ec-b0c2-bbdc848e036d"
  },
  {
    "key": "ce_source",
    "stringValue": "hostname_value"
  },
  {
    "key": "ce_specversion",
    "stringValue": "1.0"
  },
  {
    "key": "ce_type",
    "stringValue": "mcolomerc/kafka-client/event"
  },
  {
    "key": "content-type",
    "stringValue": "application/json"
  },
  {
    "key": "ce_time",
    "stringValue": "2022-06-14T12:07:49.055556Z"
  }
]
```

### Load Configuration

Load configuration from **.env** file

```go
config, err := config.LoadConfig("./.env") 
```

### Producer 

Producing CloudEvents to Kafka: 

* Build the CloudEvents sender: 

```go  
sender, err := kafka_pb.NewSender(&config)
```
 
* Instantiate a new CloudEvent client

```go 
c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
```

* Build an Event:

```go 
evtn := event.New()
jsonEvtn, err := json.Marshal(evtn)
```

* Build a CloudEvent:

```go
e := cloudevents.NewEvent()
e.SetID(uuid.New().String())
e.SetType("mcolomerc/kafka-client/event")
e.SetSource(evtn.Hostname)
_ = e.SetData(cloudevents.ApplicationJSON, jsonEvtn)
```

* Send the Cloud Event:

```go
c.Send(kafka_pb.WithMessageKey(context.Background(), kafka_pb.StringEncoder(e.ID())), e)
```

### Consumer 

* With *NewProtocol* you can use the same client both to send and receive.

```go  
protocol, err := kafka_pb.NewProtocol(&config)
```

* CloudEvents client  

```go 
c, err := cloudevents.NewClient(protocol, cloudevents.WithTimeNow(), cloudevents.WithUUIDs()) 
```

* Start the consumer:

```go 
go func() { 
  var recvCount int32
  err = c.StartReceiver(context.TODO(), func(ctx context.Context, event cloudevents.Event) {
   consume(ctx, event)
   if atomic.AddInt32(&recvCount, 1) == count {
     done <- struct{}{}
   }
  })
...
}()
```

* Event processing: 

```go
func consume(ctx context.Context, event cloudevents.Event) {
	fmt.Printf("Received CloudEvent: %s", event)
}

```

