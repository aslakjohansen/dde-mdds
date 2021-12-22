package main

import (
  "fmt"
  "time"
  "strings"
  "encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
  topic string = "myTopic"
)

type Message struct {
    EventId      string  `json:"EventId"`
    ValueGroupId string  `json:"ValueGroupId"`
    CustomerID   int32   `json:"CustomerID"`
    SystemID     int32   `json:"SystemID"`
    SensorID     string  `json:"SensorID"`
    TimeStamp    string  `json:"TimeStamp"`
    Value        float64 `json:"Value"`
    Status       string  `json:"Status"`
    DeviceID     string  `json:"DeviceID"`
}

func producer (deviceid string, sensorid string, duration int, initial float64) {
  p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer p.Close()
	
  // Delivery report handler for produced messages
  go func() {
    for e := range p.Events() {
    switch ev := e.(type) {
      case *kafka.Message:
        if ev.TopicPartition.Error != nil {
          fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
        } else {
//          fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
        }
      }
    }
  }()
  
  // Produce messages to topic (asynchronously)
  var value float64 = initial
  for {
    t := strings.Split(time.Now().Format(time.RFC3339Nano), "+")[0]+"Z"
    var msg Message = Message{
      EventId:      "7277ddef-6150-4f57-b90a-977121b3d1cf",
      ValueGroupId: "00000000-0000-0000-0000-000000000000",
      CustomerID:   -1,
      SystemID:     2,
      SensorID:     sensorid,
      TimeStamp:    t,
      Value:        value,
      Status:       "0",
      DeviceID:     deviceid,
    }
    
    encoded, _ := json.Marshal(msg)
//    fmt.Printf("Success", success)
    
    p.Produce(&kafka.Message{
      TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
      Value:          []byte(encoded),
    }, nil)
    
    value *= 1.2
    
    // insert pause
    time.Sleep(time.Duration(duration) * time.Second)
  }
}

func main () {
  go producer("70b3d54750130803", "Energy23_32", 10,  
  
  1.0)
  go producer("a81758fffe047019", "Battery"    , 11, 12.0)
  go producer("70b3d54750130722", "Energy20_32",  7,  7.0)
  
  // wait forever 
  ch := make(chan int)
  <- ch
}
