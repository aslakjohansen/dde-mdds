package main

import (
  "fmt"
  "encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

func insert (deviceid string, sensorid string, timestamp string, value float64) {
  
}

func main () {
  c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost",
    "group.id":          "myGroup",
    "auto.offset.reset": "earliest",
  })
  
  if err != nil {
    panic(err)
  }
  
  c.SubscribeTopics([]string{"myTopic"}, nil)
  
  for {
    encoded, err := c.ReadMessage(-1)
    if err == nil {
      var msg Message
      err := json.Unmarshal(encoded.Value, &msg)
      if err == nil {
        var deviceid  string  = msg.DeviceID
        var sensorid  string  = msg.SensorID
        var timestamp string  = msg.TimeStamp
        var value     float64 = msg.Value
        fmt.Println(deviceid, sensorid, timestamp, value)
        insert(deviceid, sensorid, timestamp, value)
      }
    } else {
      // The client will automatically try to recover from all errors.
      fmt.Printf("Consumer error: %v (%v)\n", err, encoded)
    }
  }
  
  c.Close()
}
