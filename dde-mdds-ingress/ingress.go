package main

import (
  "fmt"
  "sync"
  "encoding/json"
  
  // kafka
	"github.com/confluentinc/confluent-kafka-go/kafka"
	
	// postgress
  "database/sql"
  _ "github.com/lib/pq"
)

const (
  host     = "192.168.1.38"
  port     = 5432
  user     = "docker"
  password = "docker"
  dbname   = "mdds"
  CONSUMER_COUNT = 1
)

var (
  key2id map[string]int = make(map[string]int)
  lookup_mutex sync.Mutex
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

func lookup_id (db *sql.DB, deviceid string, sensorid string) int {
  var q string
  
  // avoid potential race conditions
  lookup_mutex.Lock()
  defer lookup_mutex.Unlock()
  
  key := deviceid+""+sensorid
  
  id, exists := key2id[key]
  if exists {
    return id
  }
  
  // make sure mapping exists
  q = fmt.Sprintf("INSERT INTO metadata (device_id, sensor_id) VALUES ('%s', '%s')", deviceid, sensorid)
  _, err := db.Exec(q)
  if err != nil {
    fmt.Println("Unable to insert metadata:", q, err);
    return -1
  }
  
  // look up mapping
  q = fmt.Sprintf("SELECT id FROM metadata WHERE device_id='%s' AND sensor_id='%s'", deviceid, sensorid)
  rows, err := db.Query(q)
  if err != nil {
    fmt.Println("Unable to lookup metadata:", q, err);
    return -1
  }
  defer rows.Close()
  for rows.Next() {
    err = rows.Scan(&id)
    if err != nil {
      fmt.Println("Unable to scan metadata:", q, err);
      return -1
    }
    
    key2id[key] = id
    return id
  }
  return -1
}

func insert (ch chan Message) {
  // connect to database
  
  psqlconn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable",
        user,
        password,
        host,
        port,
        dbname)
//  psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlconn)
  if err != nil {
    fmt.Println("Unable to create connection to database", err)
    return
  }
  defer db.Close()
  
  // service loop
  for msg := range ch {
    var deviceid  string  = msg.DeviceID
    var sensorid  string  = msg.SensorID
    var timestamp string  = msg.TimeStamp
    var value     float64 = msg.Value
    fmt.Println(deviceid, sensorid, timestamp, value)
    
    var id int = lookup_id(db, deviceid, sensorid)
    if id==-1 {
      continue
    }
    
    q := fmt.Sprintf("INSERT INTO samples (metadata_id, time, value) VALUES (%d, '%s', %f)", id, timestamp, value)
    _, err = db.Exec(q)
    if err != nil {
      fmt.Println("Unable to insert samples:", q);
    }
  }
}

func main () {
  // start consumers
  var ch chan Message = make(chan Message, 16)
  for i:=0; i<CONSUMER_COUNT; i++ { go insert(ch) }
  
  // create consumer
  c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost",
    "group.id":          "myGroup",
    "auto.offset.reset": "earliest",
  })
  if err != nil {
    panic(err)
  }
  defer c.Close()
  
  // register subscription
  c.SubscribeTopics([]string{"myTopic"}, nil)
  
  // service loop
  for {
    encoded, err := c.ReadMessage(-1)
    if err == nil {
      var msg Message
      err := json.Unmarshal(encoded.Value, &msg)
      if err == nil {
        ch <- msg
      }
    } else {
      // The client will automatically try to recover from all errors.
      fmt.Printf("Consumer error: %v (%v)\n", err, encoded)
    }
  }
}
