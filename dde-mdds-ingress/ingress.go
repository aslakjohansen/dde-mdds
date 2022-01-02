package main

import (
  "fmt"
  "sync"
  "encoding/json"
  "os"
  "strconv"
  
  // kafka
	"github.com/confluentinc/confluent-kafka-go/kafka"
	
	// postgress
  "database/sql"
  _ "github.com/lib/pq"
)

const (
  default_broker         = "localhost"
  default_topic          = "myTopic"
  default_host           = "192.168.1.38"
  default_port           = 5432
  default_user           = "docker"
  default_password       = "docker"
  default_database       = "mdds"
  default_consumer_count = 1
)

var (
  key2id map[string]int = make(map[string]int)
  lookup_mutex sync.Mutex
  broker string
  topic string
  host string
  port int
  user string
  password string
  database string
  consumer_count int
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

func pull_env () {
  parameter_string := func (key string, default_value string) string {
    value, present := os.LookupEnv(key)
    if present {
      return value
    } else {
      return default_value
    }
  }
  parameter_int := func (key string, default_value int) int {
    value, present := os.LookupEnv(key)
    if present {
      i, err := strconv.Atoi(value)
      if err==nil {
        return i
      } else {
        fmt.Printf("Err: Unable to parse environment variable '%s' as int\n", key)
        os.Exit(1)
        return -1
      }
    } else {
      return default_value
    }
  }
  
  broker         = parameter_string("KAFKA_BROKER"      , default_broker)
  topic          = parameter_string("KAFKA_INGEST_TOPIC", default_topic)
  host           = parameter_string("DBMS_HOST"    , default_host)
  user           = parameter_string("DBMS_USER"    , default_user)
  password       = parameter_string("DBMS_PASSWORD", default_password)
  database       = parameter_string("DBMS_DATABASE", default_database)
  port           = parameter_int(   "DBMS_PORT"      , default_port)
  consumer_count = parameter_int(   "CONSUMER_COUNT"   , default_consumer_count)
  
  fmt.Println("Configuration (override through environment variables):")
  fmt.Printf(" - Kafka:\n")
  fmt.Printf("   - broker='%s' (env KAFKA_BROKER)\n", broker)
  fmt.Printf("   - topic='%s' (env KAFKA_INGEST_TOPIC)\n", topic)
  fmt.Printf(" - DBMS:\n")
  fmt.Printf("   - host='%s' (env DBMS_HOST)\n", host)
  fmt.Printf("   - user='%s' (env DBMS_USER)\n", user)
  fmt.Printf("   - password='%s' (env DBMS_PASSWORD)\n", password)
  fmt.Printf("   - database='%s' (env DBMS_DATABASE)\n", database)
  fmt.Printf("   - port='%d' (env DBMS_PORT)\n", port)
  fmt.Printf(" - Operation:\n")
  fmt.Printf("   - consumer_count='%d' (env CONSUMER_COUNT)\n", consumer_count)
  fmt.Printf("\n")
}

func lookup_id (db *sql.DB, deviceid string, sensorid string) int {
  var q string
  fmt.Println("a")
  
  // avoid potential race conditions
  lookup_mutex.Lock()
  defer lookup_mutex.Unlock()
  fmt.Println("b")
  
  key := deviceid+""+sensorid
  
  // check cache
  id, exists := key2id[key]
  if exists {
    return id
  }
  fmt.Println("c")
  
  // make sure mapping exists
  q = fmt.Sprintf("INSERT INTO metadata (device_id, sensor_id) VALUES ('%s', '%s')", deviceid, sensorid)
  fmt.Println(" -", q)
//  _, err :=
  fmt.Println("cd")
  db.Exec(q)
  fmt.Println("d")
  
  // look up mapping
  q = fmt.Sprintf("SELECT id FROM metadata WHERE device_id='%s' AND sensor_id='%s'", deviceid, sensorid)
  rows, err := db.Query(q)
  if err != nil {
    fmt.Println("Unable to lookup metadata:", q, err);
    return -1
  }
  fmt.Println("e")
  defer rows.Close()
  for rows.Next() {
    err = rows.Scan(&id)
    if err != nil {
      fmt.Println("Unable to scan metadata:", q, err);
      return -1
    }
    
    key2id[key] = id
    
    // insert in control
    q = fmt.Sprintf("INSERT INTO control (metadata_id, processed) VALUES ('%d', FALSE)", id)
    _, err := db.Exec(q)
    if err != nil {
      fmt.Println("Unable to add to control:", q, err);
    }
    
    return id
  }
  fmt.Println("f")
  return -1
}

func insert (ch chan Message) {
  // connect to database
  
//  psqlconn := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable",
//        user,
//        password,
//        host,
//        port,
//        dbname)
  psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, database)
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
      fmt.Println("Unable to lookup id")
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
  pull_env()
  
  // start consumers
  var ch chan Message = make(chan Message, 16)
  for i:=0; i<consumer_count; i++ { go insert(ch) }
  
  // create consumer
  c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": broker,
    "group.id":          "myGroup",
    "auto.offset.reset": "earliest",
  })
  if err != nil {
    panic(err)
  }
  defer c.Close()
  
  // register subscription
  c.SubscribeTopics([]string{topic}, nil)
  
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
