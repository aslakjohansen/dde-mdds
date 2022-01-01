package main

import (
  "fmt"
  "sync"
  "time"
  "container/list"
  
  // kafka
	"github.com/confluentinc/confluent-kafka-go/kafka"
	
	// postgress
  "database/sql"
  _ "github.com/lib/pq"
)

type WorkPackage struct {
  device_id string
  sensor_id string
}

type Reading struct {
  time  time.Time
  value float64
}

const (
  host     = "192.168.1.38"
  port     = 5432
  user     = "docker"
  password = "docker"
  dbname   = "mdds"
  WORKER_COUNT = 1
  WORKQUEUE_SIZE = 16
  SLEEP_TIME      = 60 // unit: s
  COLLECTION_TIME = 60 // unit: ?
  topic    = "myTopic"
)

var (
  wg       *sync.WaitGroup  = new(sync.WaitGroup)
  worklist chan WorkPackage = make(chan WorkPackage, WORKQUEUE_SIZE)
)

func start_postgress_client () (*sql.DB, error) {
  psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlconn)
  if err != nil {
    fmt.Println("Unable to create connection to database", err)
    return db, err
  }
  
  return db, err
}

func start_kafka_client () (*kafka.Producer, error) {
  k, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	
  // Delivery report handler for produced messages
  go func() {
    for e := range k.Events() {
      switch ev := e.(type) {
      case *kafka.Message:
        if ev.TopicPartition.Error != nil {
          fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
        } else {
          fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
        }
      }
    }
  }()
  
  return k, err
}

func worker () {
  // start postgress client
  db, _ := start_postgress_client()
	defer db.Close()
  
  // start kafka client
  k, _ := start_kafka_client()
	defer k.Close()
  
  for wp := range worklist {
    var readings *list.List = list.New()
    fmt.Println(wp)
    
    // fetch timeseries
    q := fmt.Sprintf("SELECT samples.time, samples.value FROM metadata, samples WHERE samples.metadata_id = metadata.id")
    fmt.Println(q)
    rows, err := db.Query(q)
    if err != nil {
      fmt.Println("Unable to query samples:", q, err);
      wg.Done()
      continue
    } else {
      for rows.Next() {
        var r Reading
//        var t time.Time
//        var v float64
        err = rows.Scan(&r.time, &r.value)
        if err != nil {
          fmt.Println("Unable to scan samples:", q, err);
          break
        } else {
          fmt.Println(r)
          readings.PushBack(r)
        }
      }
    }
    fmt.Println(readings.Len())
    
    // start worker
    
    // push timeseries to worker
    
    // fetch result from worker
    
    // publish result
//    k.Produce(&kafka.Message{
//      TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
//      Value:          []byte(word),
//    }, nil)
    
    wg.Done()
  }
}

func main () {
  // init
  for i:=0; i<WORKER_COUNT; i++ { go worker() }
  db, _ := start_postgress_client()
	defer db.Close()
  
  for {
    // fetch
    q := fmt.Sprintf("SELECT device_id, sensor_id, MAX(EXTRACT(EPOCH FROM (s1.time-s2.time))) timediff FROM control, metadata, samples AS s1, samples AS s2 WHERE control.metadata_id=metadata.id AND s1.metadata_id=metadata.id AND s2.metadata_id=s1.metadata_id AND control.processed=FALSE GROUP BY metadata.device_id, metadata.sensor_id")
    fmt.Println(q)
    rows, err := db.Query(q)
    if err != nil {
      fmt.Println("Unable to query worklist:", q, err);
    }
    
    // push to queue
    if err==nil {
      defer rows.Close()
      
      var device_id string
      var sensor_id string
      var timediff float64
      for rows.Next() {
        err = rows.Scan(&device_id, &sensor_id, &timediff)
        if err != nil {
          fmt.Println("Unable to scan worklist:", q, err);
          break
        }
        
        if timediff > COLLECTION_TIME {
          worklist <- WorkPackage{device_id, sensor_id}
          wg.Add(1)
        }
      }
    }
    
    // iterate
    wg.Wait()
    
    // wait
    time.Sleep(time.Duration(SLEEP_TIME) * time.Second)
  }
}
