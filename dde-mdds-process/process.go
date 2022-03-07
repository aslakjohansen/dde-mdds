package main

import (
  "fmt"
  "sync"
  "time"
  "container/list"
  "os/exec"
  "io"
  "bufio"
  "os"
  "strconv"
  
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
  default_broker   = "localhost"
  default_topic    = "metadata_discovery"
  default_host     = "192.168.1.38"
  default_port     = 5432
  default_user     = "docker"
  default_password = "docker"
  default_database = "mdds"
  default_worker_count = 1
  default_workqueue_size = 16
  default_sleep_time      = 60 // unit: s
  default_collection_time = 60 // unit: ?
)

var (
  wg       *sync.WaitGroup  = new(sync.WaitGroup)
  worklist chan WorkPackage
  broker string
  topic string
  host string
  port int
  user string
  password string
  database string
  worker_count int
  workqueue_size int
  sleep_time int
  collection_time float64
)

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
  parameter_float := func (key string, default_value float64) float64 {
    value, present := os.LookupEnv(key)
    if present {
      f, err := strconv.ParseFloat(value, 64)
      if err==nil {
        return f
      } else {
        fmt.Printf("Err: Unable to parse environment variable '%s' as int\n", key)
        os.Exit(1)
        return -1
      }
    } else {
      return default_value
    }
  }
  
  broker          = parameter_string("KAFKA_BROKER"   , default_broker)
  topic           = parameter_string("KAFKA_PUB_TOPIC", default_topic)
  host            = parameter_string("DBMS_HOST"    , default_host)
  user            = parameter_string("DBMS_USER"    , default_user)
  password        = parameter_string("DBMS_PASSWORD", default_password)
  database        = parameter_string("DBMS_DATABASE", default_database)
  port            = parameter_int(   "DBMS_PORT"      , default_port)
  worker_count    = parameter_int(   "WORKER_COUNT"   , default_worker_count)
  workqueue_size  = parameter_int(   "WORKQUEUE_SIZE" , default_workqueue_size)
  sleep_time      = parameter_int(   "SLEEP_TIME"     , default_sleep_time)
  collection_time = parameter_float("COLLECTION_TIME", default_collection_time)
  
  fmt.Println("Configuration (override through environment variables):")
  fmt.Printf(" - Kafka:\n")
  fmt.Printf("   - broker='%s' (env KAFKA_BROKER)\n", broker)
  fmt.Printf("   - topic='%s' (env KAFKA_PUB_TOPIC)\n", topic)
  fmt.Printf(" - DBMS:\n")
  fmt.Printf("   - host='%s' (env DBMS_HOST)\n", host)
  fmt.Printf("   - user='%s' (env DBMS_USER)\n", user)
  fmt.Printf("   - password='%s' (env DBMS_PASSWORD)\n", password)
  fmt.Printf("   - database='%s' (env DBMS_DATABASE)\n", database)
  fmt.Printf("   - port='%d' (env DBMS_PORT)\n", port)
  fmt.Printf(" - Operation:\n")
  fmt.Printf("   - worker_count='%d' (env WORKER_COUNT)\n", worker_count)
  fmt.Printf("   - workqueue_size='%d' (env WORKQUEUE_SIZE)\n", workqueue_size)
  fmt.Printf("   - sleep_time='%d' (env SLEEP_TIME)\n", sleep_time)
  fmt.Printf("   - collection_time='%f' (env COLLECTION_TIME)\n", collection_time)
  fmt.Printf("\n")
}

func start_postgress_client () (*sql.DB, error) {
  psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, database)
  db, err := sql.Open("postgres", psqlconn)
  if err != nil {
    fmt.Println("Unable to create connection to database", err)
    return db, err
  }
  
  return db, err
}

func start_kafka_client () (*kafka.Producer, chan bool, error) {
  var response chan bool = make(chan bool)
  
  k, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		panic(err)
	}
	
  // Delivery report handler for produced messages
  go func(response chan bool) {
    for e := range k.Events() {
      switch ev := e.(type) {
      case *kafka.Message:
        if ev.TopicPartition.Error != nil {
          fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
          response <- false
        } else {
          fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
          response <- true
        }
      }
    }
  }(response)
  
  return k, response, err
}

func worker () {
  // start postgress client
  db, _ := start_postgress_client()
	defer db.Close()
  
  // start kafka client
  k, kafka_response, _ := start_kafka_client()
	defer k.Close()
  
  for wp := range worklist {
    var readings *list.List = list.New()
    fmt.Println(wp)
    
    // fetch timeseries
    q := fmt.Sprintf("SELECT samples.time, samples.value FROM metadata, samples WHERE samples.metadata_id = metadata.id AND metadata.device_id='%s' and metadata.sensor_id='%s'", wp.device_id, wp.sensor_id)
    fmt.Println(q)
    rows, err := db.Query(q)
    if err != nil {
      fmt.Println("Unable to query samples:", q, err);
      wg.Done()
      continue
    } else {
      for rows.Next() {
        var r Reading
        err = rows.Scan(&r.time, &r.value)
        if err != nil {
          fmt.Println("Unable to scan samples:", q, err);
          break
        } else {
          readings.PushBack(r)
        }
      }
    }
    
    // start worker
    
    var cmd *
    exec.Cmd = exec.Command("/usr/bin/python", "./workers/dummy.py")
	  stdin, err := cmd.StdinPipe()
	  if err != nil {
		  fmt.Println("Unable to connect to STDIN of worker:", err)
	  }
	  stdout, err := cmd.StdoutPipe()
	  if err != nil {
		  fmt.Println("Unable to connect to STDOUT of worker:", err)
	  }
	  stderr, err := cmd.StderrPipe()
	  if err != nil {
		  fmt.Println("Unable to connect to STDERROR of worker:", err)
	  }
    
    err = cmd.Start()
	  if err != nil {
		  fmt.Println("Unable to start worker:", err)
		  wg.Done()
		  continue
	  }
    
    // push timeseries to worker
    for e := readings.Front(); e != nil; e = e.Next() {
      var r Reading = e.Value.(Reading)
      io.WriteString(stdin, fmt.Sprintf("%d %f\n", r.time.Unix(), r.value))
    }
    io.WriteString(stdin, "\n")
	  stdin.Close()
    
    // fetch result from worker
    var outchan chan string = make(chan string)
    go func (stdout io.Reader, response chan string) {
      var output string = ""
	    scanner := bufio.NewScanner(stdout)
	    scanner.Split(bufio.ScanLines)
	    for scanner.Scan() {
	      output += scanner.Text()+"\n"
	    }
	    response <- output
    }(stdout, outchan)
	  var output string = <- outchan
    err = cmd.Wait()
	  if err != nil {
		  fmt.Println("Unable to wait worker:", fmt.Sprint(err) + ": " + string(stderr))
		  wg.Done()
		  continue
	  }
	  fmt.Println("Output:", output)
	  
    // publish result
    k.Produce(&kafka.Message{
      TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
      Value:          []byte(output),
    }, nil)
    
    // mark as processed
    var success bool = <- kafka_response
    if success {
      q := fmt.Sprintf("UPDATE control SET processed=TRUE FROM metadata WHERE control.metadata_id=metadata.id AND  metadata.device_id='%s' and metadata.sensor_id='%s'", wp.device_id, wp.sensor_id)
      _, err = db.Exec(q)
      if err != nil {
        fmt.Println("Unable to mark as processed:", q);
      }
    } 
    
    wg.Done()
  }
}

func main () {
  pull_env()
  worklist = make(chan WorkPackage, workqueue_size)
  
  // init
  for i:=0; i<worker_count; i++ { go worker() }
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
        
        if timediff > collection_time {
          worklist <- WorkPackage{device_id, sensor_id}
          wg.Add(1)
        }
      }
    }
    
    // iterate
    wg.Wait()
    
    // wait
    time.Sleep(time.Duration(sleep_time) * time.Second)
  }
}
