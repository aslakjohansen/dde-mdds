# Dependencies

Confluent API
```shell
go get github.com/confluentinc/confluent-kafka-go/kafka
go get github.com/lib/pq
```

# Running

```shell
make
./process
```

# Dockerization

Build
```shell
make docker
```

Run
```shell
docker run -d dde-mdds-process
```

