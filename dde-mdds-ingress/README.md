# Dependencies

Confluent API
```shell
go get github.com/confluentinc/confluent-kafka-go/kafka
go get github.com/lib/pq
```

# Running

```shell
make
./ingress
```

# TODO

Wrap the writes to the `metadata` and `control`tables in a transaction.

