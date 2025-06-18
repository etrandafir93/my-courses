
# Demo Cheatsheet

connect to the Kafka container
```bash
docker exec -it kafka bash
cd ../../bin
```

console consumer
```bash
./kafka-console-consumer --bootstrap-server kafka:9092 --topic foo --from-beginning --group test-id
```

console producer
```bash
./kafka-console-producer --broker-list kafka:9092 --topic foo --property "parse.key=true" --property "key.separator=:"
```