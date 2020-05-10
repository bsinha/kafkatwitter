# kafkatwitter
POC to feed twitter tweets into Kafka - Producer and Consumer

### How to start zookeeper
> zookeeper-server-start config\zookeeper.properties

### How to start Kafka server
> kafka-server-start config/server.properties

### Create a topic named twitter_tweets
> kafka-topics --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1

### Kafka Console Consumer to consumer topic twitter_tweets
> kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets

### Idempotent Producer configuration
 enable.idempotence = true (producer level) + min.insync.replicas = 2 (broker level)
- implies acks = all, retries = MAX_INT, max.in.flight.requests.per.connection=5 (for kafka >=1.0 or 1 for kafka 0.11)
- while keeping ordering guarantees and improved performance ! 

### Kafka Consumer Group Details 
- shows partitions and current-offset

>kafka-consumer-groups --bootstrap-server 127.0.0.1:9092  --group kafka-elasticsearch --describe


- reset offsets to earliest

>kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group kafka-elasticsearch --reset-offsets --execute --to-earliest --topic twitter_tweets

## Elastic search config

http://bonsai.io

**GET** /twitter/_doc/:id

## Kafka Connect
https://www.confluent.io/product/connectors-repository/
