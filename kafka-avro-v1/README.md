**Kafka Avro is using docker to start Kafka, creating topics and starting schema registry**

# How to start Docker
Pre-requisite: Docker (Windows) is installed 

1. Start Docker from Windows' menu. It will appear in Tray.

2. Run the docker-compose file using the command given below:

<code>
docker-compose up
</code>

This will start zookeeper, kafkaserver and schema registry

Docker Powered Landoop UI will be running and accessible on:

<code>
http://127.0.0.1:3030/
</code>