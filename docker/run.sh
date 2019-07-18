#!/bin/bash

echo "Building the MongoDB Kafka Connector"
(
cd ..
./gradlew clean createConfluentArchive
echo -e "Unzipping the confluent archive plugin....\n"
unzip -d ./build/confluent ./build/confluent/*.zip
find ./build/confluent -maxdepth 1 -type d ! -wholename "./build/confluent" -exec mv {} ./build/confluent/kafka-connect-mongodb \;
)

echo "Starting docker ."
docker-compose up -d --build

sleep 5
echo -ne "\n\nWaiting for the systems to be ready.."
until $(curl --output /dev/null --silent --head --fail http://localhost:8082); do
    printf '.'
    sleep 1
done

until $(curl --output /dev/null --silent --head --fail http://localhost:18083); do
    printf '.'
    sleep 1
done

echo -e "\nConfiguring the MongoDB ReplicaSet.\n"
docker-compose exec mongo1 /usr/bin/mongo --eval '''if (rs.status()["ok"] == 0) {
    rsconf = {
      _id : "rs0",
      members: [
        { _id : 0, host : "mongo1:27017", priority: 1.0 },
        { _id : 1, host : "mongo2:27017", priority: 0.5 },
        { _id : 2, host : "mongo3:27017", priority: 0.5 }
      ]
    };
    rs.initiate(rsconf);
}

rs.conf();'''

function finish {
    curl --output /dev/null -X DELETE http://localhost:18083/connectors/mongo-sink
    curl --output /dev/null -X DELETE http://localhost:18083/connectors/mongo-source
    docker-compose exec mongo1 /usr/bin/mongo --eval "db.dropDatabase()"
    docker-compose down
    echo -e "Bye!\n"
}
trap finish EXIT

echo -e "\nKafka Topics:"
curl -X GET "http://localhost:8082/topics" -w "\n"

echo -e "\nKafka Connectors:"
curl -X GET "http://localhost:18083/connectors/" -w "\n"

echo -e "\nAdding MongoDB Kafka Sink Connector for the 'source' topic into the 'test.source' collection:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongo-sink",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"source",
     "connection.uri":"mongodb://mongo1:27017,mongo2:27017,mongo3:27017",
     "database":"test",
     "collection":"sink",
     "key.converter":"org.apache.kafka.connect.json.JsonConverter",
     "key.converter.schemas.enable":false,
     "value.converter":"org.apache.kafka.connect.json.JsonConverter",
     "value.converter.schemas.enable":false
}}' http://localhost:18083/connectors -w "\n"

echo -e "\nAdding MongoDB Kafka Source Connector for the 'test.sink' collection:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongo-source",
   "config": {
     "tasks.max":"1",
     "connector.class":"com.mongodb.kafka.connect.MongoSourceConnector",
     "connection.uri":"mongodb://mongo1:27017,mongo2:27017,mongo3:27017",
     "topic.prefix":"mongo",
     "database":"test",
     "collection":"sink"
}}' http://localhost:18083/connectors -w "\n"

sleep 2

echo -e "\nKafka Connectors: \n"
curl -X GET "http://localhost:18083/connectors/" -w "\n"

echo "Looking at data in `db.sink`:"
docker-compose exec mongo1 /usr/bin/mongo --eval "db.sink.find()"

echo -e "\nAdding some data to the 'source' topic \n"
curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
          --data '''{"records":[{"value":{"i":"0"}}, {"value":{"i":"1"}}, {"value":{"i":"2"}}, {"value":{"i":"3"}},
           {"value":{"i":"4"}}, {"value":{"i":"5"}}, {"value":{"i":"6"}}, {"value":{"i":"7"}}, {"value":{"i":"8"}},
           {"value":{"i":"9"}}]}''' \
          "http://localhost:8082/topics/source"

sleep 5
echo "Looking at data in `db.sink`:"
docker-compose exec mongo1 /usr/bin/mongo --eval "db.sink.find()"

echo -e '''

==============================================================================================================
Examine the topics in the Kafka UI: http://localhost:8000/
  - The `source` topic should contain the 10 simple documents added.
  - The `mongo.test.sink` should contain the change events.

Examine the collections:
  - In your shell run: docker-compose exec mongo1 /usr/bin/mongo
  - Adding, updating data in `test.sink` - will show up as change stream events in the `mongo.test.sunk` topic
==============================================================================================================

Use <ctrl>-c to quit'''

read -r -d '' _ </dev/tty
