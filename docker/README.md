# MongoDB & Kafka Docker end to end example

A simple example that takes JSON documents from the `pageviews` topic and stores them into the `test.pageviews` collection in MongoDB using 
the MongoDB Kafka Sink Connector. 

The MongoDB Kafka Source Connector also publishes all change stream events from `test.pageviews` into the `mongo.test.pageviews` topic.

## Requirements
  - Docker 18.09+
  - Docker compose 1.24+
  - *nix system

## Running the example

To run the example: `./run.sh` which will:
  
  - Run `docker-compose up` 
  - Wait for MongoDB, Kafka, Kafka Connect to be ready
  - Register the Confluent Datagen Connector
  - Register the MongoDB Kafka Sink Connector
  - Register the MongoDB Kafka Source Connector
  - Publish some events to Kafka via the Datagen connector
  - Write the events to MongoDB  
  - Write the change stream messages back into Kafka


Once running, examine the topics in the Kafka control center: http://localhost:9021/
  - The `pageviews` topic should contain the 10 simple documents added. Each similar to:<br>
    ```json
    {"viewtime": {"$numberLong": "81"}, "pageid": "Page_1", "userid": "User_8"}
    ```
  - The `mongo.test.pageviews` should contain the 10 change events. Each similar to:<br>
    ```json
     {"_id": {"_data": "<resumeToken>"}, 
      "operationType": "insert", 
      "clusterTime": {"$timestamp": {"t": 1563461814, "i": 4}}, 
      "fullDocument": {"_id": {"$oid": "5d3088b6bafa7829964150f3"}, "viewtime": {"$numberLong": "81"}, "pageid": "Page_1", "userid": "User_8"}, 
      "ns": {"db": "test", "coll": "pageviews"}, 
      "documentKey": {"_id": {"$oid": "5d3088b6bafa7829964150f3"}}} 
    ```

Examine the collections in MongoDB:
  - In your shell run: docker-compose exec mongo1 /usr/bin/mongo
  - Adding, updating data in `test.sink` - will show up as change stream events in the `mongo.test.sunk` topic

## docker-compose.yml

The following systems will be created:

  - Zookeeper
  - Kafka
  - Confluent Schema Registry
  - Confluent Kafka Connect
  - Confluent Control Center
  - Confluent KSQL Server
  - Kafka Rest Proxy
  - Kafka Topics UI
  - MongoDB - a 3 node replicaset

---
### Next

- [Installation guide](../docs/install.md)
- [The MongoDB Kafka sink connector guide](../docs/sink.md)
- [The MongoDB Kafka source connector guide](../docs/source.md)
- A docker end 2 end example
- [Changelog](../docs/changelog.md)
