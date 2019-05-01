# MongoDB & Kafka Docker end to end example

A simple example that takes JSON documents from the `source` topic and stores them into the `test.sink` collection in MongoDB using 
the MongoDB Kafka Sink Connector. 

The MongoDB Kafka Source Connector also publishes all change stream events from `test.sink` into the `mongo.test.sink` topic.

## Requirements
  - Docker 18.09+
  - Docker compose 1.24+
  - *nix system

## Running the example

To run the example: `./run.sh` which will:
  
  - Run `docker-compose up` 
  - Wait for MongoDB, Kafka, Kafka Connect to be ready
  - Register the MongoDB Kafka Sink Connector
  - Register the MongoDB Kafka Source Connector
  - Publish some events to Kafka
  - Write the events to MongoDB  
  - Write the change stream messages to Kafka


Once running, examine the topics in the Kafka UI: http://localhost:8000/
  - The `source` topic should contain the 10 simple documents added. Each similar to:<br>
    ```json
    {"i": "0"}
    ```
  - The `mongo.test.sink` should contain the 10 change events. Each similar to:<br>
    ```json
    {"_id": {"_data": "<resumeToken>"}, 
     "operationType": "insert",
     "ns": {"db": "test", "coll": "sink"},
     "documentKey": {"_id": {"$oid": "5cc99f4893283d634cb3f59e"}},
     "clusterTime": {"$timestamp": {"t": 1556717385, "i": 1}},
     "fullDocument": {"_id": {"$oid": "5cc99f4893283d634cb3f59e"}, "i": "0"}}
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
