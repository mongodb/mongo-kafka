# MongoDB Kafka BsonObjectId

This implementaion allows the MongoDB Kafka sink connector to convert an existing _id that is string as ObjectId

# Usage

If the id is provided as part of the value you can use:
```
		"post.processor.chain": "com.mongodb.kafka.connect.sink.processor.DocumentIdAdder",
		"document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidProvidedInValueStrategy",
		"document.id.strategy.overwrite.existing": true,
```

If the id is provided as part of the key you can use:
```
		"post.processor.chain": "com.mongodb.kafka.connect.sink.processor.DocumentIdAdder",
		"document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidProvidedInKeyStrategy",
		"document.id.strategy.overwrite.existing": true,
```

