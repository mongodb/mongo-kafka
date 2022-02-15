/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.avro;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;

final class SchemaNameIntegrationTest extends MongoKafkaTestCase {
  @Test
  @DisplayName(
      "Ensure Avro schema namespaces are correctly reflected in Kafka Connect schema names and can be stored in Schema Registry")
  void schemaName() throws RestClientException, IOException {
    SchemaRegistryClient src = new CachedSchemaRegistryClient(KAFKA.schemaRegistryUrl(), 1);
    org.apache.kafka.connect.data.Schema kafkaConnectRecordSchema =
        com.mongodb.kafka.connect.source.schema.AvroSchema.fromJson(
            "{\n"
                + "  \"name\": \"recordSchemaName\",\n"
                + "  \"namespace\": \"recordSchemaNamespace\",\n"
                + "  \"type\": \"record\",\n"
                + "  \"fields\": [\n"
                + "    {\n"
                + "      \"name\": \"nestedRecordFieldName\",\n"
                + "      \"type\": {\n"
                + "        \"name\": \"nested.Record.Schema.Namespace.nestedRecordSchemaName\",\n"
                + "        \"namespace\": \"nestedRecordSchemaNamespace\",\n"
                + "        \"type\": \"record\",\n"
                + "        \"fields\": [\n"
                + "          {\n"
                + "            \"name\": \"fieldName\",\n"
                + "            \"type\": \"int\"\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}");
    org.apache.kafka.connect.data.Schema kafkaConnectNestedRecordFieldSchema =
        kafkaConnectRecordSchema.field("nestedRecordFieldName").schema();
    assertAll(
        () ->
            assertEquals("recordSchemaNamespace.recordSchemaName", kafkaConnectRecordSchema.name()),
        () ->
            assertEquals(
                "nested.Record.Schema.Namespace.nestedRecordSchemaName",
                kafkaConnectNestedRecordFieldSchema.name()));
    org.apache.avro.Schema avroRecordSchema =
        new AvroData(1).fromConnectSchema(kafkaConnectRecordSchema);
    int registeredSchemaId = src.register("subject", new AvroSchema(avroRecordSchema));
    ParsedSchema retrievedSchema = src.getSchemaById(registeredSchemaId);
    org.apache.avro.Schema retrievedAvroRecordSchema =
        (org.apache.avro.Schema) retrievedSchema.rawSchema();
    org.apache.avro.Schema retrievedAvroNestedRecordSchema =
        retrievedAvroRecordSchema.getField("nestedRecordFieldName").schema();
    assertAll(
        () -> assertEquals("recordSchemaName", retrievedAvroRecordSchema.getName()),
        () -> assertEquals("recordSchemaNamespace", retrievedAvroRecordSchema.getNamespace()),
        () -> assertEquals("nestedRecordSchemaName", retrievedAvroNestedRecordSchema.getName()),
        () ->
            assertEquals(
                "nested.Record.Schema.Namespace", retrievedAvroNestedRecordSchema.getNamespace()));
  }
}
