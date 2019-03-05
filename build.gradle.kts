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

plugins {
    `java-library`
}

group = "org.mongodb.kafka"
version = "0.1-SNAPSHOT"
description = "A basic Apache Kafka Connect SinkConnector allowing data from Kafka topics to be stored in MongoDB collections."

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    maven("http://packages.confluent.io/maven/")
    mavenCentral()
}

extra.apply {
    set("kafkaVersion", "2.0.0")
    set("mongodbDriverVersion", "3.8.2")
    set("logbackVersion", "1.2.3")
    set("jacksonVersion", "2.9.7")
    set("confluentSerializerVersion", "5.0.0")
    set("confluentConnectPluginVersion", "0.11.1")
    set("junitJupiterVersion", "5.3.1")
    set("junitPlatformVersion", "1.3.1")
    set("hamcrestVersion", "2.0.0.0")
    set("mockitoVersion", "2.22.0")
    set("testcontainersVersion", "1.9.1")
    set("avroVersion", "1.8.2")
    set("okHttpVersion", "3.11.0")
    set("yamlBeansVersion", "1.13")
    set("connectUtilsVersion", "[0.2.31,0.2.1000)")
}

dependencies {
    api("org.apache.kafka:connect-api:${extra["kafkaVersion"]}")
    implementation("org.mongodb:mongodb-driver:${extra["mongodbDriverVersion"]}")
    implementation("ch.qos.logback:logback-classic:${extra["logbackVersion"]}")
    implementation("com.fasterxml.jackson.core:jackson-core:${extra["jacksonVersion"]}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${extra["jacksonVersion"]}")
    implementation("io.confluent:kafka-avro-serializer:${extra["confluentSerializerVersion"]}")
    implementation("io.confluent:kafka-connect-maven-plugin:${extra["confluentConnectPluginVersion"]}")

    testImplementation("org.junit.jupiter:junit-jupiter-engine:${extra["junitJupiterVersion"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${extra["junitJupiterVersion"]}")
    testImplementation("org.junit.vintage:junit-vintage-engine:${extra["junitJupiterVersion"]}")
    testImplementation("org.junit.platform:junit-platform-runner:${extra["junitPlatformVersion"]}")
    testImplementation("org.junit.platform:junit-platform-console:${extra["junitPlatformVersion"]}")
    testImplementation("org.hamcrest:hamcrest-junit:${extra["hamcrestVersion"]}")
    testImplementation("org.mockito:mockito-core:${extra["mockitoVersion"]}")
    testImplementation("org.testcontainers:testcontainers:${extra["testcontainersVersion"]}")
    testImplementation("org.apache.avro:avro:${extra["avroVersion"]}")
    testImplementation("org.apache.avro:avro-maven-plugin:${extra["avroVersion"]}")
    testImplementation("com.squareup.okhttp3:okhttp:${extra["okHttpVersion"]}")
    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${extra["jacksonVersion"]}")
    testImplementation("com.esotericsoftware.yamlbeans:yamlbeans:${extra["yamlBeansVersion"]}")
    testImplementation("com.github.jcustenborder.kafka.connect:connect-utils:${extra["connectUtilsVersion"]}")
}


tasks {
    test {
        testLogging.showExceptions = true
        exclude("**/*IT.class")
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
