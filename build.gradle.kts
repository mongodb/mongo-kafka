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

import java.io.ByteArrayOutputStream

buildscript {
    repositories {
        mavenCentral()
        jcenter()
    }
}

plugins {
    `java-library`
    idea
    checkstyle
    id("de.fuerstenau.buildconfig") version "1.1.8"
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
    set("kafkaVersion", "2.1.0")
    set("confluentVersion", "5.1.0")
    set("mongodbDriverVersion", "3.8.2")
    set("logbackVersion", "1.2.3")
    set("confluentSerializerVersion", "5.1.1")
    set("junitJupiterVersion", "5.4.0")
    set("junitPlatformVersion", "1.4.0")
    set("hamcrestVersion", "2.0.0.0")
    set("mockitoVersion", "2.24.0")
    set("avroVersion", "1.8.2")
    set("scalaVersion", "2.11.12")
    set("scalaMajMinVersion", "2.11")
    set("curatorVersion", "2.9.0")
}

dependencies {
    api("org.apache.kafka:connect-api:${extra["kafkaVersion"]}")
    implementation("org.mongodb:mongodb-driver:${extra["mongodbDriverVersion"]}")
    implementation("ch.qos.logback:logback-classic:${extra["logbackVersion"]}")
    implementation("io.confluent:kafka-avro-serializer:${extra["confluentSerializerVersion"]}")

    testImplementation("org.junit.jupiter:junit-jupiter:${extra["junitJupiterVersion"]}")
    testImplementation("org.junit.platform:junit-platform-runner:${extra["junitPlatformVersion"]}")
    testImplementation("org.hamcrest:hamcrest-junit:${extra["hamcrestVersion"]}")
    testImplementation("org.mockito:mockito-core:${extra["mockitoVersion"]}")

    // Integration Tests
    testImplementation("org.apache.avro:avro:${extra["avroVersion"]}")
    testImplementation("org.apache.curator:curator-test:${extra["curatorVersion"]}")
    testImplementation("org.apache.kafka:connect-runtime:${extra["kafkaVersion"]}")
    testImplementation("org.apache.kafka:kafka-clients:${extra["kafkaVersion"]}:test")
    testImplementation("org.apache.kafka:kafka-streams:${extra["kafkaVersion"]}")
    testImplementation("org.apache.kafka:kafka-streams:${extra["kafkaVersion"]}:test")
    testImplementation("org.scala-lang:scala-library:${extra["scalaVersion"]}")
    testImplementation("org.apache.kafka:kafka_${extra["scalaMajMinVersion"]}:${extra["kafkaVersion"]}")
    testImplementation("org.apache.kafka:kafka_${extra["scalaMajMinVersion"]}:${extra["kafkaVersion"]}:test")
    testImplementation("io.confluent:kafka-connect-avro-converter:${extra["confluentVersion"]}")
    testImplementation("io.confluent:kafka-schema-registry:${extra["confluentVersion"]}")
}

checkstyle {
    toolVersion = "7.4"
}

val gitVersion: String by lazy {
    val describeStdOut = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "describe", "--tags", "--always", "--dirty")
        standardOutput = describeStdOut
    }
    describeStdOut.toString().substring(1).trim()
}

buildConfig {
    appName = "mongo-kafka"
    version = gitVersion
    clsName = "Versions"
    packageName = "at.grahsl.kafka.connect.mongodb"
}


tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += sourceSets["main"].output + configurations["testRuntimeClasspath"]
        runtimeClasspath += output + compileClasspath + sourceSets["test"].runtimeClasspath
    }
}

tasks.create("integrationTest", Test::class.java) {
    description = "Runs the integration tests"
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    shouldRunAfter("test")
    outputs.upToDateWhen { false }
}


tasks.withType<Test> {
    tasks.getByName("check").dependsOn(this)
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    addTestListener(object : TestListener {
        override fun beforeTest(testDescriptor: TestDescriptor?) {}
        override fun beforeSuite(suite: TestDescriptor?) {}
        override fun afterTest(testDescriptor: TestDescriptor?, result: TestResult?) {}
        override fun afterSuite(d: TestDescriptor?, r: TestResult?) {
            if (d != null && r != null && d.parent == null) {
                val resultsSummary = """Tests summary:
                    | ${r.testCount} tests,
                    | ${r.successfulTestCount} succeeded,
                    | ${r.failedTestCount} failed,
                    | ${r.skippedTestCount} skipped""".trimMargin().replace("\n", "")

                val border = "=".repeat(resultsSummary.length)
                logger.lifecycle("\n${border}")
                logger.lifecycle("Test result: ${r.resultType}")
                logger.lifecycle(resultsSummary)
                logger.lifecycle("${border}\n")
            }
        }
    })
}
