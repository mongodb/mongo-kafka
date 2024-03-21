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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    `java-library`
    `maven-publish`
    signing
    id("com.github.gmazzo.buildconfig") version "3.0.3"
    id("com.github.spotbugs") version "4.7.9"
    id("com.diffplug.spotless") version "5.17.1"
    id("com.github.johnrengelman.shadow") version "6.1.0"
}

group = "org.mongodb.kafka"
version = "1.12.0"
description = "The official MongoDB Apache Kafka Connect Connector."

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
    maven("https://jitpack.io")
}

extra.apply {
    set("mongodbDriverVersion", "[4.7,4.7.99)")
    set("kafkaVersion", "2.6.0")
    set("avroVersion", "1.9.2")

    // Testing dependencies
    set("junitJupiterVersion", "5.8.1")
    set("junitPlatformVersion", "1.8.1")
    set("hamcrestVersion", "2.2")
    set("mockitoVersion", "4.0.0")

    // Integration test dependencies
    set("confluentVersion", "6.0.1")
    set("scalaVersion", "2.13")
    set("curatorVersion", "2.9.0")
    set("connectUtilsVersion", "0.4+")
}

val mongoDependencies: Configuration by configurations.creating
val mongoAndAvroDependencies: Configuration by configurations.creating

dependencies {
    implementation("org.apache.kafka:connect-api:${project.extra["kafkaVersion"]}")
    implementation("org.mongodb:mongodb-driver-sync:${project.extra["mongodbDriverVersion"]}")
    implementation("org.apache.avro:avro:${project.extra["avroVersion"]}")

    mongoDependencies("org.mongodb:mongodb-driver-sync:${project.extra["mongodbDriverVersion"]}")

    mongoAndAvroDependencies("org.mongodb:mongodb-driver-sync:${project.extra["mongodbDriverVersion"]}")
    mongoAndAvroDependencies("org.apache.avro:avro:${project.extra["avroVersion"]}")

    // Unit Tests
    testImplementation(platform("org.junit:junit-bom:${project.extra["junitJupiterVersion"]}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.platform:junit-platform-runner")
    testImplementation("org.apiguardian:apiguardian-api:1.1.2") // https://github.com/gradle/gradle/issues/18627
    testImplementation("org.hamcrest:hamcrest:${project.extra["hamcrestVersion"]}")
    testImplementation("org.mockito:mockito-junit-jupiter:${project.extra["mockitoVersion"]}")

    // Integration Tests
    testImplementation("org.apache.curator:curator-test:${project.extra["curatorVersion"]}")
    testImplementation("com.github.jcustenborder.kafka.connect:connect-utils:${project.extra["connectUtilsVersion"]}")
    testImplementation(platform("io.confluent:kafka-schema-registry-parent:${project.extra["confluentVersion"]}"))
    testImplementation(group = "com.google.guava", name = "guava")
    testImplementation(group = "io.confluent", name = "kafka-schema-registry")
    testImplementation(group = "io.confluent", name = "kafka-connect-avro-converter")
    testImplementation(group = "org.apache.kafka", name = "connect-runtime")
    testImplementation(group = "org.apache.kafka", name = "kafka-clients", classifier = "test")
    testImplementation(group = "org.apache.kafka", name = "kafka-streams")
    testImplementation(group = "org.apache.kafka", name = "kafka-streams", classifier = "test")
    testImplementation(group = "org.scala-lang", name = "scala-library")
    testImplementation(group = "org.apache.kafka", name = "kafka_${project.extra["scalaVersion"]}")
    testImplementation(group = "org.apache.kafka", name = "kafka_${project.extra["scalaVersion"]}", classifier = "test")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(8)
}

val defaultJdkVersion = 17
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(defaultJdkVersion))
    }
}

/*
 * Generated files
 */
val gitVersion: String by lazy {
    val describeStdOut = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "describe", "--tags", "--always", "--dirty")
        standardOutput = describeStdOut
    }
    describeStdOut.toString().substring(1).trim()
}

val gitDiffNameOnly: String by lazy {
    val describeStdOut = ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "diff", "--name-only")
        standardOutput = describeStdOut
    }
    describeStdOut.toString().replaceIndent(" - ")
}

buildConfig {
    className("Versions")
    packageName("com.mongodb.kafka.connect")
    useJavaOutput()
    buildConfigField("String", "NAME", "\"mongo-kafka\"")
    buildConfigField("String", "VERSION", provider { "\"${gitVersion}\"" })
}

/*
 * Testing
 */

sourceSets.create("integrationTest") {
    java.srcDir("src/integrationTest/java")
    compileClasspath += sourceSets["main"].output + configurations["testRuntimeClasspath"]
    runtimeClasspath += output + compileClasspath + sourceSets["test"].runtimeClasspath
}

tasks.create("integrationTest", Test::class.java) {
    description = "Runs the integration tests"
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    outputs.upToDateWhen { false }
    mustRunAfter("test")
}

tasks.withType<Test> {
    tasks.getByName("check").dependsOn(this)
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }

    val javaVersion: Int = (project.findProperty("javaVersion") as String? ?: defaultJdkVersion.toString()).toInt()
    logger.info("Running tests using JDK$javaVersion")
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(javaVersion))
    })

    systemProperties(mapOf("org.mongodb.test.uri" to System.getProperty("org.mongodb.test.uri", "")))

    val jdkHome = project.findProperty("jdkHome") as String?
    jdkHome.let {
        val javaExecutablesPath = File(jdkHome, "bin/java")
        if (javaExecutablesPath.exists()) {
            executable = javaExecutablesPath.absolutePath
        }
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
                logger.lifecycle("\n$border")
                logger.lifecycle("Test result: ${r.resultType}")
                logger.lifecycle(resultsSummary)
                logger.lifecycle("${border}\n")
            }
        }
    })
}

spotbugs {
    excludeFilter.set(project.file("config/spotbugs-exclude.xml"))
    showProgress.set(true)
    setReportLevel("high")
    setEffort("max")
}

tasks.withType<com.github.spotbugs.snom.SpotBugsTask> {
    enabled = baseName.equals("main")
    reports.maybeCreate("html").isEnabled = !project.hasProperty("xmlReports.enabled")
    reports.maybeCreate("xml").isEnabled = project.hasProperty("xmlReports.enabled")
}

// Spotless is used to lint and reformat source files.
spotless {
    java {
        googleJavaFormat("1.12.0")
        importOrder("java", "io", "org", "org.bson", "com.mongodb", "com.mongodb.kafka", "")
        removeUnusedImports() // removes any unused imports
        trimTrailingWhitespace()
        endWithNewline()
        indentWithSpaces()
    }

    kotlinGradle {
        ktlint("0.30.0")
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }

    format("extraneous") {
        target("*.xml", "*.yml", "*.md")
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }
}

tasks.named("compileJava") {
    dependsOn(":spotlessApply")
}

/*
 * ShadowJar
 */
tasks.register<ShadowJar>("confluentJar") {
    archiveClassifier.set("confluent")
    from(mongoDependencies, sourceSets.main.get().output)
}

tasks.register<ShadowJar>("allJar") {
    archiveClassifier.set("all")
    from(mongoAndAvroDependencies, sourceSets.main.get().output)
}

tasks.withType<ShadowJar> {
    archiveAppendix.set("connect")
    doLast {
        val fatJar = archiveFile.get().asFile
        val fatJarSize = "%.4f".format(fatJar.length().toDouble() / (1_000 * 1_000))
        println("FatJar: ${fatJar.path} ($fatJarSize MB)")
    }

    // Disable the default shadowJar task
    tasks.named("shadowJar").configure {
        enabled = false
    }
}

/*
 * Publishing
 */
tasks.register<Jar>("sourcesJar") {
    description = "Create the sources jar"
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
}

tasks.register<Jar>("javadocJar") {
    description = "Create the Javadoc jar"
    from(tasks.javadoc)
    archiveClassifier.set("javadoc")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "mongo-kafka-connect"
            from(components["java"])
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])
            artifact(tasks["confluentJar"])
            artifact(tasks["allJar"])

            pom {
                name.set(project.name)
                description.set(project.description)
                url.set("http://www.mongodb.org")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("Various")
                        organization.set("MongoDB")
                    }
                    developer {
                        id.set("Hans-Peter Grahsl")
                    }
                }
                scm {
                    connection.set("scm:https://github.com/mongodb/mongo-kafka.git")
                    developerConnection.set("scm:git@github.com:mongodb/mongo-kafka.git")
                    url.set("https://github.com/mongodb/mongo-kafka")
                }
            }
        }
    }

    repositories {
        maven {
            val snapshotsRepoUrl = URI("https://oss.sonatype.org/content/repositories/snapshots/")
            val releasesRepoUrl = URI("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials {
                val nexusUsername: String? by project
                val nexusPassword: String? by project
                username = nexusUsername ?: ""
                password = nexusPassword ?: ""
            }
        }
    }
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["mavenJava"])
}

tasks.javadoc {
    if (JavaVersion.current().isJava9Compatible) {
        (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    }
}

tasks.register("publishSnapshots") {
    group = "publishing"
    description = "Publishes snapshots to Sonatype"
    if (version.toString().endsWith("-SNAPSHOT")) {
        dependsOn(tasks.withType<PublishToMavenRepository>())
    }
}

tasks.register("publishArchives") {
    group = "publishing"
    description = "Publishes a release and uploads to Sonatype / Maven Central"

    doFirst {
        if (gitVersion != version) {
            val cause = """
                | Version mismatch:
                | =================
                |
                | $version != $gitVersion
                |
                | Modified Files:
                |$gitDiffNameOnly
                |
                | The project version does not match the git tag.
                |""".trimMargin()
            throw GradleException(cause)
        } else {
            println("Publishing: ${project.name} : $gitVersion")
        }
    }

    if (gitVersion == version) {
        dependsOn(tasks.withType<PublishToMavenRepository>())
    }
}

// Confluent Archive
val releaseDate by extra(DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDateTime.now()))
val archiveFilename = "mongodb-kafka-connect-mongodb"
tasks.register<Copy>("prepareConfluentArchive") {
    group = "Confluent"
    description = "Prepares the Confluent Archive ready for the hub"
    dependsOn("confluentJar")

    val baseDir = "$archiveFilename-${project.version}"
    from("config/archive/manifest.json") {
        expand(project.properties)
        destinationDir = file("$buildDir/confluentArchive/$baseDir")
    }

    from("config/archive/assets") {
        into("assets")
    }

    from("config") {
        include(listOf("MongoSinkConnector.properties", "MongoSourceConnector.properties"))
        into("etc")
    }

    from("$buildDir/libs") {
        include(listOf("${project.name}-connect-${project.version}-confluent.jar"))
        into("lib")
    }

    from(".") {
        include(listOf("README.md", "LICENSE.txt"))
        into("doc")
    }
}

tasks.register<Zip>("createConfluentArchive") {
    group = "Confluent"
    description = "Creates the Confluent Archive zipfile to be uploaded to the Confluent Hub"
    dependsOn("prepareConfluentArchive")
    from(files("$buildDir/confluentArchive"))
    archiveBaseName.set("")
    archiveAppendix.set(archiveFilename)
    archiveVersion.set(project.version.toString())
    destinationDirectory.set(file("$buildDir/confluent"))
}
