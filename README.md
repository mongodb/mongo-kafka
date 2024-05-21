# MongoDB Kafka Connector

The official MongoDB Kafka Connector.


## Documentation

Documentation for the connector is available on [https://docs.mongodb.com/kafka-connector/current/](https://docs.mongodb.com/kafka-connector/current/)

## Downloading

The connector will be published on [maven central](https://search.maven.org/search?q=g:org.mongodb.kafka%20AND%20a:mongo-kafka-connect).

## Support / Feedback

For issues with, questions about, or feedback for the MongoDB Kafka Connector, please look into our
[support channels](http://www.mongodb.org/about/support). Please do not email any of the Kafka connector developers directly with issues or
questions - you're more likely to get an answer on the
[MongoDB Community Forums](https://community.mongodb.com/tags/c/drivers-odms-connectors/7/kafka-connector).

At a minimum, please include in your description the exact version of the driver that you are using.  If you are having
connectivity issues, it's often also useful to paste in the Kafka connector configuration. You should also check your application logs for
any connectivity-related exceptions and post those as well.

## Bugs / Feature Requests

Think you’ve found a bug? Want to see a new feature in the Kafka driver? Please open a case in our issue management tool, JIRA:

- [Create an account and login](https://jira.mongodb.org).
- Navigate to [the KAFKA project](https://jira.mongodb.org/browse/KAFKA).
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for the connector are **public**.

If you’ve identified a security vulnerability in a connector or any other MongoDB project, please report it according to the
[instructions here](https://docs.mongodb.com/manual/tutorial/create-a-vulnerability-report/).

## Versioning

The MongoDB Kafka Connector follows semantic versioning.
See the [changelog](./CHANGELOG.md) for information about changes between releases.

## Build

### Note: The following instructions are intended for internal use.

Java 8+ is required to build and compile the source. To build and test the driver:

```
$ git clone https://github.com/mongodb/mongo-kafka.git
$ cd mongo-kafka
$ ./gradlew check -Dorg.mongodb.test.uri=mongodb://localhost:27017
```

The test suite requires mongod to be running. Note, the source connector requires a replicaSet.

## Maintainers

* Ross Lawley          ross@mongodb.com

Original Sink connector work by: Hans-Peter Grahsl : https://github.com/hpgrahsl/kafka-connect-mongodb

Additional contributors can be found [here](https://github.com/mongodb/mongo-kafka/graphs/contributors).

## Release process

- `./gradlew publishArchives` - publishes to Maven
-  `./gradlew createConfluentArchive` - creates the confluent archive / github release zip file

## IntelliJ IDEA

A couple of manual configuration steps are required to run the code in IntelliJ:

  - **Error:** `java: cannot find symbol. symbol: variable Versions`<br>
    **Fixes:** Any of the following: <br>
      - Run the `compileBuildConfig` task: eg: `./gradlew compileBuildConfig` or via Gradle > mongo-kafka > Tasks > other > compileBuildConfig
      - Set `compileBuildConfig` to execute Before Build. via Gradle > Tasks > other > right click compileBuildConfig - click on "Execute Before Build"
      - Delegate all build actions to Gradle: Settings > Build, Execution, Deployment > Build Tools > Gradle > Runner - tick "Delegate IDE build/run actions to gradle"

## Custom Auth Provider Interface

The `com.mongodb.kafka.connect.util.custom.credentials.CustomCredentialProvider` interface can be implemented to provide an object of type `com.mongodb.MongoCredential` which gets wrapped in the MongoClient that is constructed for the sink and source connector.
The following properties need to be set -

```
mongo.custom.auth.mechanism.enable - set to true.
mongo.custom.auth.mechanism.providerClass - qualified class name of the implementation class
```
Additional properties and can be set as required within the implementation class.
The init and validate methods of the implementation class get called when the connector initializes.

### Example
When using MONGODB-AWS authentication mechanism for atlas, one can specify the following configuration -

```
"connection.uri": "mongodb+srv://<sever>/?authMechanism=MONGODB-AWS"
"mongo.custom.auth.mechanism.enable": true,
"mongo.custom.auth.mechanism.providerClass": "sample.AwsAssumeRoleCredentialProvider"
"mongodbaws.auth.mechanism.roleArn": "arn:aws:iam::<ACCOUNTID>:role/<ROLENAME>"
```
Here the `sample.AwsAssumeRoleCredentialProvider` must be available on the classpath. `mongodbaws.auth.mechanism.roleArn` is an example of custom properties that can be read by `sample.AwsAssumeRoleCredentialProvider`.

### Sample code for implementing Custom role provider
Here is sample code that can work.

```java
public class AwsAssumeRoleCredentialProvider implements CustomCredentialProvider {

  public AwsAssumeRoleCredentialProvider() {}
  @Override
  public MongoCredential getCustomCredential(Map<?, ?> map) {
    AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
    Supplier<AwsCredential> awsFreshCredentialSupplier = () -> {
      AWSSecurityTokenService stsClient = AWSSecurityTokenServiceAsyncClientBuilder.standard()
          .withCredentials(provider)
          .withRegion("us-east-1")
          .build();
      AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withDurationSeconds(3600)
          .withRoleArn((String)map.get("mongodbaws.auth.mechanism.roleArn"))
          .withRoleSessionName("Test_Session");
      AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);
      Credentials creds = assumeRoleResult.getCredentials();
      // Add your code to fetch new credentials
      return new AwsCredential(creds.getAccessKeyId(), creds.getSecretAccessKey(), creds.getSessionToken());
    };
    return MongoCredential.createAwsCredential(null, null)
        .withMechanismProperty(MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY, awsFreshCredentialSupplier);
  }

  @Override
  public void validate(Map<?, ?> map) {
    String roleArn = (String) map.get("mongodbaws.auth.mechanism.roleArn");
    if (StringUtils.isNullOrEmpty(roleArn)) {
      throw new RuntimeException("Invalid value set for customProperty");
    }
  }

  @Override
  public void init(Map<?, ?> map) {

  }
}
```
### pom file to build the sample CustomRoleProvider into a jar
Here is the pom.xml that can build the complete jar containing the AwsAssumeRoleCredentialProvider

```java
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>sample</groupId>
    <artifactId>AwsAssumeRoleCredentialProvider</artifactId>
    <version>1.0-SNAPSHOT</version>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.3</version>
                <configuration>
                    <!-- put your configurations here -->
                </configuration>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

    <dependencies>
        <!-- Java MongoDB Driver dependency -->
        <!-- https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-sync -->
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-sync</artifactId>
            <version>5.1.0</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk -->
        <dependency>
            <groupId>com.amazonaws</groupId>
            <artifactId>aws-java-sdk</artifactId>
            <version>1.12.723</version>
        </dependency>

        <!-- slf4j logging dependency, required for logging output from the MongoDB Java Driver -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-jdk14</artifactId>
            <version>1.7.28</version>
        </dependency>

        <dependency>
            <groupId>kafka-connect</groupId>
            <artifactId>kafka-connect</artifactId>
            <scope>system</scope>
            <version>1.12.1-SNAPSHOT</version>
            <systemPath>/Users/jagadish.nallapaneni/mongo-kafka/build/libs/mongo-kafka-connect-1.12.1-SNAPSHOT-confluent.jar</systemPath>
        </dependency>
    </dependencies>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

</project>
```
