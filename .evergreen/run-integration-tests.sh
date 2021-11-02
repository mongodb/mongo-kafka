#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)

MONGODB_URI=${MONGODB_URI:-}
export JDK8="/opt/java/jdk8"
export JDK17="/opt/java/jdk17"
export JAVA_HOME=$JDK17

############################################
#            Main Program                  #
############################################


echo "Running tests connecting to $MONGODB_URI on JDK17"

./gradlew -version
./gradlew -Dorg.mongodb.test.uri=${MONGODB_URI} --stacktrace --info integrationTest
