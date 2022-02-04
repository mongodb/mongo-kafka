#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  JAVA_VERSION The version of java to use for the tests
JAVA_VERSION=${JAVA_VERSION:-17}

export JDK8="/opt/java/jdk8"
export JDK17="/opt/java/jdk17"
export JAVA_HOME=$JDK17

############################################
#            Main Program                  #
############################################

echo "Running unit tests on JDK${JAVA_VERSION}"

./gradlew -version
./gradlew -PjavaVersion=$JAVA_VERSION --stacktrace --info test

