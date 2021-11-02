#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export JDK8="/opt/java/jdk8"
export JDK17="/opt/java/jdk17"
export JAVA_HOME=$JDK17

############################################
#            Main Program                  #
############################################

echo "Running unit tests on JDK17 and JDK8"

./gradlew -version
./gradlew --stacktrace --info test
./gradlew --stacktrace --info testsOnJava8
