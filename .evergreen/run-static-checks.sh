#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export JDK8="/opt/java/jdk8"
export JDK17="/opt/java/jdk17"
export JAVA_HOME=$JDK17

############################################
#            Main Program                  #
############################################

echo "Compiling and running checks"

# We always compile with the latest version of java
./gradlew -version
./gradlew -PxmlReports.enabled=true --info -x test -x integrationTest clean check jar testClasses javadoc
