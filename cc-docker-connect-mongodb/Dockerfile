ARG DOCKER_REGISTRY
ARG BASE_VERSION

FROM ${DOCKER_REGISTRY}confluentinc/cc-built-base:${BASE_VERSION}

COPY \
    kafka-connect-mongodb/build/confluent/kafka-connect-mongodb/lib \
    /usr/share/java/connectors/plugins/kafka-connect-mongodb
