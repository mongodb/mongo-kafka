FROM confluentinc/cp-kafka-connect:5.2.2

ENV CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components"

RUN  confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
