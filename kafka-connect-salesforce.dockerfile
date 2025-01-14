FROM confluentinc/cp-kafka-connect:5.2.5

#If you want to run a private build uncomment the COPY command and make sure the JAR file is in the directory path
# COPY mongo-kafka-connect-<<INSERT BUILD HERE>>3-all.jar /usr/share/confluent-hub-components

# If you want to run the latest production release of the connector make sure the following is not commented:
# RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

COPY target /usr/share/confluent-hub-components