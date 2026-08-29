FROM alpine:3.22 AS connector-download

ARG SNOWFLAKE_CONNECTOR_VERSION=4.1.0
ARG SNOWFLAKE_CONNECTOR_SHA256=4366bd5d4fd0f49f28706a71d0c770d5d375305bc3bd5855fca9a1fb9ae20ce2
ARG SNOWFLAKE_CONNECTOR_URL=https://hub-downloads.confluent.io/api/plugins/snowflakeinc/snowflake-kafka-connector/versions/${SNOWFLAKE_CONNECTOR_VERSION}/snowflakeinc-snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}.zip

RUN apk add --no-cache ca-certificates curl unzip \
    && curl --fail --location --retry 3 \
        --output /tmp/snowflake-kafka-connector.zip \
        "${SNOWFLAKE_CONNECTOR_URL}" \
    && echo "${SNOWFLAKE_CONNECTOR_SHA256}  /tmp/snowflake-kafka-connector.zip" \
        | sha256sum --check \
    && mkdir -p /opt/connectors \
    && unzip -q /tmp/snowflake-kafka-connector.zip -d /opt/connectors \
    && test -f "/opt/connectors/snowflakeinc-snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}/lib/snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}.jar"

FROM quay.io/debezium/connect:3.6.1.Final

ARG SNOWFLAKE_CONNECTOR_VERSION=4.1.0

COPY --from=connector-download \
    /opt/connectors/snowflakeinc-snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}/ \
    /kafka/connect/snowflake-kafka-connector/
