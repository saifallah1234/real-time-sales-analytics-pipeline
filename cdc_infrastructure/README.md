# Local CDC infrastructure

The Kafka Connect image is built from the pinned Debezium image and installs the
complete Snowflake Kafka connector distribution from Confluent Hub. The archive
checksum is verified during the build, so downloaded connector JARs do not need
to be stored in this repository.

Build and start the services from the repository root:

```bash
docker compose build connect
docker compose up -d
```

To upgrade the Snowflake connector, update all three values together:

1. `SNOWFLAKE_CONNECTOR_VERSION` in `connect/Dockerfile`.
2. `SNOWFLAKE_CONNECTOR_SHA256` in `connect/Dockerfile`.
3. The version in the local image name in `docker-compose.yaml`.

Calculate the checksum from the exact Confluent Hub archive before committing
the upgrade. Never commit `secrets/` or `snowflake-sink.json`; the latter
contains the Snowflake private key used by the connector.
