"""
Wrapper for SchemaRegistryClient, to separate Feast from
the extensive auth and configuration process of
connecting to a SchemaRegistry.

Copyright 2024 Expedia Group
Author: matcarlin@expediagroup.com
"""

import json
import os
import tempfile
from typing import Dict

import requests
from confluent_kafka.schema_registry import RegisteredSchema, SchemaRegistryClient


class SchemaRegistry():
    props: Dict[str, str]
    kafka_params: Dict[str, str]
    schema_registry_config: Dict[str, str]
    client: SchemaRegistryClient

    def __init__(self):
        pass

    def initialize_client(
        self,
        user: str,
        password: str,
        urn: str,
        environment: str,
        cert_path: str,  # https://stackoverflow.com/questions/55203791/python-requests-using-certificate-value-instead-of-path
    ) -> None:
        """
        Discover a Schema Registry with the provided urn and credentials,
        obtain a set of properties for use in Schema Registry calls,
        and initialize the SchemaRegistryClient.
        """

        discovery_url = "https://stream-discovery-service-{environment}.rcp.us-east-1.data.{environment}.exp-aws.net/v2/discovery/urn/{urn}".format(
            environment=environment, urn=urn
        )

        response = requests.get(
            discovery_url,
            auth=(user, password),
            headers={"Accept": "application/json"},
            verify=cert_path,
        )

        if response.status_code != 200:
            raise RuntimeError(
                "Discovery API returned unexpected HTTP status: {status}".format(
                    status=str(response.status_code)
                )
            )

        try:
            props = json.loads(response.text)
        except (TypeError, UnicodeDecodeError):
            raise TypeError(
                "Discovery API response did not contain valid json: {response}".format(
                    response=response.text
                )
            )

        self.props = props

        # write ssl key and cert to disk
        ssl_key_file, ssl_key_path = tempfile.mkstemp()
        with os.fdopen(ssl_key_file, "w") as f:
            f.write(props["serde"]["schema.registry.ssl.keystore.key"])

        ssl_certificate_file, ssl_certificate_path = tempfile.mkstemp()
        with os.fdopen(ssl_certificate_file, "w") as f:
            f.write(props["serde"]["schema.registry.ssl.keystore.certificate.chain"])

        self.kafka_params = {
            "kafka.security.protocol": props["security"]["security.protocol"],
            "kafka.bootstrap.servers": props["connection"]["bootstrap.servers"],
            "subscribe": props["connection"]["topic"],
            "startingOffsets": props["connection"]["auto.offset.reset"],
            "kafka.ssl.truststore.certificates": props["security"][
                "ssl.truststore.certificates"
            ],
            "kafka.ssl.keystore.certificate.chain": props["security"][
                "ssl.keystore.certificate.chain"
            ],
            "kafka.ssl.keystore.key": props["security"]["ssl.keystore.key"],
            "kafka.ssl.endpoint.identification.algorithm": props["security"][
                "ssl.endpoint.identification.algorithm"
            ],
            "kafka.ssl.truststore.type": props["security"]["ssl.truststore.type"],
            "kafka.ssl.keystore.type": props["security"]["ssl.keystore.type"],
            "kafka.topic": props["connection"]["topic"],
            "kafka.schema.registry.url": props["serde"]["schema.registry.url"],
            "kafka.schema.registry.topic": props["connection"]["topic"],
            "kafka.schema.registry.ssl.keystore.type": props["serde"][
                "schema.registry.ssl.keystore.type"
            ],
            "kafka.schema.registry.ssl.keystore.certificate.chain": props["serde"][
                "schema.registry.ssl.keystore.certificate.chain"
            ],
            "kafka.schema.registry.ssl.keystore.key": props["serde"][
                "schema.registry.ssl.keystore.key"
            ],
            "kafka.schema.registry.ssl.truststore.certificates": props["serde"][
                "schema.registry.ssl.truststore.certificates"
            ],
            "kafka.schema.registry.ssl.truststore.type": props["serde"][
                "schema.registry.ssl.truststore.type"
            ],
            "value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
        }

        self.schema_registry_config = {
            "schema.registry.topic": props["connection"]["topic"],
            "schema.registry.url": props["serde"]["schema.registry.url"],
            "schema.registry.ssl.keystore.type": props["serde"][
                "schema.registry.ssl.keystore.type"
            ],
            "schema.registry.ssl.keystore.certificate.chain": props["serde"][
                "schema.registry.ssl.keystore.certificate.chain"
            ],
            "schema.registry.ssl.keystore.key": props["serde"][
                "schema.registry.ssl.keystore.key"
            ],
            "schema.registry.ssl.truststore.certificates": props["serde"][
                "schema.registry.ssl.truststore.certificates"
            ],
            "schema.registry.ssl.truststore.type": props["serde"][
                "schema.registry.ssl.truststore.type"
            ],
        }

        schema_registry_url = props["serde"]["schema.registry.url"]

        self.client = SchemaRegistryClient(
            {
                "url": schema_registry_url,
                "ssl.ca.location": cert_path,
                "ssl.key.location": ssl_key_path,
                "ssl.certificate.location": ssl_certificate_path,
            }
        )

    def get_latest_version(
        self,
        topic_name: str,
    ) -> RegisteredSchema:
        """
        Get the latest version of the topic.
        """
        if not self.client:
            raise RuntimeError("Client has not been initialized. Please call initialize_client first.")

        latest_version = self.client.get_latest_version(topic_name)

        return latest_version

    def get_client(
        self
    ) -> SchemaRegistryClient:
        """
        Return the client.
        """
        if not self.client:
            raise RuntimeError("Client has not been initialized. Please call initialize_client first.")

        return self.client
