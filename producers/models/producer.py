"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        # DONE: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "client.id" : "trains-arrival-producer",
            "linger.ms" : 1000,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer 
        schema_registry = CachedSchemaRegistryClient( SCHEMA_REGISTRY_URL )

        self.producer = AvroProducer(
            {"bootstrap.servers": BROKER_URL},
            schema_registry= SCHEMA_REGISTRY_URL 
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        # DONE: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        client = AdminClient({"bootstrap.servers": BROKER_URL})

        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        
        if self.topic_name not in set(t.topic for t in iter(topic_metadata.topics.values())):
            topics_created = client.create_topics([
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=10,
                    replication_factor=1,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": "2000",
                        "file.delete.delay.ms": "2000",
                    }
                )
            ])

            for topic, future in topics_created.items():
                try:
                    future.result()
                    print("topic created")
                except Exception as e:
                    print(f"failed to create topic {self.topic_name}: {e}")


        logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
