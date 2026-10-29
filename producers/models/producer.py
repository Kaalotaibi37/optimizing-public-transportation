"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


# Host URLs (see README). The producers run on the host, outside of Docker Compose.
BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    # One AvroProducer per (key schema, value schema) pair, shared by all instances.
    # The simulation creates ~230 Station/Turnstile objects; sharing the client keeps
    # the number of broker connections and librdkafka threads small.
    _avro_producers = {}

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

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            # Batch small messages together and compress them to reduce broker load
            "linger.ms": 100,
            "compression.type": "lz4",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = self._get_avro_producer()

    def _get_avro_producer(self):
        """Returns the shared AvroProducer configured for this producer's schemas"""
        cache_key = (str(self.key_schema), str(self.value_schema))
        if cache_key not in Producer._avro_producers:
            Producer._avro_producers[cache_key] = AvroProducer(
                dict(self.broker_properties),
                default_key_schema=self.key_schema,
                default_value_schema=self.value_schema,
            )
        return Producer._avro_producers[cache_key]

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": BROKER_URL})

        topic_metadata = client.list_topics(timeout=5)
        if self.topic_name in topic_metadata.topics:
            logger.debug("topic %s already exists - skipping creation", self.topic_name)
            return

        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": "2000",
                        "file.delete.delay.ms": "2000",
                    },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic %s created", topic)
            except Exception as e:
                # Another producer may have created the topic in the meantime
                logger.info("topic %s not created: %s", topic, e)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if getattr(self, "producer", None) is not None:
            # Deliver any buffered messages before shutting down
            self.producer.flush(timeout=10)
        logger.debug("producer for %s closed", self.topic_name)

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
