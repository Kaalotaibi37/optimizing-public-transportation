"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


# All turnstiles publish to one shared topic so that KSQL can build a single
# `turnstile` table from it (see consumers/ksql.py). Each event carries the
# station_id, station_name and line, so every station is represented in the topic.
TURNSTILE_TOPIC = "org.chicago.cta.turnstiles"


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            TURNSTILE_TOPIC,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        # One event per rider entry; KSQL counts the events per station
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name,
                },
            )
        self.producer.poll(0)
        logger.debug(
            "turnstile: %s entries at station %s (%s)",
            num_entries,
            self.station.station_id,
            self.station.name,
        )
