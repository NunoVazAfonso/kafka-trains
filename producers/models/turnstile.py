"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # DONE: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # DONE: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            #f"com.nunovazafonso.kafka_trains.turnstile.{station_name}", # DONE: Come up with a better topic name
            "com.nunovazafonso.kafka_trains.turnstiles",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, #DONE: Uncomment once schema is defined
            num_partitions=1, #DONE
            num_replicas=1, #DONE
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("turnstile kafka integration incomplete - skipping")
        #
        #
        # DONE: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
        self.producer.produce(
           topic=self.topic_name,
           key_schema={"timestamp": self.time_millis()},
           value_schema={
                "station_id": self.station.station_id ,
                "station_name": self.station.name,
                "line": self.station.color.name # this is an enum
           },
        )
