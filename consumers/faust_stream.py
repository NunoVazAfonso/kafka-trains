"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# DONE: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# DONE: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic(
    "com.nunovazafonso.kafka_trains.stations", 
    value_type=Station
)
# DONE: Define the output Kafka Topic
out_topic = app.topic(
    "com.nunovazafonso.kafka_trains.stations_processed", 
    partitions=1, 
    value_type=TransformedStation
)
# DONE: Define a Faust Table
table = app.Table(
    "com.nunovazafonso.kafka_trains.stations_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

@app.agent(topic)
async def station(stations):
    async for s in stations: 
        table[s.station_id] = TransformedStation(
            station_id = s.stop_id,
            station_name = s.stop_name,
            order = s.order,
            line = "red" if s.red else "blue" if s.blue else "green"
        )
        #await out_topic.send(key=out_s.station_name, value=out_s) 

if __name__ == "__main__":
    app.main()
