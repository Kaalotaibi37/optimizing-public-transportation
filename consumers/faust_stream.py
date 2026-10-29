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


# Ingests the raw stations table from the Kafka Connect topic and keeps only the fields
# the web server needs, with the line color as a single string.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Kafka Connect writes to `<topic.prefix><table>` (see producers/connector.py)
topic = app.topic("org.chicago.cta.stations", value_type=Station)

# The web server consumes this topic (see consumers/server.py)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# The table's changelog is the output topic, so every update to the table is
# published there, keyed by station_id.
table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


def get_line(station):
    """Returns the line color of a raw station record, or None if it has none"""
    if station.red:
        return "red"
    if station.blue:
        return "blue"
    if station.green:
        return "green"
    return None


def transform(station):
    """Transforms a raw `Station` record into a `TransformedStation` record"""
    line = get_line(station)
    if line is None:
        return None
    return TransformedStation(
        station_id=station.station_id,
        station_name=station.station_name,
        order=station.order,
        line=line,
    )


@app.agent(topic)
async def process_stations(stations):
    """Transforms each incoming station and stores it in the table"""
    async for station in stations:
        transformed = transform(station)
        if transformed is None:
            logger.warning("station %s has no line color - skipping", station.stop_id)
            continue
        table[transformed.station_id] = transformed


if __name__ == "__main__":
    app.main()
