# Optimizing Public Transportation

A streaming event pipeline built with Apache Kafka for the Udacity Data Streaming Nanodegree (Project 1). It simulates the Chicago Transit Authority (CTA) train system and shows live train status on a web page.

- **Producers:** train arrivals and turnstile entries go to Kafka as Avro events, and weather events come in through the Kafka REST Proxy.
- **Kafka Connect:** loads station data from PostgreSQL into Kafka.
- **Faust and KSQL:** Faust transforms the station data, and KSQL counts turnstile entries per station.
- **Web app:** a Tornado server reads all topics and shows line status, train arrivals and weather in real time.
