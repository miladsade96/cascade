# Contributing

Run `sbt test` before submitting changes. Protocol changes must include:

1. An update to the `Compatibility.supported` matrix.
2. A codec test for every added version and flexible tagged-field boundary.
3. A raw-socket integration test for framing and correlation IDs.
4. An end-to-end test using an independent Kafka client when that client exposes the API.

Do not advertise a protocol version until every request and response field for that version is encoded exactly according to the Apache Kafka grammar. Preserve request order within a TCP connection and serialize offset assignment within a partition.

