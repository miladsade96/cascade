# Contributing

Thanks for helping me improve Cascade.

Please run `sbt test` before opening a pull request. If your change touches the Kafka protocol, I also need:

1. The matching change in `Compatibility.supported`.
2. A codec test for each new version and flexible tagged-field boundary.
3. A raw-socket integration test that checks framing and correlation IDs.
4. An end-to-end test with an independent Kafka client, when that client exposes the API.

I don't want Cascade to advertise compatibility that it can't prove. Before adding a protocol version, make sure every request and response field matches the Apache Kafka grammar. Please keep requests ordered inside each TCP connection and keep offset assignment serialized inside each partition.

If you're unsure about the behavior of an API, open an issue before building a large change. I'm happy to discuss the design first.
