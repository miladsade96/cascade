# Kafka magic-v2 integrity

I treat every Kafka magic-v2 batch as untrusted bytes until its complete envelope and CRC32C are valid. This rule applies to client Produce requests, restart recovery, index reconstruction, and compaction output.

## CRC contract

Kafka stores the CRC32C at byte offset 17. I calculate it over the bytes from `attributes` at offset 21 through the end of the declared batch. The broker-assigned `baseOffset`, `batchLength`, `partitionLeaderEpoch`, `magic`, and stored CRC are outside that coverage. Cascade can therefore assign a base offset without changing the producer's CRC, but any change to attributes, timestamps, producer state, record count, or payload requires a new CRC.

Before I trust a batch, Cascade checks all of these conditions:

- the complete fixed header is present;
- `batchLength + 12` exactly matches the available batch bytes;
- magic is exactly 2;
- offset and record counters are not negative;
- the compression identifier is one of none, Gzip, Snappy, LZ4, or Zstd; and
- the stored CRC32C exactly matches the computed value.

A corrupt Produce request gets Kafka error `CORRUPT_MESSAGE` (code 2), consumes no offset, and leaves the connection usable. Cascade assigns the broker offset only after the validation succeeds.

## Safe compaction rewrite

Compaction keeps the original base offset and immutable producer/header fields. It changes only the compressed payload, declared batch length, retained record count, and CRC32C. Record decoding is bounded by both the decompressed-batch limit and each record's own declared boundary. Offset deltas must stay within the batch range and increase strictly, header counts must fit inside the record, timestamps must not overflow, and variable-length integers must fit their target type.

After re-encoding, Cascade recalculates CRC32C, validates the new envelope, decompresses it again, and confirms the decoded record count before the atomic segment replacement can proceed. A malformed or undecodable compactable batch remains opaque instead of being rewritten.

## Recovery boundary

An incomplete active tail can be truncated after a crash. A complete CRC-invalid batch is repairable only when it is the final physical batch of the final active segment. CRC corruption inside the durable prefix fails startup and preserves the segment bytes for investigation. I do not silently discard a valid suffix after interior corruption.

This policy detects corruption and prevents unsafe rewriting. It does not replace filesystem checksums, ECC memory, redundant replicas, backups, or physical power/device-loss qualification.
