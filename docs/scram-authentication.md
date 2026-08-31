# SCRAM authentication runbook

I use SCRAM-SHA-512 over `SASL_SSL` for password-authenticated Kafka clients. SCRAM proves knowledge of a password without sending that password to the broker, and Cascade stores only salted verifier material. SCRAM does not encrypt Kafka traffic, so I still require TLS outside isolated development environments.

## Generate verifier records offline

I put the password in a temporary, access-restricted file and run the credential tool outside the broker process:

```powershell
[IO.File]::WriteAllText('alice.password', 'replace-with-a-long-random-secret', [Text.UTF8Encoding]::new($false))
.\sbt.bat "runMain cascade.security.CredentialTool alice --password-file alice.password --mechanism SCRAM-SHA-512"
Remove-Item -LiteralPath alice.password
```

The tool prints one line shaped like this:

```text
SCRAM-SHA-512 alice=scram-sha-512$16384$<salt>$<stored-key>$<server-key>
```

I copy the complete output into `scram-users.conf`. Blank lines and `#` comments are allowed. I generate every record with the tool instead of assembling keys by hand. Cascade accepts 4,096 through 1,000,000 PBKDF2 iterations and the tool defaults to 16,384. I increase the count only after measuring authentication latency and CPU on the target hardware.

I can store separate SCRAM-SHA-256 and SCRAM-SHA-512 records for the same user. Each mechanism/user pair must be unique, and user names must contain 1-255 visible ASCII characters.

## Configure the broker and client

This is the listener shape I use:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9093 --advertised-host broker.example.com --data-dir data --security-protocol SASL_SSL --ssl-keystore broker.p12 --ssl-keystore-password-file broker-store.password --sasl-mechanisms SCRAM-SHA-512 --scram-credentials-file scram-users.conf --credential-reload-ms 1000 --acl-file acls.conf --audit-log security-audit.jsonl"
```

The matching Kafka client properties are:

```properties
bootstrap.servers=broker.example.com:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="replace-with-a-long-random-secret";
ssl.truststore.location=cluster-ca.p12
ssl.truststore.password=replace-with-the-truststore-password
ssl.truststore.type=PKCS12
group.protocol=classic
```

For a staged PLAIN-to-SCRAM migration, I use `--sasl-mechanisms PLAIN,SCRAM-SHA-512` and provide both `--credentials-file` and `--scram-credentials-file`. I move clients to SCRAM, verify the PLAIN counters stop advancing, and then remove PLAIN from the configured mechanism list and restart the listener.

## Rotate a password verifier

I generate the replacement record in a separate file, construct a complete replacement credential file, validate that every intended identity is present, and atomically replace `scram-users.conf` on the same filesystem. Cascade polls at `--credential-reload-ms` and swaps the entire parsed snapshot only after validation succeeds.

A malformed replacement does not displace the last valid in-memory snapshot. The broker stays live, the `credential_policy` readiness check fails, and existing valid credentials continue to work. I repair and atomically replace the file, wait for readiness to recover, and confirm the new password before retiring the old deployment secret. Existing authenticated connections remain authenticated until they close or their configured SASL session lifetime expires.

## Monitor authentication

Prometheus exposes these bounded-cardinality counters with `node_id` and `mechanism` labels:

- `cascade_sasl_authentication_successes_total`
- `cascade_sasl_authentication_failures_total`

The mechanism label is one of `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, or `UNKNOWN`. `/v1/status` also reports aggregate `sasl_authentication_successes` and `sasl_authentication_failures`. I alert on a sustained failure increase, investigate unexpected `UNKNOWN` traffic, and correlate it with the forced JSONL authentication audit. Audit entries include the negotiated mechanism but never the password, client proof, stored key, or server key.

## Bounds and compatibility

Cascade limits each SCRAM message to 16 KiB, each nonce to 512 visible characters, and each message to 16 attributes. It rejects duplicate attributes, invalid UTF-8/Base64, unsupported mandatory extensions, authorization identities, channel binding, reauthentication on an already authenticated connection, and messages after a completed exchange. Unknown-user exchanges use synthetic verifier work so they do not take an obvious fast failure path.

I test both mechanisms with Kafka 4.3.1 clients. The end-to-end suite covers SCRAM-SHA-256 interoperability, SCRAM-SHA-512 over TLS with ACL-protected produce/consume, correct server proofs, wrong-password and unknown-user denial, live rotation, malformed-file readiness, last-valid snapshot preservation, audit events, metrics, connection isolation, and protocol bounds.

## Current limitations

I do not yet implement SCRAM channel binding or Kafka `AlterUserScramCredentials`. TLS key/trust stores and SCRAM verifier snapshots do reload atomically with last-known-good retention. I support signed OAuth/OIDC JWTs as a separate client mechanism. SCRAM verifier management remains an operator-owned file workflow, so I use filesystem permissions and encrypted secret distribution for the verifier file, TLS key material, password inputs, and backup copies, and I track the remaining release gates in my [production-readiness checklist](production-readiness.md).
