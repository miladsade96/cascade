# Broker-to-broker security runbook

I use mutual TLS for every controller, metadata, replication, and recovery request between Cascade brokers. TLS validates the trust chain and advertised hostname; Cascade then binds the node ID claimed in the internal Kafka client ID to the certificate subject in my peer identity policy.

## Certificate requirements

I issue every broker a distinct private key and certificate from the same cluster CA. Each certificate has both `serverAuth` and `clientAuth` extended-key usages and a subject alternative name matching the broker's advertised host. I never reuse one certificate across node IDs.

My trust store contains the issuing CA, not arbitrary leaf certificates. I keep key-store and trust-store passwords in files readable only by the service identity. Cascade loads TLS keys and trust roots at startup, so I rotate that material with a rolling restart.

## Identity policy

My identity file contains one node ID followed by one RFC 2253 X.500 subject per line. Blank lines and lines beginning with `#` are ignored:

```text
1 CN=cascade-1,OU=Production,O=Example Corp
2 CN=cascade-2,OU=Production,O=Example Corp
3 CN=cascade-3,OU=Production,O=Example Corp
```

Cascade canonicalizes each subject before comparing it. I can assign more than one certificate subject to the same node during rotation, but I cannot assign one subject to multiple node IDs. A malformed replacement never displaces the last valid in-memory policy.

## Broker configuration

Every broker uses its own key store and the same trust store and identity policy. This is the node 1 shape I use:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9092 --advertised-host cascade-1.example.com --advertised-port 9092 --node-id 1 --data-dir data-1 --cluster-nodes 1@cascade-1.example.com:9092,2@cascade-2.example.com:9092,3@cascade-3.example.com:9092 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2 --security-protocol SSL --ssl-keystore secrets/cascade-1.p12 --ssl-keystore-password-file secrets/keystore.password --ssl-truststore secrets/cluster-ca.p12 --ssl-truststore-password-file secrets/truststore.password --ssl-client-auth requested --peer-security-protocol SSL --peer-identity-file secrets/peer-identities.conf --peer-identity-reload-ms 1000 --audit-log logs/security-audit.jsonl --operations-port 9404"
```

`requested` lets ordinary TLS Kafka clients connect without a client certificate while internal requests still require one. I use `required` when every client also has a trusted certificate. Peer TLS cannot start unless the broker listener uses `SSL` or `SASL_SSL`, client-certificate verification is enabled, a trust store is configured, and the identity file is present.

## Rotation without an authorization gap

I rotate one broker at a time:

1. I issue the replacement certificate with the same advertised-host SAN and a new subject or key.
2. I add a second identity-policy line for that node and atomically replace the policy file.
3. I wait longer than `--peer-identity-reload-ms` and confirm readiness remains healthy.
4. I install the new key store and restart that broker while the other two maintain quorum.
5. I confirm replication catches up and peer TLS authentication counters advance.
6. I remove the old subject from the policy, atomically publish it, and repeat for the next node.

If the replacement policy is malformed, `peer_identity_policy` fails readiness and the last valid policy remains active. I repair the file instead of restarting the broker.

## What I monitor

I alert on increases in `cascade_peer_authentication_rejections_total`. I compare it with `cascade_peer_authentications_total` and `cascade_peer_tls_authentications_total`; in an SSL peer deployment the two accepted counters should advance together. `/v1/status` exposes the same totals without Prometheus.

The security audit contains `peer_authentication` events for allowed and denied internal requests. I investigate an unexpected `peer_identity_denied`, `peer_certificate_required`, or hostname/trust handshake failure as a certificate inventory, node configuration, or active impersonation problem.

## Qualification I require

My automated tests use three distinct CA-signed broker certificates. They verify hostname-checked mutual handshakes, rejection of untrusted and hostname-mismatched certificates, rejection when one broker certificate claims another node ID, live identity-policy rotation, encrypted RF=3 replication, and continued `acks=all` production after the original broker/controller stops.

This closes the broker-to-broker encryption and authentication gate. It does not provide TLS key-store hot reload, SCRAM/OAuth, Kafka ACL Admin APIs, or a rolling-version protocol negotiation layer; I still track those separately in my [production-readiness checklist](production-readiness.md).
