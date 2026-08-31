# TLS key and trust rotation

I rotate Cascade listener and broker-to-broker TLS material without restarting a broker. Cascade fingerprints the configured key store and optional trust store every `--ssl-reload-ms`, parses both files into a new `SSLContext`, and publishes that context as one generation only after the whole replacement validates.

Existing TLS sessions continue on the context that authenticated them. Every new Kafka client handshake uses the current generation. Persistent peer channels compare their connection generation before each RPC and reconnect after a successful rotation, so controller, metadata, replication, and recovery traffic moves to the replacement certificate without waiting for a random network failure.

## Configuration

I enable polling with a positive interval; `0` disables live reload:

```powershell
.\sbt.bat "run --security-protocol SSL --ssl-keystore secrets/broker.p12 --ssl-keystore-password-file secrets/keystore.password --ssl-truststore secrets/cluster-trust.p12 --ssl-truststore-password-file secrets/truststore.password --ssl-client-auth requested --ssl-reload-ms 1000"
```

Cascade accepts PKCS12 and JKS files up to 64 MiB. I keep the store passwords and optional private-key password stable during live rotation because password files are read at process startup. If I must change a password, I first migrate the store under the current password or use a controlled rolling restart.

## Atomic publication

I never edit an active store in place. I write and force the complete replacement beside the active file, verify it with `keytool`, preserve restrictive ownership and permissions, then atomically rename it over the configured path. A Kubernetes projected Secret symlink swap has the same property. On Windows I allow a short bounded retry because an antivirus scanner or Cascade's fingerprint read can briefly hold the old path open.

The reload unit contains both the key store and trust store. If either file is missing, empty, too large, unreadable, has the wrong password, or contains invalid key/trust material, Cascade rejects the entire candidate. It keeps the previous generation serving traffic, increments `cascade_tls_material_reload_failures_total`, emits `tls_material_reload_failed`, and fails the `tls_material` readiness check. Repairing or rolling back the files clears readiness without a restart.

## Leaf certificate rotation under one CA

I use this sequence when the issuing CA is unchanged:

1. I issue a replacement certificate with the broker's advertised DNS/IP SANs and the required `serverAuth` and `clientAuth` usages.
2. If its subject changes, I add the new subject to `peer-identities.conf`, publish that policy atomically, and wait for `peer_identity_policy` readiness.
3. I atomically replace one broker's key store.
4. I wait for `cascade_tls_material_generation` to advance by one, require `tls_material` readiness, open a new Kafka TLS connection, and confirm peer authentication counters advance.
5. I repeat one broker at a time, then remove obsolete identity-policy subjects after no old certificate remains.

I do not restart the broker and I do not deliberately close established client sessions. New sessions prove the new leaf is active; old sessions are allowed to drain naturally.

## CA rotation with overlapping trust

I avoid a trust gap by rotating in three phases:

1. I publish a bridge trust store containing both the old and new CA chains to every broker and wait for every generation to advance.
2. I rotate each broker key store to a leaf issued by the new CA. After each broker advances, I require an authenticated peer RPC and an `acks=all` replicated produce.
3. After every broker and client uses the new chain, I publish a trust store containing only the new CA and wait for one final generation change on every broker.

I keep Kafka client trust stores overlapping for the entire migration. When mutual TLS clients also change issuing CA, I publish the bridge store before any new client certificate and remove the old CA only after new connections from every client population pass.

## Monitoring and rollback

I alert on `cascade_tls_material_reload_failures_total` and on a non-healthy `tls_material` check. I track `cascade_tls_enabled`, `cascade_tls_material_generation`, and `cascade_tls_material_reloads_total` per node and correlate them with `tls_material_reloaded` structured events. I also watch peer TLS authentication/rejection counters during cluster PKI changes.

If verification fails, I atomically restore the last known-good files. When their fingerprint matches the active in-memory generation, Cascade clears the reload error without manufacturing a new generation. I finish a rotation only after a fresh Kafka client connection, replicated write/read, readiness check, and peer-authentication check pass on every broker.

## Qualification

My automated tests cover changed and unchanged fingerprints, store size/password failures, invalid replacements, last-known-good retention, readiness recovery, listener certificate replacement with established and fresh Kafka clients, mutual-TLS trust replacement, peer-channel certificate reconnection, and a three-broker old-CA → bridge → new-leaf → new-CA sequence. The cluster test keeps RF=3/minimum ISR=2 traffic moving through every phase, removes the original controller afterward, and consumes the exact committed sequence from the surviving majority.
