# OAuth and OIDC authentication runbook

I use Kafka `OAUTHBEARER` with an OAuth 2.0 or OpenID Connect provider that issues signed JWT access tokens. Cascade validates the signature against the provider's JWKS, then checks the issuer, audience, expiry, optional `nbf`/`iat`, principal claim, and required scopes before it creates a connection identity.

Bearer tokens are credentials. I always use `SASL_SSL`; Cascade rejects an `OAUTHBEARER` listener configured without TLS. I never put an access token or client secret on the broker command line, in an ACL file, or in an audit event.

## Provider contract

I configure an explicit JWKS URI, issuer, and audience instead of trusting values discovered from an unverified token. The JWKS URI must use `https` or `file`; plain HTTP, redirects, URI user information, and fragments are rejected. HTTPS uses the JVM trust roots and hostname verification.

Cascade accepts `RS256`, `RS384`, `RS512`, `ES256`, `ES384`, `ES512`, and `EdDSA` JWT signatures. I keep the allowlist as small as the identity provider permits and use `RS256` by default. RSA keys must be 2,048-8,192 bits, EC keys must use P-256/P-384/P-521 with their matching ES algorithm, and OKP keys must use Ed25519 with `EdDSA`. Every token and usable key needs a unique `kid`, and a JWK `alg`, `use`, or `key_ops` restriction must agree with verification.

The default claim contract is:

| Value | Default or requirement |
| --- | --- |
| Principal | Non-empty `sub` string, at most 255 characters, without control characters or whitespace |
| Issuer | Exact match with `--oauth-issuer` |
| Audience | String or string array containing `--oauth-audience` |
| Expiry | Required integer `exp`; expired tokens are rejected outside the configured clock skew |
| Activation | Optional integer `nbf` and `iat` cannot be in the future outside the clock skew |
| Scope | `scope` string separated by spaces or a string array; every configured required scope must be present |

I can select another principal or scope claim with `--oauth-principal-claim` and `--oauth-scope-claim`. I map the resulting principal to the same Cascade ACL format used by PLAIN and SCRAM.

For shared authorization roles, I configure `--oauth-role-claim` together with an explicit `--oauth-role-map`. The claim can be a string or string array. Cascade ignores unapproved values, maps only exact allowlisted values, and exposes each mapped role to ACL evaluation as `Role:<local-role>`. For example, `--oauth-role-claim roles --oauth-role-map orders-writer=orders-write,orders-reader=orders-read` lets an ACL target `Role:orders-write` without trusting arbitrary token text as a local role.

## Broker configuration

This is the production shape I use with an HTTPS identity provider:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9093 --advertised-host broker.example.com --data-dir data --security-protocol SASL_SSL --ssl-keystore broker.p12 --ssl-keystore-password-file broker-store.password --sasl-mechanisms OAUTHBEARER --oauth-jwks-uri https://identity.example.com/oauth2/keys --oauth-issuer https://identity.example.com --oauth-audience cascade --oauth-required-scopes cascade.read,cascade.write --oauth-allowed-algorithms RS256,ES256,EdDSA --oauth-role-claim roles --oauth-role-map orders-writer=orders-write,orders-reader=orders-read --oauth-jwks-refresh-ms 300000 --oauth-http-timeout-ms 5000 --acl-file acls.conf --audit-log security-audit.jsonl"
```

I use a `file:` JWKS URI only when my secret/configuration agent publishes a complete local JWKS atomically. The file still contains public keys, not client secrets or access tokens.

## Kafka client configuration

Kafka 4.3.1 can obtain a token with the OAuth `client_credentials` grant. I keep the client secret in my deployment secret store and inject it into the client configuration rather than committing it:

```properties
bootstrap.servers=broker.example.com:9093
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;
sasl.oauthbearer.client.credentials.client.id=orders-service
sasl.oauthbearer.client.credentials.client.secret=${OAUTH_CLIENT_SECRET}
sasl.oauthbearer.scope=cascade.read cascade.write
sasl.oauthbearer.token.endpoint.url=https://identity.example.com/oauth2/token
ssl.truststore.location=cluster-ca.p12
ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD}
ssl.truststore.type=PKCS12
group.protocol=classic
```

Kafka also supports signed client assertions and custom `AuthenticateCallbackHandler` implementations. Cascade only sees the resulting RFC 7628 bearer exchange, so the client-side acquisition method can follow the identity provider's supported production flow.

## Rotate signing keys safely

I rotate signing keys with an overlap:

1. I publish the new public key alongside the old key in the provider JWKS.
2. I wait for every broker to complete at least one `--oauth-jwks-refresh-ms` interval and keep `credential_policy` healthy.
3. I start issuing tokens whose header selects the new `kid` and verify a new Kafka connection.
4. I wait for the maximum old-token lifetime plus clock skew.
5. I remove the old JWK and confirm another successful refresh on every broker.

Cascade uses HTTPS ETags when the provider supplies them. A malformed, empty, oversized, unreachable, redirected, or non-successful replacement does not displace the last valid key set. The broker remains live, `credential_policy` fails readiness, and already loaded valid keys continue to verify tokens. I repair the endpoint or file and require readiness plus a fresh authentication to recover before completing rotation.

## Monitor and respond

Prometheus exposes bounded `cascade_sasl_authentication_successes_total` and `cascade_sasl_authentication_failures_total` counters with `mechanism="OAUTHBEARER"`. `/v1/status` includes aggregate SASL totals, and the JSONL security audit records allowed or denied authentication with the mechanism and resulting principal. Cascade never logs the token, signature, claims document, JWK modulus, or client secret.

I alert on `credential_policy` readiness failure, a sustained OAuth failure-rate increase, and an unexpected fall to another enabled SASL mechanism. For an emergency signing-key compromise, I remove the key from JWKS and shorten the refresh interval during the response, knowing that stateless JWTs already accepted on a connection remain valid until their `exp` plus clock skew.

## Bounds and limitations

The default token limit is 16 KiB and can be configured from 1 KiB to 1 MiB. Cascade bounds JSON depth, members, strings, numbers, JWKS documents to 1 MiB, JWKS key count to 128, RFC 7628 envelope attributes to 16, RSA modulus size to 2,048-8,192 bits, EC coordinates to the declared curve, Ed25519 keys to 32 bytes, role claim/mapping sizes, HTTP timeouts to 60 seconds, and clock skew to five minutes. It rejects duplicate JSON members, malformed UTF-8/Base64URL, non-canonical JOSE ECDSA signatures, `alg=none`, algorithm/key confusion, mismatched GS2 authorization identities, and post-expiry connection identity.

I currently validate signed JWT access tokens only. I do not yet implement opaque-token introspection, automatic OIDC discovery, dynamic claim-to-group expansion beyond the explicit role allowlist, token revocation callbacks, or OAuth for broker-to-broker identity. I keep broker mTLS for internal traffic and track the remaining release gates in my [production-readiness checklist](production-readiness.md).
