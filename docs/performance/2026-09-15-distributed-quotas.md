# Distributed quota reclamation qualification

I qualified Cascade 1.7.0 on 2026-09-15 after replacing permanently divided broker shares with one controller-fenced cluster ledger per principal and quota class.

## Safety properties I tested

- One busy broker can reclaim capacity that idle brokers do not consume.
- Concurrent reservations from different brokers cannot spend more than the configured cluster rate and burst.
- Request, response, Produce, and Fetch capacity remains isolated per principal and class.
- A new controller term starts without the predecessor's burst, and the old term is fenced.
- A broker with inconsistent quota settings fails closed.
- Level 2 activates only after every voter commits support; mixed-version clusters retain conservative level-1 shares.
- An ambiguous peer result is not retried, authenticated peer traffic cannot recurse into client quotas, and acknowledged egress waits its complete calculated delay.

The focused gates passed 5/5 ledger tests, 3/3 wire-codec tests, 6/6 local quota tests, and 3/3 real three-broker fault tests. The fault tests exercise cross-broker reclamation, aggregate bounding, controller failover, and configuration mismatch over actual peer connections. The monitoring and rolling-activation suites also passed 13/13 tests.

## Complete regression result

I ran the complete Scala and Kafka Java client suite with Eclipse Adoptium JDK 21.0.11:

```text
Passed: Total 577, Failed 0, Errors 0, Passed 577
Total time: 156 s (0:02:36.0)
```

An earlier run passed 576/577 and exposed one stale assertion for the frozen internal peer-API boundary after adding quota reservation key `-130`. I updated that compatibility assertion and reran both the focused suite and the complete suite. I did not hide or reclassify the failure.

## Boundary of this result

This proves deterministic correctness in the repository's unit, integration, end-to-end, and real-process fault harnesses. It is not a production capacity number. Exact reservation adds one controller RPC to quota-limited client admission, so controller CPU and peer latency must be measured using the intended batch size, authentication, broker count, and hardware.

I still require the 72-hour authenticated multi-tenant soak, dedicated-host RF=3 capacity campaign, arbitrary packet impairment, and physical power/device-loss qualification before calling Cascade production ready.
