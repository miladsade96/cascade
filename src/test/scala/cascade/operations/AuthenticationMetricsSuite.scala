package cascade.operations

final class AuthenticationMetricsSuite extends munit.FunSuite:
  test("counts authentication outcomes in a bounded mechanism set") {
    val metrics = AuthenticationMetrics()
    metrics.recordSuccess(Some("PLAIN"))
    metrics.recordSuccess(Some("SCRAM-SHA-256"))
    metrics.recordFailure(Some("SCRAM-SHA-256"))
    metrics.recordFailure(Some("unbounded-client-value"))
    metrics.recordFailure(None)

    val snapshot = metrics.snapshot
    assertEquals(snapshot.successes, 2L)
    assertEquals(snapshot.failures, 3L)
    assertEquals(snapshot.mechanisms.find(_.mechanism == "SCRAM-SHA-256").map(_.failures), Some(1L))
    assertEquals(snapshot.mechanisms.find(_.mechanism == "UNKNOWN").map(_.failures), Some(2L))
    assertEquals(snapshot.mechanisms.size, 4)
  }
