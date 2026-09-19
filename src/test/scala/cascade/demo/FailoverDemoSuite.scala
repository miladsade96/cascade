package cascade.demo

import java.nio.file.Path
import munit.FunSuite

final class FailoverDemoSuite extends FunSuite:
  test("configuration exposes every reproducibility input") {
    val config = FailoverDemoConfig.parse(Array(
      "--bootstrap", "one:1,two:2,three:3",
      "--compose-project", "portfolio",
      "--topic", "orders",
      "--records", "2500",
      "--output", "artifacts/result.json"
    ))
    assertEquals(config.bootstrapServers, "one:1,two:2,three:3")
    assertEquals(config.composeProject, "portfolio")
    assertEquals(config.topic, "orders")
    assertEquals(config.records, 2500)
    assertEquals(config.output, Some(Path.of("artifacts/result.json")))
  }

  test("result JSON reports only measured failover fields") {
    val result = FailoverDemoResult(1, 2, 1000, 1000, 0, 0, 814, 4012)
    assertEquals(
      result.json,
      """{"old_leader":1,"new_leader":2,"produced":1000,"consumed":1000,"lost":0,"unexpected_duplicates":0,"failover_ms":814,"elapsed_ms":4012}"""
    )
  }

  test("demo refuses a workload that cannot cross the failure boundary") {
    intercept[IllegalArgumentException](FailoverDemoConfig(records = 1))
  }

