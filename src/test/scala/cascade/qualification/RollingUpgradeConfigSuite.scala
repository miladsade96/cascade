package cascade.qualification

import munit.FunSuite

final class RollingUpgradeConfigSuite extends FunSuite:
  test("historical feature maps preserve levels and reject ambiguous baselines") {
    assertEquals(RollingUpgradeConfig.parseFeatures(""), Map.empty[String, Short])
    assertEquals(RollingUpgradeConfig.parseFeatures("coordinator-deltas:1,consumer-protocol:2"),
      Map("coordinator-deltas" -> 1.toShort, "consumer-protocol" -> 2.toShort))
    Vector("a:0", "a:-1", "a:1,a:2", "a", "a:1,", "a:32768", "a:b").foreach { value =>
      intercept[IllegalArgumentException](RollingUpgradeConfig.parseFeatures(value))
    }
  }
