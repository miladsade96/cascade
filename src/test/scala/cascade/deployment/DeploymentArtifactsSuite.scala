package cascade.deployment

import java.nio.file.{Files, Paths}
import munit.FunSuite

final class DeploymentArtifactsSuite extends FunSuite:
  private val deployment = read("deploy/kubernetes/statefulsets.yaml")

  test("Kubernetes brokers use independent durable identities and secret material") {
    assertEquals(occurrences(deployment, "kind: StatefulSet"), 3)
    (1 to 3).foreach { nodeId =>
      assert(deployment.contains(s"name: cascade-$nodeId"))
      assert(deployment.contains(s"secretName: cascade-node-$nodeId-tls"))
      assert(deployment.contains(s"cascade-$nodeId-0.cascade-peer.cascade.svc.cluster.local"))
    }
    assertEquals(occurrences(deployment, "persistentVolumeClaimRetentionPolicy:"), 3)
    assertEquals(occurrences(deployment, "storage: 100Gi"), 3)
    assertEquals(occurrences(deployment, "type: OnDelete"), 3)
  }

  test("Kubernetes brokers retain the restricted container security contract") {
    assertEquals(occurrences(deployment, "runAsNonRoot: true"), 3)
    assertEquals(occurrences(deployment, "runAsUser: 65532"), 3)
    assertEquals(occurrences(deployment, "readOnlyRootFilesystem: true"), 3)
    assertEquals(occurrences(deployment, "allowPrivilegeEscalation: false"), 3)
    assertEquals(occurrences(deployment, "automountServiceAccountToken: false"), 3)
    assertEquals(occurrences(deployment, "seccompProfile:"), 3)
    assertEquals(occurrences(deployment, "drop: [ALL]"), 3)
  }

  test("Kubernetes brokers require encrypted authenticated traffic and bounded capacity") {
    assertEquals(occurrences(deployment, "- SASL_SSL"), 3)
    assertEquals(occurrences(deployment, "- SCRAM-SHA-512"), 3)
    assertEquals(occurrences(deployment, "- --peer-security-protocol"), 3)
    assertEquals(occurrences(deployment, "- --acl-file"), 3)
    assertEquals(occurrences(deployment, "- --no-auto-create"), 3)
    assertEquals(occurrences(deployment, "minimumFreeBytes"), 0)
    assertEquals(occurrences(deployment, "- --minimum-free-bytes"), 3)
    assertEquals(occurrences(deployment, "memory: 8Gi"), 3)
  }

  test("quorum disruption and network policies preserve the intended failure domains") {
    val disruption = read("deploy/kubernetes/disruption-budget.yaml")
    val network = read("deploy/kubernetes/network-policy.yaml")
    assert(disruption.contains("minAvailable: 2"))
    assert(network.contains("name: cascade-default-deny"))
    assert(network.contains("cascade.dev/client-access"))
    assert(network.contains("cascade.dev/monitoring-access"))
    assertEquals(occurrences(deployment, "requiredDuringSchedulingIgnoredDuringExecution:"), 3)
  }

  test("alerts and dashboard query metrics emitted by the broker") {
    val source = read("src/main/scala/cascade/operations/BrokerMetrics.scala")
    val emitted = "cascade_[a-z0-9_]+".r.findAllIn(source).toSet
    val rules = read("deploy/kubernetes/prometheus-rules.yaml")
    val dashboard = read("deploy/kubernetes/dashboards/cascade.json")
    val queried = "cascade_[a-z0-9_]+".r.findAllIn(rules + dashboard).toSet
    assert(queried.nonEmpty)
    assertEquals(queried.diff(emitted), Set.empty)
    assert(rules.contains("CascadeBrokerDown"))
    assert(rules.contains("CascadeStorageAdmissionRejected"))
    assert(dashboard.contains("Cascade production overview"))
  }

  private def read(path: String): String = Files.readString(Paths.get(path))

  private def occurrences(source: String, value: String): Int =
    source.sliding(value.length).count(_ == value)
