[CmdletBinding()]
param(
    [string]$Image = 'eclipse-temurin:21-jdk-jammy@sha256:ce5767b7222312d42395f5bab033cd91f09e44032a2f21bdfd7b5b912dbe1e77'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$classpathFile = Join-Path $repository 'target/streams/test/fullClasspath/_global/streams/export'
if (-not (Test-Path -LiteralPath $classpathFile)) { throw 'Run sbt Test/compile first to export the test classpath.' }
$classpath = (Get-Content -LiteralPath $classpathFile -Raw).Trim().Split([IO.Path]::PathSeparator)
$mountArguments = @('--mount', "type=bind,source=$repository,target=/repo,readonly")
$mapped = [Collections.Generic.List[string]]::new()
$dependencyIndex = 0
foreach ($entry in $classpath) {
    $resolved = [IO.Path]::GetFullPath($entry)
    if (-not (Test-Path -LiteralPath $resolved)) { throw "Missing classpath entry: $resolved" }
    if ($resolved.StartsWith($repository + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        $relative = [IO.Path]::GetRelativePath($repository, $resolved).Replace('\', '/')
        $mapped.Add("/repo/$relative")
    } else {
        if (-not $resolved.EndsWith('.jar', [StringComparison]::OrdinalIgnoreCase)) { throw "Expected a dependency jar: $resolved" }
        $target = "/dependencies/$dependencyIndex.jar"
        $mountArguments += @('--mount', "type=bind,source=$resolved,target=$target,readonly")
        $mapped.Add($target)
        $dependencyIndex++
    }
}
$linuxClasspath = $mapped -join ':'
$common = @('run', '--rm', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
    '--tmpfs', '/tmp:size=1g,mode=1777', '--workdir', '/repo') + $mountArguments
& docker @common $Image java -cp $linuxClasspath cascade.qualification.ShardDirectoryQualification
if ($LASTEXITCODE -ne 0) { throw 'Directory forcing/reclamation probe failed.' }
$suites = @('cascade.cluster.ShardObjectRefSuite', 'cascade.cluster.ShardObjectStoreSuite',
    'cascade.cluster.ShardMetadataRecordSuite', 'cascade.cluster.ShardMetadataStoreSuite',
    'cascade.cluster.ShardStorageFeatureSuite', 'cascade.cluster.MetadataPublicationFailureSuite',
    'cascade.cluster.CoordinatorPayloadCacheSuite', 'cascade.backup.ShardStorageBackupSuite',
    'cascade.cluster.MetadataHeartbeatPublicationSuite', 'cascade.cluster.MetadataFailureFencingSuite',
    'cascade.group.OffsetBatchConfigSuite', 'cascade.group.OffsetCommitBatcherSuite',
    'cascade.group.OffsetCommitIsolationSuite', 'cascade.group.OffsetBatchQuorumSuite',
    'cascade.group.GroupSnapshotCacheSuite', 'cascade.delivery.DeliverySnapshotCacheSuite',
    'cascade.coordinator.ShardEncodingCacheSuite', 'cascade.coordinator.CoordinatorSnapshotCacheSuite',
    'cascade.group.GroupImageInstallationSuite', 'cascade.group.OwnedSessionQuorumSuite',
    'cascade.group.OffsetViewCacheSuite', 'cascade.coordinator.CoordinatorSnapshotQualificationSuite',
    'cascade.operations.PrometheusMetricsSuite')
& docker @common $Image java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp $linuxClasspath org.junit.runner.JUnitCore @suites
if ($LASTEXITCODE -ne 0) { throw 'Linux shard-storage regression tests failed.' }
