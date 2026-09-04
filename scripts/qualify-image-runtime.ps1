[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$BrokerImage,
    [string]$ExpectedVersion,
    [string]$Java
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ([string]::IsNullOrWhiteSpace($ExpectedVersion)) { $ExpectedVersion = (Get-Content -LiteralPath (Join-Path $repository 'VERSION') -Raw).Trim() }
if ([string]::IsNullOrWhiteSpace($Java)) { $Java = (Get-Command java -ErrorAction Stop).Source }
$classpathFile = Join-Path $repository 'target/streams/test/fullClasspath/_global/streams/export'
if (-not (Test-Path -LiteralPath $classpathFile)) { throw 'Run sbt Test/compile first.' }
$imageJson = & docker image inspect $BrokerImage
if ($LASTEXITCODE -ne 0) { throw 'Release image must exist locally.' }
$metadata = @($imageJson | ConvertFrom-Json)[0]
if ($metadata.Config.User -ne '65532:65532' -or $metadata.Config.Labels.'org.opencontainers.image.version' -ne $ExpectedVersion) {
    throw 'Unexpected image user/version.'
}
$mountArguments = @('--mount', "type=bind,source=$repository,target=/repo,readonly")
$mapped = [Collections.Generic.List[string]]::new()
# Packaged broker and Scala jars take precedence; never substitute staged/main classes.
$mapped.Add('/opt/cascade/lib/*')
$dependencyIndex = 0
foreach ($entry in (Get-Content -LiteralPath $classpathFile -Raw).Trim().Split([IO.Path]::PathSeparator)) {
    $resolved = [IO.Path]::GetFullPath($entry)
    if (-not (Test-Path -LiteralPath $resolved)) { throw "Missing classpath entry: $resolved" }
    if ($resolved -match '[/\\]target[/\\]scala-[^/\\]+[/\\]classes$') { continue }
    if ($resolved.StartsWith($repository + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        $mapped.Add('/repo/' + [IO.Path]::GetRelativePath($repository, $resolved).Replace('\', '/'))
    } else {
        if (-not $resolved.EndsWith('.jar', [StringComparison]::OrdinalIgnoreCase)) { throw 'Expected a dependency jar.' }
        $target = "/dependencies/$dependencyIndex.jar"
        $mountArguments += @('--mount', "type=bind,source=$resolved,target=$target,readonly")
        $mapped.Add($target)
        $dependencyIndex++
    }
}
$common = @('run', '--rm', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
    '--memory', '3g', '--tmpfs', '/tmp:size=1g,mode=1777,nosuid,nodev', '--workdir', '/repo',
    '--entrypoint', '/opt/java/bin/java') + $mountArguments
$suites = @('cascade.security.TlsContextFactorySuite', 'cascade.security.ReloadableTlsContextSuite',
    'cascade.security.MutualTlsMaterialSuite', 'cascade.security.PeerTlsClientSuite',
    'cascade.broker.PeerSecurityIntegrationSuite')
& docker @common $metadata.Id '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp ($mapped -join ':') org.junit.runner.JUnitCore @suites
if ($LASTEXITCODE -ne 0) { throw 'Packaged-image TLS/SASL/security regression failed.' }
& $Java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp (Get-Content -LiteralPath $classpathFile -Raw).Trim() cascade.e2e.ExternalSecureImageQualification $metadata.Id
if ($LASTEXITCODE -ne 0) { throw 'External secure Kafka clients failed against the actual image.' }
Write-Output "IMAGE_RUNTIME_SECURITY_RESULT passed image_id=$($metadata.Id) version=$ExpectedVersion suites=$($suites.Count) broker_classes=image"
