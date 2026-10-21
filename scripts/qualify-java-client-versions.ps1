[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$BrokerImage,
    [string]$ExpectedVersion,
    [string]$VersionsFile,
    [string]$Report,
    [string]$MavenImage = 'maven@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ([string]::IsNullOrWhiteSpace($ExpectedVersion)) {
    $ExpectedVersion = (Get-Content -LiteralPath (Join-Path $repository 'VERSION') -Raw).Trim()
}
if ([string]::IsNullOrWhiteSpace($VersionsFile)) {
    $VersionsFile = Join-Path $repository 'compatibility/java/versions.txt'
}
$VersionsFile = [IO.Path]::GetFullPath($VersionsFile)
if ([string]::IsNullOrWhiteSpace($Report)) {
    $Report = Join-Path $repository "artifacts/java-client-versions-$ExpectedVersion.json"
}
$Report = [IO.Path]::GetFullPath($Report)
if (Test-Path -LiteralPath $Report) { throw "Java client evidence path already exists: $Report" }

$versions = @(Get-Content -LiteralPath $VersionsFile | ForEach-Object { $_.Trim() } | Where-Object { $_ })
if ($versions.Count -lt 2 -or ($versions | Select-Object -Unique).Count -ne $versions.Count) {
    throw 'Java client matrix must contain at least two unique versions.'
}
foreach ($version in $versions) {
    if ($version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+$') { throw "Invalid Kafka Java client version: $version" }
}

$metadata = @(& docker image inspect $BrokerImage | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0) { throw "Broker image is unavailable: $BrokerImage" }
if ($metadata.Config.User -ne '65532:65532') { throw 'Broker image must declare UID/GID 65532.' }
if ($metadata.Config.Labels.'org.opencontainers.image.version' -ne $ExpectedVersion) { throw 'Broker image version label is incorrect.' }
if ($metadata.Id -notmatch '^sha256:[0-9a-f]{64}$') { throw 'Broker image ID is not immutable.' }
$imageId = $metadata.Id
$revision = $metadata.Config.Labels.'org.opencontainers.image.revision'

$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
$listener.Start()
$hostPort = ([Net.IPEndPoint]$listener.LocalEndpoint).Port
$listener.Stop()
$runId = [Guid]::NewGuid().ToString('N')
$containerName = "cascade-java-matrix-$runId"
$volumeName = "cascade-java-matrix-data-$runId"
$javaProject = [IO.Path]::GetFullPath((Join-Path $repository 'compatibility/java'))
$bootstrap = "host.docker.internal:$hostPort"
$startedAt = [DateTimeOffset]::UtcNow

try {
    & docker volume create --label 'cascade.qualification=java-client-matrix' $volumeName | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create the Java client matrix volume.' }
    & docker run --detach --init --name $containerName --read-only --cap-drop ALL `
        --security-opt no-new-privileges:true --memory 2g `
        --tmpfs '/tmp:size=64m,mode=1777,nosuid,nodev,noexec' `
        --publish "127.0.0.1:$hostPort`:9092" `
        --mount "type=volume,source=$volumeName,target=/var/lib/cascade" `
        $imageId --host 0.0.0.0 --port 9092 --advertised-host host.docker.internal `
        --advertised-port $hostPort --data-dir /var/lib/cascade --flush-policy sync --operations-port 9404 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not start the Java client matrix broker.' }

    $healthy = $false
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $status = (& docker inspect --format '{{.State.Health.Status}}' $containerName).Trim()
        if ($status -eq 'healthy') { $healthy = $true; break }
        if ($status -eq 'unhealthy') { break }
        Start-Sleep -Milliseconds 500
    }
    if (-not $healthy) { throw "Java client matrix broker did not become healthy: $(docker logs $containerName 2>&1)" }

    foreach ($version in $versions) {
        $topic = "java-client-$($version.Replace('.', '-'))"
        & docker run --rm --add-host 'host.docker.internal:host-gateway' `
            --mount "type=bind,source=$javaProject,target=/work" --workdir /work `
            $MavenImage mvn -q "-Dkafka.version=$version" `
            "-Dexec.args=$bootstrap $topic $version" compile exec:java
        if ($LASTEXITCODE -ne 0) { throw "Kafka Java client $version failed." }
    }

    $brokerLog = & docker logs $containerName 2>&1
    if ($brokerLog -match '"event":"protocol_error"') { throw 'Broker reported a protocol error during the Java version matrix.' }
    $evidence = [ordered]@{
        status = 'passed'
        started_at = $startedAt.ToString('O')
        image = $BrokerImage
        image_id = $imageId
        release = $ExpectedVersion
        revision = $revision
        client_versions = $versions
        records_per_version = 25
    }
    $directory = Split-Path -Parent $Report
    if (-not (Test-Path -LiteralPath $directory)) { New-Item -ItemType Directory -Path $directory -Force | Out-Null }
    $temporary = "$Report.tmp"
    [IO.File]::WriteAllText($temporary, (($evidence | ConvertTo-Json -Depth 4) + [Environment]::NewLine), [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $Report
    Write-Output "JAVA_CLIENT_VERSION_MATRIX_RESULT passed image_id=$imageId versions=$($versions -join ',') records_each=25 report=$Report"
}
finally {
    & docker rm --force $containerName 2>$null | Out-Null
    & docker volume rm --force $volumeName 2>$null | Out-Null
}
