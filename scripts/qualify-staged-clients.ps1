[CmdletBinding()]
param(
    [string]$Java,
    [string]$JdkImage = 'eclipse-temurin:21-jdk-jammy@sha256:ce5767b7222312d42395f5bab033cd91f09e44032a2f21bdfd7b5b912dbe1e77',
    [string]$GoProxy = 'https://proxy.golang.org,direct',
    [string]$BrokerImage,
    [string]$ExpectedVersion
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ([string]::IsNullOrWhiteSpace($Java)) { $Java = (Get-Command java -ErrorAction Stop).Source }
$classpathFile = Join-Path $repository 'target/streams/test/fullClasspath/_global/streams/export'
$libraries = Join-Path $repository 'target/docker-stage/lib'
if (-not (Test-Path -LiteralPath $classpathFile) -or
    ([string]::IsNullOrWhiteSpace($BrokerImage) -and -not (Test-Path -LiteralPath $libraries))) {
    throw 'Run sbt Test/compile (and stage for staged-runtime mode) before qualifying clients.'
}
if (-not (Test-Path -LiteralPath (Join-Path $repository 'compatibility/node/node_modules/kafkajs'))) {
    throw 'Run npm ci in compatibility/node before this qualification.'
}
$classpath = (Get-Content -LiteralPath $classpathFile -Raw).Trim()
$brokerClasspath = '/cascade/lib/*'
$qualifiedImageId = $null
if (-not [string]::IsNullOrWhiteSpace($BrokerImage)) {
    if ([string]::IsNullOrWhiteSpace($ExpectedVersion)) { $ExpectedVersion = (Get-Content -LiteralPath (Join-Path $repository 'VERSION') -Raw).Trim() }
    $imageJson = & docker image inspect $BrokerImage
    if ($LASTEXITCODE -ne 0) { throw 'The image must be built or pulled before qualification.' }
    $imageMetadata = @($imageJson | ConvertFrom-Json)[0]
    if ($imageMetadata.Config.User -ne '65532:65532') { throw 'Release image must declare UID/GID 65532.' }
    if ($imageMetadata.Config.Labels.'org.opencontainers.image.version' -ne $ExpectedVersion) { throw 'Image release label does not match the expected version.' }
    if ($null -eq $imageMetadata.Config.Healthcheck) { throw 'Release image must include a health check.' }
    $qualifiedImageId = $imageMetadata.Id
    $brokerClasspath = '/opt/cascade/lib/*'
}
$runId = [Guid]::NewGuid().ToString('N')
$containerName = "cascade-client-$runId"
$data = New-Item -ItemType Directory -Path (Join-Path $repository "artifacts/client-data-$runId")
$oldBootstrap = $env:CASCADE_BOOTSTRAP_SERVERS
$env:CASCADE_BOOTSTRAP_SERVERS = '127.0.0.1:19092'
function Assert-ClientExit([string]$Phase) {
    if ($LASTEXITCODE -ne 0) { throw "$Phase failed with exit code $LASTEXITCODE" }
}
function Wait-StagedBroker {
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        & docker exec $containerName java -cp $brokerClasspath cascade.operations.ContainerHealthCheck 2>$null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Milliseconds 500
    }
    throw 'Staged broker did not become ready.'
}
try {
    if ([string]::IsNullOrWhiteSpace($BrokerImage)) {
        & docker run --detach --name $containerName --read-only --user '65532:65532' --cap-drop ALL --security-opt no-new-privileges:true --memory 2g --tmpfs /tmp --publish '127.0.0.1:19092:19092' --mount "type=bind,source=$libraries,target=/cascade/lib,readonly" --mount "type=bind,source=$($data.FullName),target=/data" $JdkImage java -Xmx1g -cp '/cascade/lib/*' cascade.Main --host 0.0.0.0 --port 19092 --advertised-host 127.0.0.1 --advertised-port 19092 --data-dir /data --flush-policy sync --operations-port 9404
    } else {
        # Run the inspected immutable image ID, with its declared user, entry point and JVM settings.
        & docker run --detach --name $containerName --read-only --cap-drop ALL --security-opt no-new-privileges:true --memory 2g --tmpfs '/tmp:size=64m,mode=1777,nosuid,nodev,noexec' --publish '127.0.0.1:19092:19092' --mount "type=bind,source=$($data.FullName),target=/var/lib/cascade" $qualifiedImageId --host 0.0.0.0 --port 19092 --advertised-host 127.0.0.1 --advertised-port 19092 --data-dir /var/lib/cascade --flush-policy sync --operations-port 9404
    }
    Assert-ClientExit 'broker start'
    Wait-StagedBroker
    & $Java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp $classpath cascade.e2e.ExternalBrokerSmokeTest '127.0.0.1:19092' 'staged-java'
    Assert-ClientExit 'Java smoke'
    & node (Join-Path $repository 'compatibility/node/smoke.js') 'staged-node'
    Assert-ClientExit 'KafkaJS smoke'
    & docker run --rm --network "container:$containerName" --mount "type=bind,source=$repository/compatibility/python,target=/work,readonly" python:3.13-slim sh -c 'pip install --disable-pip-version-check --target /tmp/client -r /work/requirements.txt && PYTHONPATH=/tmp/client python /work/smoke.py staged-python'
    Assert-ClientExit 'Python smoke'
    & docker run --rm --network "container:$containerName" --env "GOPROXY=$GoProxy" --mount "type=bind,source=$repository/compatibility/go,target=/work,readonly" --workdir /work golang:1.25 go run .
    Assert-ClientExit 'Go smoke'
    & docker run --rm --network "container:$containerName" --tmpfs /work:rw,exec,nosuid,nodev --mount "type=bind,source=$repository/compatibility/dotnet/CascadeCompatibility.csproj,target=/work/CascadeCompatibility.csproj,readonly" --mount "type=bind,source=$repository/compatibility/dotnet/Program.cs,target=/work/Program.cs,readonly" --workdir /work mcr.microsoft.com/dotnet/sdk:8.0 dotnet run --configuration Release
    Assert-ClientExit '.NET smoke'
    & docker restart --timeout 120 $containerName
    Assert-ClientExit 'broker restart'
    Wait-StagedBroker
    & $Java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp $classpath cascade.e2e.ExternalBrokerSmokeTest '127.0.0.1:19092' 'staged-java' --verify-only
    Assert-ClientExit 'Java recovery'
    $brokerLog = & docker logs $containerName 2>&1
    if ($brokerLog -match '"event":"protocol_error"') { throw 'Broker reported a protocol error.' }
    if (-not [string]::IsNullOrWhiteSpace($BrokerImage)) {
        $healthy = $false
        for ($attempt = 0; $attempt -lt 60; $attempt++) {
            if ((& docker inspect --format '{{.State.Health.Status}}' $containerName) -eq 'healthy') { $healthy = $true; break }
            Start-Sleep -Milliseconds 500
        }
        if (-not $healthy) { throw 'The image health check did not become healthy after restart.' }
        Write-Output "IMAGE_CLIENT_MATRIX_RESULT passed image=$BrokerImage image_id=$qualifiedImageId version=$ExpectedVersion languages=5 records_each=25 restart_records=25"
    }
    Write-Output "STAGED_CLIENT_MATRIX_RESULT passed languages=5 records_each=25 restart_records=25 data=$($data.FullName)"
}
finally {
    $env:CASCADE_BOOTSTRAP_SERVERS = $oldBootstrap
    # docker run can create a container and then fail to start it (for example, a port collision).
    $ownedContainer = & docker container ls --all --quiet --filter "name=^/$containerName`$"
    if (-not [string]::IsNullOrWhiteSpace($ownedContainer)) { & docker rm --force $containerName }
}
