[CmdletBinding()]
param(
    [string]$Java,
    [string]$JdkImage = 'eclipse-temurin:21-jdk-jammy@sha256:ce5767b7222312d42395f5bab033cd91f09e44032a2f21bdfd7b5b912dbe1e77',
    [string]$GoProxy = 'https://proxy.golang.org,direct'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ([string]::IsNullOrWhiteSpace($Java)) { $Java = (Get-Command java -ErrorAction Stop).Source }
$classpathFile = Join-Path $repository 'target/streams/test/fullClasspath/_global/streams/export'
$libraries = Join-Path $repository 'target/docker-stage/lib'
if (-not (Test-Path -LiteralPath $classpathFile) -or -not (Test-Path -LiteralPath $libraries)) {
    throw 'Run sbt Test/compile stage before qualifying staged clients.'
}
if (-not (Test-Path -LiteralPath (Join-Path $repository 'compatibility/node/node_modules/kafkajs'))) {
    throw 'Run npm ci in compatibility/node before this qualification.'
}
$classpath = (Get-Content -LiteralPath $classpathFile -Raw).Trim()
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
        & docker exec $containerName java -cp '/cascade/lib/*' cascade.operations.ContainerHealthCheck 2>$null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Milliseconds 500
    }
    throw 'Staged broker did not become ready.'
}
try {
    & docker run --detach --name $containerName --read-only --user '65532:65532' --cap-drop ALL --security-opt no-new-privileges:true --memory 2g --tmpfs /tmp --publish '127.0.0.1:19092:19092' --mount "type=bind,source=$libraries,target=/cascade/lib,readonly" --mount "type=bind,source=$($data.FullName),target=/data" $JdkImage java -Xmx1g -cp '/cascade/lib/*' cascade.Main --host 0.0.0.0 --port 19092 --advertised-host 127.0.0.1 --advertised-port 19092 --data-dir /data --flush-policy sync --operations-port 9404
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
    & docker restart --time 10 $containerName
    Assert-ClientExit 'broker restart'
    Wait-StagedBroker
    & $Java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp $classpath cascade.e2e.ExternalBrokerSmokeTest '127.0.0.1:19092' 'staged-java' --verify-only
    Assert-ClientExit 'Java recovery'
    $brokerLog = & docker logs $containerName 2>&1
    if ($brokerLog -match '"event":"protocol_error"') { throw 'Broker reported a protocol error.' }
    Write-Output "STAGED_CLIENT_MATRIX_RESULT passed languages=5 records_each=25 restart_records=25 data=$($data.FullName)"
}
finally {
    $env:CASCADE_BOOTSTRAP_SERVERS = $oldBootstrap
    # docker run can create a container and then fail to start it (for example, a port collision).
    $ownedContainer = & docker container ls --all --quiet --filter "name=^/$containerName`$"
    if (-not [string]::IsNullOrWhiteSpace($ownedContainer)) { & docker rm --force $containerName }
}
