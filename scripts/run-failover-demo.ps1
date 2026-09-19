[CmdletBinding()]
param(
    [string]$Image = 'miladsade96/cascade:1.8.0',
    [ValidateRange(2, 10000000)][int]$Records = 100000,
    [string]$Java = 'java',
    [string]$Project = 'cascade-failover-demo',
    [string]$ClusterHost,
    [string]$Output
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$classpathFile = Join-Path $repository 'target/streams/test/fullClasspath/_global/streams/export'
if (-not (Test-Path -LiteralPath $classpathFile)) { throw 'Run sbt Test/compile before recording the demo.' }
if ([string]::IsNullOrWhiteSpace($Output)) {
    $stamp = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHHmmssZ')
    $Output = Join-Path $repository "artifacts/failover-demo-$stamp.json"
}
$Output = [IO.Path]::GetFullPath($Output)
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $repository 'artifacts'))
if (-not $Output.StartsWith($artifactRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Failover evidence must be written beneath the repository artifacts directory.'
}

& docker image inspect $Image *> $null
if ($LASTEXITCODE -ne 0) { throw "Build the requested image first: $Image" }
if ([string]::IsNullOrWhiteSpace($ClusterHost)) {
    $routeProbe = [Net.Sockets.UdpClient]::new()
    try {
        $routeProbe.Connect('8.8.8.8', 53)
        $ClusterHost = ([Net.IPEndPoint]$routeProbe.Client.LocalEndPoint).Address.ToString()
    } finally {
        $routeProbe.Dispose()
    }
}
$env:CASCADE_IMAGE = $Image
$env:CASCADE_CLUSTER_HOST = $ClusterHost
$compose = @('compose', '-p', $Project, '-f', (Join-Path $repository 'compose.cluster.yaml'))
try {
    & docker @compose up --detach --no-build
    if ($LASTEXITCODE -ne 0) { throw 'Could not start the three-broker demo cluster.' }
    $healthy = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $states = @(& docker @compose ps --format json | ForEach-Object { $_ | ConvertFrom-Json })
        if ($states.Count -eq 3 -and @($states | Where-Object { $_.Health -ne 'healthy' }).Count -eq 0) {
            $healthy = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $healthy) { throw 'The three-broker demo cluster did not become healthy.' }

    $classpath = (Get-Content -LiteralPath $classpathFile -Raw).Trim()
    & $Java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp $classpath cascade.demo.FailoverDemo `
        --compose-project $Project --records $Records --output $Output
    if ($LASTEXITCODE -ne 0) { throw 'The failover verifier failed.' }
    Write-Output "FAILOVER_DEMO_EVIDENCE $Output"
}
finally {
    & docker @compose down --volumes --remove-orphans
    Remove-Item Env:CASCADE_IMAGE -ErrorAction SilentlyContinue
    Remove-Item Env:CASCADE_CLUSTER_HOST -ErrorAction SilentlyContinue
}
