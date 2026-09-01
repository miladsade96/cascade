[CmdletBinding()]
param(
    [string]$Report,
    [switch]$KeepData
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ([string]::IsNullOrWhiteSpace($Report)) {
    $Report = Join-Path $repository 'artifacts/rolling-upgrade.json'
}
$Report = [IO.Path]::GetFullPath($Report)

$baselinePath = Join-Path $repository 'compatibility/releases/1.0.0.properties'
$baseline = Get-Content -LiteralPath $baselinePath -Raw | ConvertFrom-StringData
$currentVersion = (Get-Content -LiteralPath (Join-Path $repository 'VERSION') -Raw).Trim()
$currentRevision = (& git -C $repository rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Could not resolve the current Git revision.' }
if ((& git -C $repository status --porcelain).Count -gt 0) {
    $currentRevision = "$currentRevision+working-tree"
}

$sbtVersion = ((Get-Content -LiteralPath (Join-Path $repository 'project/build.properties') -Raw).Trim() -split '=', 2)[1]
$launcherDirectory = Join-Path $repository '.tools'
$launcher = Join-Path $launcherDirectory "sbt-launch-$sbtVersion.jar"
if (-not (Test-Path -LiteralPath $launcher)) {
    New-Item -ItemType Directory -Path $launcherDirectory -Force | Out-Null
    $launcherUri = "https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/$sbtVersion/sbt-launch-$sbtVersion.jar"
    Invoke-WebRequest -Uri $launcherUri -OutFile $launcher
}

$javaExecutable = $null
if (-not [string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
    $candidate = Join-Path $env:JAVA_HOME $(if ($IsWindows) { 'bin/java.exe' } else { 'bin/java' })
    if (Test-Path -LiteralPath $candidate) { $javaExecutable = $candidate }
}
if ($null -eq $javaExecutable) {
    $javaExecutable = (Get-Command java -ErrorAction Stop).Source
}
$javaVersion = (& $javaExecutable -version 2>&1 | Select-Object -First 1).ToString()
if ($javaVersion -notmatch 'version "(2[1-9]|[3-9][0-9])') {
    throw "Java 21 or newer is required; found $javaVersion"
}

function Invoke-CascadeSbt {
    param([string]$Directory, [string[]]$Commands)
    Push-Location $Directory
    try {
        & $javaExecutable -Xms256m -Xmx2g '-Dsbt.supershell=false' '-Dsbt.log.noformat=true' '-Dsbt.server.autostart=false' -jar $launcher @Commands
        if ($LASTEXITCODE -ne 0) { throw "sbt failed in $Directory with exit code $LASTEXITCODE" }
    }
    finally {
        Pop-Location
    }
}

$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) "cascade-rolling-$([Guid]::NewGuid().ToString('N'))"
$oldCheckout = Join-Path $temporaryRoot 'cascade-1.0.0'
$worktreeAdded = $false
New-Item -ItemType Directory -Path $temporaryRoot | Out-Null

try {
    & git -C $repository worktree add --detach $oldCheckout $baseline.revision
    if ($LASTEXITCODE -ne 0) { throw "Could not create the $($baseline.version) worktree." }
    $worktreeAdded = $true

    Invoke-CascadeSbt -Directory $oldCheckout -Commands @('stage')
    Invoke-CascadeSbt -Directory $repository -Commands @('stage')

    $oldRuntime = Join-Path $oldCheckout 'target/docker-stage'
    $currentRuntime = Join-Path $repository 'target/docker-stage'
    $oldRuntimeArgument = $oldRuntime.Replace('\', '/')
    $currentRuntimeArgument = $currentRuntime.Replace('\', '/')
    $reportArgument = $Report.Replace('\', '/')
    $runner = @(
        'Test / runMain cascade.qualification.RollingUpgradeQualification',
        '--old-runtime', ('"' + $oldRuntimeArgument + '"'),
        '--current-runtime', ('"' + $currentRuntimeArgument + '"'),
        '--old-version', $baseline.version,
        '--current-version', $currentVersion,
        '--old-revision', $baseline.revision,
        '--current-revision', $currentRevision,
        '--report', ('"' + $reportArgument + '"')
    ) -join ' '
    if ($KeepData) { $runner += ' --keep-data' }
    Invoke-CascadeSbt -Directory $repository -Commands @($runner)
}
finally {
    if ($worktreeAdded) {
        & git -C $repository worktree remove --force $oldCheckout 2>$null
    }
    $resolvedTemporary = [IO.Path]::GetFullPath($temporaryRoot)
    $systemTemporary = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    $safeName = [IO.Path]::GetFileName($resolvedTemporary).StartsWith('cascade-rolling-', [StringComparison]::Ordinal)
    if ((Test-Path -LiteralPath $resolvedTemporary) -and $resolvedTemporary.StartsWith($systemTemporary, [StringComparison]::OrdinalIgnoreCase) -and $safeName) {
        Remove-Item -LiteralPath $resolvedTemporary -Recurse -Force
    }
}
