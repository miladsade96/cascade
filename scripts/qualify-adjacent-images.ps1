[CmdletBinding()]
param(
    [string]$BaselineImage = 'miladsade96/cascade:1.14.0',
    [string]$CandidateImage,
    [string]$BaselineVersion = '1.14.0',
    [string]$CandidateVersion,
    [string]$Report,
    [ValidateRange(1, 10000)][int]$RecordsPerPhase = 25,
    [ValidateRange(1, 100000)][int]$TransitionRecords = 1000,
    [switch]$KeepResources
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ([string]::IsNullOrWhiteSpace($CandidateVersion)) {
    $CandidateVersion = (Get-Content -LiteralPath (Join-Path $repository 'VERSION') -Raw).Trim()
}
if ([string]::IsNullOrWhiteSpace($CandidateImage)) {
    $CandidateImage = "miladsade96/cascade:$CandidateVersion"
}
if ([string]::IsNullOrWhiteSpace($Report)) {
    $Report = Join-Path $repository "artifacts/adjacent-images-$BaselineVersion-to-$CandidateVersion.json"
}
$Report = [IO.Path]::GetFullPath($Report)
if (Test-Path -LiteralPath $Report) { throw "Rolling evidence path already exists: $Report" }
if ($TransitionRecords -lt $RecordsPerPhase) { throw 'TransitionRecords must be at least RecordsPerPhase.' }

$changes = @(& git -C $repository status --porcelain)
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect the Git working tree.' }
if ($changes.Count -ne 0) { throw 'Adjacent image qualification requires a clean Git working tree.' }
$revision = (& git -C $repository rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') { throw 'Could not resolve the exact release revision.' }

$candidateRevision = (& docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' $CandidateImage).Trim()
if ($LASTEXITCODE -ne 0) { throw "Candidate image is unavailable: $CandidateImage" }
if ($candidateRevision -ne $revision) {
    throw "Candidate image revision $candidateRevision does not match checked-out revision $revision."
}

Push-Location $repository
try {
    & .\sbt.bat Test/compile
    if ($LASTEXITCODE -ne 0) { throw "Test compilation failed with exit code $LASTEXITCODE" }
    $reportArgument = $Report.Replace('\', '/')
    $runner = @(
        'Test / runMain cascade.qualification.AdjacentImageRollingQualification',
        '--baseline-image', $BaselineImage,
        '--candidate-image', $CandidateImage,
        '--baseline-version', $BaselineVersion,
        '--candidate-version', $CandidateVersion,
        '--records-per-phase', $RecordsPerPhase,
        '--transition-records', $TransitionRecords,
        '--report', ('"' + $reportArgument + '"')
    ) -join ' '
    if ($KeepResources) { $runner += ' --keep-resources' }
    & .\sbt.bat $runner
    if ($LASTEXITCODE -ne 0) { throw "Adjacent image qualification failed with exit code $LASTEXITCODE" }
    if (-not (Test-Path -LiteralPath $Report)) { throw 'Adjacent image qualification did not publish its report.' }
    $evidence = Get-Content -LiteralPath $Report -Raw | ConvertFrom-Json
    if ($evidence.status -ne 'passed' -or -not $evidence.exact_recovery -or $evidence.idempotent_retries -lt 1) {
        throw 'Adjacent image evidence did not satisfy the release gate.'
    }
    Write-Output "ADJACENT_IMAGE_GATE passed baseline=$BaselineImage candidate=$CandidateImage records=$($evidence.records) retries=$($evidence.idempotent_retries) report=$Report"
}
finally {
    Pop-Location
}
