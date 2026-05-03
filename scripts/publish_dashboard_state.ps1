param(
    [switch]$DryRun
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$paths = @(
    "state\haver_status.json",
    "state\haver_events.jsonl",
    "state\README.md",
    "docs\haver-status.schema.json",
    "docs\dashboard-data-contract.md"
)

$commitMessage = $env:HAVER_GITHUB_COMMIT_MESSAGE
if ([string]::IsNullOrWhiteSpace($commitMessage)) {
    $commitMessage = "Update dashboard state"
}

if ($DryRun) {
    Write-Host "Dry run: would publish dashboard state from $repoRoot"
    $paths | ForEach-Object { Write-Host " - $_" }
    exit 0
}

git -C $repoRoot add -- $paths
if ($LASTEXITCODE -ne 0) {
    Write-Error "git add failed."
    exit $LASTEXITCODE
}

git -C $repoRoot diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "No dashboard state changes to publish."
    exit 0
}

git -C $repoRoot commit -m $commitMessage
if ($LASTEXITCODE -ne 0) {
    Write-Error "git commit failed."
    exit $LASTEXITCODE
}

$remote = $env:HAVER_GITHUB_PUSH_REMOTE
if ([string]::IsNullOrWhiteSpace($remote)) {
    $remote = "origin"
}

$branch = $env:HAVER_GITHUB_PUSH_BRANCH
if ([string]::IsNullOrWhiteSpace($branch)) {
    $branch = (git -C $repoRoot rev-parse --abbrev-ref HEAD).Trim()
}

git -C $repoRoot push $remote $branch
if ($LASTEXITCODE -ne 0) {
    Write-Error "git push failed."
    exit $LASTEXITCODE
}

Write-Host "Published dashboard state to $remote/$branch."
