param(
    [string]$DbPath = ".local/config/classifier.db"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$resolvedDbPath = if ([System.IO.Path]::IsPathRooted($DbPath)) {
    $DbPath
} else {
    Join-Path $repoRoot $DbPath
}

if (-not (Test-Path -LiteralPath $resolvedDbPath)) {
    throw "Database file not found: $resolvedDbPath"
}

$tablesRaw = sqlite3 $resolvedDbPath "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
$tables = @($tablesRaw | Where-Object { $_ -and $_.Trim().Length -gt 0 })

if ($tables.Count -eq 0) {
    Write-Host "No application tables to clean."
    exit 0
}

$sqlLines = @(
    "PRAGMA busy_timeout = 5000;",
    "PRAGMA foreign_keys = OFF;",
    "BEGIN IMMEDIATE;"
)

foreach ($table in $tables) {
    $escaped = $table.Replace('"', '""')
    $sqlLines += "DELETE FROM ""$escaped"";"
}

$sqlLines += @(
    "COMMIT;",
    "PRAGMA foreign_keys = ON;"
)

$sql = ($sqlLines -join [Environment]::NewLine)
$sql | sqlite3 $resolvedDbPath | Out-Null

Write-Host "Data cleared (schema preserved): $resolvedDbPath"
Write-Host ""
Write-Host "Current row counts:"

foreach ($table in $tables) {
    $escaped = $table.Replace('"', '""')
    $count = sqlite3 $resolvedDbPath "SELECT COUNT(*) FROM ""$escaped"";"
    Write-Host ("- {0}: {1}" -f $table, $count)
}
