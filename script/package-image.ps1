param(
    [string]$ImageName = $(if ($env:CLASSIFIER_IMAGE) { $env:CLASSIFIER_IMAGE } else { "classifier" }),
    [string]$ImageTag = $(if ($env:CLASSIFIER_TAG) { $env:CLASSIFIER_TAG } else { "local" }),
    [string]$Platform = $(if ($env:DOCKER_PLATFORM) { $env:DOCKER_PLATFORM } else { "linux/amd64" }),
    [string]$OutputDir = $(if ($env:OUTPUT_DIR) { $env:OUTPUT_DIR } else { "output" })
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$safeImageName = $ImageName -replace "[\\/:]", "-"
$safePlatform = $Platform -replace "[/:]", "-"
$tarName = "$safeImageName-$ImageTag-$safePlatform.tar"
$tarPath = Join-Path $repoRoot (Join-Path $OutputDir $tarName)

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$Arguments = @()
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
    }
}

Push-Location $repoRoot
try {
    New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

    Invoke-CheckedCommand -FilePath docker -Arguments @("buildx", "build", "--platform", $Platform, "--tag", "${ImageName}:${ImageTag}", "--load", ".")
    Invoke-CheckedCommand -FilePath docker -Arguments @("save", "${ImageName}:${ImageTag}", "-o", $tarPath)

    Write-Host "Image packaged: $tarPath"
    Write-Host "On ZSpace NAS, run: docker load -i $tarName"
}
finally {
    Pop-Location
}
