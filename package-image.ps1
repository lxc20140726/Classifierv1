param(
    [string]$ImageName = $(if ($env:CLASSIFIER_IMAGE) { $env:CLASSIFIER_IMAGE } else { "classifier" }),
    [string]$ImageTag = $(if ($env:CLASSIFIER_TAG) { $env:CLASSIFIER_TAG } else { "local" }),
    [string]$Platform = $(if ($env:DOCKER_PLATFORM) { $env:DOCKER_PLATFORM } else { "linux/amd64" }),
    [string]$OutputDir = $(if ($env:OUTPUT_DIR) { $env:OUTPUT_DIR } else { "output" })
)

$ErrorActionPreference = "Stop"

& "$PSScriptRoot\script\package-image.ps1" `
    -ImageName $ImageName `
    -ImageTag $ImageTag `
    -Platform $Platform `
    -OutputDir $OutputDir
