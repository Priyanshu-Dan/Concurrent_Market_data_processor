$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

g++ -std=c++17 -O3 -D_WIN32 -I include producer.cpp -o producer.exe -lws2_32
if ($LASTEXITCODE -ne 0) {
    Write-Error 'Producer compilation failed.'
    exit $LASTEXITCODE
}

g++ -std=c++17 -O3 -D_WIN32 -I include engine.cpp -o engine.exe -lws2_32
if ($LASTEXITCODE -ne 0) {
    Write-Error 'Engine compilation failed.'
    exit $LASTEXITCODE
}

if ((Test-Path -LiteralPath '.\producer.exe') -and (Test-Path -LiteralPath '.\engine.exe')) {
    Write-Host 'Build succeeded.' -ForegroundColor Green
    Write-Host 'Open two PowerShell terminals in this directory:'
    Write-Host '  Terminal 1: .\engine.exe'
    Write-Host '  Terminal 2: .\producer.exe'
} else {
    Write-Error 'Build completed but one or both executables are missing.'
    exit 1
}
