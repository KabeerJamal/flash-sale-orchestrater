# chaos.ps1

$containers = docker ps --format "{{.Names}}"

while ($true) {
    Start-Sleep -Seconds (Get-Random -Minimum 10 -Maximum 30)

    $target = Get-Random -InputObject $containers
    $time = Get-Date -Format "HH:mm:ss"

    Write-Host "$time Chaos: Killing $target"

    $stopResult = docker stop $target 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "$time ERROR: $stopResult"
        continue
    }

    Start-Sleep -Seconds (Get-Random -Minimum 5 -Maximum 15)

    $time = Get-Date -Format "HH:mm:ss"
    Write-Host "$time Chaos: Restarting $target"

    $startResult = docker start $target 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "$time ERROR: $startResult"
    }
}