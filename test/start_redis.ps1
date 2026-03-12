# Запуск Redis для тестов (через Docker)
# Если Docker не установлен - установите Redis вручную или используйте Docker Desktop

$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

if (Get-Command docker -ErrorAction SilentlyContinue) {
    Write-Host "Запуск Redis через Docker..."
    docker compose -f test/docker-compose.yml up -d
    Start-Sleep -Seconds 2
    Write-Host "Redis запущен на localhost:6379"
} else {
    Write-Host "Docker не найден. Запустите Redis вручную:" -ForegroundColor Yellow
    Write-Host "  - Установите Docker Desktop и выполните: docker compose -f test/docker-compose.yml up -d"
    Write-Host "  - Или установите Redis и запустите redis-server"
}
