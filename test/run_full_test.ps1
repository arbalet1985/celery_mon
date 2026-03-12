# Полный тест celery_monitor
# Требования: Redis на localhost:6379 (запустите: docker compose -f test/docker-compose.yml up -d)
# Запуск: .\test\run_full_test.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

# Проверка Redis
try {
    $null = python -c "import redis; r=redis.Redis(host='localhost', port=6379); r.ping()" 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Redis check failed" }
    Write-Host "Redis OK" -ForegroundColor Green
} catch {
    Write-Host "Redis не запущен. Запустите:" -ForegroundColor Red
    Write-Host "  docker compose -f test/docker-compose.yml up -d" -ForegroundColor Yellow
    Write-Host "  или redis-server" -ForegroundColor Yellow
    exit 1
}

Write-Host "`n=== 1. Discover tasks (нужен воркер) ===" -ForegroundColor Cyan
python celery_monitor.py -A test.celery_app --discover tasks -c test/config.yaml 2>&1

Write-Host "`n=== 2. Discover queues ===" -ForegroundColor Cyan
python celery_monitor.py -A test.celery_app --discover queues -c test/config.yaml 2>&1

Write-Host "`n=== 3. Discover workers (нужен воркер) ===" -ForegroundColor Cyan
python celery_monitor.py -A test.celery_app --discover workers -c test/config.yaml 2>&1

Write-Host "`n=== 4. One-shot dry-run ===" -ForegroundColor Cyan
python celery_monitor.py -A test.celery_app --once --dry-run -c test/config.yaml 2>&1

Write-Host "`nДля полного теста с событиями:" -ForegroundColor Yellow
Write-Host "  1. В одном терминале: celery -A test.celery_app worker -l info"
Write-Host "  2. В другом: python celery_monitor.py -A test.celery_app -c test/config.yaml --daemon --dry-run -v"
Write-Host "  3. В третьем: python test/run_test.py"
Write-Host "  (задачи add и fail_task выполнятся, метрики появятся в терминале 2)"
