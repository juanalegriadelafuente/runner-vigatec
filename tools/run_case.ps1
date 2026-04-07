param(
  [string]$BaseUrl = "http://localhost:8000",
  [string]$CasePath = ".\case.xlsx",
  [int]$PollSeconds = 2,
  [string]$DownloadDir = ".\downloads"
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $CasePath)) {
  throw "No encuentro el case en: $CasePath"
}

New-Item -ItemType Directory -Force -Path $DownloadDir | Out-Null

Write-Host "POST $BaseUrl/runs (subiendo $CasePath)..."

# OJO: en PowerShell 5.1 usa curl.exe (no el alias de Invoke-WebRequest)
$json = & curl.exe -s -X POST "$BaseUrl/runs" -F "file=@$CasePath"
if ([string]::IsNullOrWhiteSpace($json)) { throw "Respuesta vacía del API" }

$run = $json | ConvertFrom-Json
$runId = $run.id
Write-Host "Run creado: $runId"

# Poll status
while ($true) {
  Start-Sleep -Seconds $PollSeconds
  $st = Invoke-RestMethod "$BaseUrl/runs/$runId"

  $status = $st.status
  $qa = $st.qa_status
  $msg = $st.qa_message

  Write-Host ("[{0}] status={1} qa={2} msg={3}" -f (Get-Date).ToString("HH:mm:ss"), $status, $qa, $msg)

  if ($status -eq "success") { break }
  if ($status -eq "failed") {
    throw ("Run failed: " + $st.error_message)
  }
}

Write-Host "OK. Listando artefactos..."
$arts = Invoke-RestMethod "$BaseUrl/runs/$runId/artifacts"

# Descarga artifacts típicos si existen
$want = @("plan_visual.xlsx","plan_mensual.xlsx","certificado_plan.txt","logs/solver.log","qa_plan.json")
foreach ($name in $want) {
  $exists = $false
  foreach ($a in $arts.artifacts) {
    if ($a.name -eq $name) { $exists = $true; break }
  }
  if ($exists) {
    $outFile = Join-Path $DownloadDir ($name -replace "[/\\]", "_")
    Write-Host "Descargando $name -> $outFile"
    & curl.exe -s -L -o "$outFile" "$BaseUrl/runs/$runId/artifacts/$name" | Out-Null
  }
}

Write-Host "Listo. RunId=$runId"
Write-Host "Tip: abre $BaseUrl/docs para ver/replicar llamadas."