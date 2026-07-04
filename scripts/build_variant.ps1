param([string]$Name)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$staging = Join-Path $root "build\staging_$($Name.Replace('.','_'))"
$exe = Join-Path $root "build\execname_$($Name.Replace('.','_')).exe"
$src = Join-Path $root "src"
$vsrc = Join-Path $root "variants\$Name\src"
if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Force -Path $staging | Out-Null
$shared = @("parser.cpp","machine_state.cpp","output.cpp","models.h","parser.h","output.h","machine_state.h")
foreach ($f in $shared) { Copy-Item (Join-Path $src $f) $staging }
$main = Join-Path $vsrc "main.cpp"
if (Test-Path $main) { Copy-Item $main $staging } else { Copy-Item (Join-Path $src "main.cpp") $staging }
Copy-Item (Join-Path $vsrc "scheduler.cpp") $staging
Copy-Item (Join-Path $vsrc "scheduler.h") $staging
$files = @("main.cpp","parser.cpp","machine_state.cpp","scheduler.cpp","output.cpp") | ForEach-Object { Join-Path $staging $_ }
& g++ -std=c++17 -O2 @files -o $exe
Write-Output "Built $exe"
