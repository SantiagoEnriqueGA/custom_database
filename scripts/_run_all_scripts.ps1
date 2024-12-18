# Get all .ps1 files in the scripts/ folder except _run_all_scripts.ps1
$scripts = Get-ChildItem -Path "scripts/" -Filter "*.ps1" | Where-Object { $_.Name -ne '_run_all_scripts.ps1' }

# Iterate over each script and run it
foreach ($script in $scripts) {
    Write-Host "Running script: $($script.FullName)"
    & $script.FullName
}