# Script for outputting the file tree and contents of the web_app directory

$webAppPath = ".\web_app"
$outputFile = ".\scripts\out\web_app.txt"

# Generate file tree
function Get-FileTree {
    param ([string]$Path, [string]$Indent = "")
    $items = Get-ChildItem -Path $Path
    foreach ($item in $items) {
        if ($item.PSIsContainer) {
            "$Indent|- $($item.Name)/" | Out-File -Append -FilePath $outputFile
            Get-FileTree -Path $item.FullName -Indent "$Indent|  "
        } else {
            "$Indent|- $($item.Name)" | Out-File -Append -FilePath $outputFile
        }
    }
}

# Write file tree
"web_app/" | Out-File -FilePath $outputFile
Get-FileTree -Path $webAppPath -Indent "|- "

# Write file contents
function Write-FileContents {
    param ([string]$Path)
    $files = Get-ChildItem -Path $Path -Recurse -File
    foreach ($file in $files) {
        "`n-----------------------------------------------------------------" | Out-File -Append -FilePath $outputFile
        "$($file.FullName -replace ".*custom_database", "custom_database"):" | Out-File -Append -FilePath $outputFile
        "-----------------------------------------------------------------" | Out-File -Append -FilePath $outputFile
        Get-Content -Path $file.FullName | Out-File -Append -FilePath $outputFile
    }
}

# Write contents of all files
Write-FileContents -Path $webAppPath