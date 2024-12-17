" "

# Dynamic separator line based on desired length
$separatorLength = 100
$separator = ("_" * $separatorLength) -join ''

"Lines per Python file, sorted by descending order:"
Write-Output $separator

# Collect file info in an array, filtering out paths containing "__archive"
$files = Get-ChildItem -Recurse -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | ForEach-Object {
    $filePath = $_.FullName -replace ".*custom_database", "`tcustom_database"
    $lineCount = (Get-Content $_.FullName | Measure-Object -Line).Lines
    [PSCustomObject]@{
        FilePath  = $filePath
        LineCount = $lineCount
    }
}

# Sort the array by LineCount in descending order
$sortedFiles = $files | Sort-Object -Property LineCount -Descending

# Display the sorted results
$sortedFiles | ForEach-Object {
    "{0,-80}: {1,5}" -f $_.FilePath, $_.LineCount
}

# Calculate the total number of lines, excluding paths with "__archive"
$totalLines = Get-ChildItem -Recurse -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | Get-Content | Measure-Object -Line | Select-Object -ExpandProperty Lines

Write-Output $separator
"{0,-87}: {1,5}" -f "Total lines", $totalLines
" "
