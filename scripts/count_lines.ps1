" "

# Dynamic separator line based on desired length
$separatorLength = 100
$separator = ("_" * $separatorLength) -join ''

$output = @()

$output += "Lines per Python file, sorted by descending order:"
$output += $separator

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
    $output += "{0,-80}: {1,5}" -f $_.FilePath, $_.LineCount
}

# Calculate the total number of lines, excluding paths with "__archive"
$totalLines = Get-ChildItem -Recurse -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | Get-Content | Measure-Object -Line | Select-Object -ExpandProperty Lines

$output += $separator
$output += "{0,-87}: {1,5}" -f "Total lines", $totalLines
$output += " "

# Print the output
$output | ForEach-Object { Write-Output $_ }

# Save the output to count_lines.txt
$output | Out-File -FilePath "scripts/count_lines.txt" -Encoding utf8