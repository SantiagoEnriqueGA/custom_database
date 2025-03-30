" "

# Dynamic separator line based on desired length
$separatorLength = 100
$separator = ("_" * $separatorLength) -join ''

$output = @()

# --- Python File Counting ---

$output += "Lines per Python file, sorted by directory group (segadb, tests, examples, other) then descending count:"
$output += $separator

# Collect file info in an array, filtering out paths containing "__archive"
$files = Get-ChildItem -Recurse -Filter *.py | Where-Object {
    ($_.FullName -notlike "*__archive*") -and     # Condition 1
    ($_.FullName -notlike "*web_app_react*") -and # Condition 2 (Exclude react app python files)
    ($_.FullName -notlike "*web_app*")            # Condition 3 (Exclude older flask web app files)
} | ForEach-Object {
    # Use relative path from the assumed project root for cleaner sorting checks
    $relativePath = $_.FullName -replace [regex]::Escape($PWD.Path + "\"), "" # Get path relative to current directory ($PWD)
    $displayPath = $_.FullName -replace ".*custom_database", "`tcustom_database" # Original display path logic
    $lineCount = (Get-Content $_.FullName | Measure-Object -Line).Lines

    # Determine sort priority based on directory
    $sortPriority = 4 # Default for 'other'
    if ($relativePath -like 'segadb\*') { $sortPriority = 1 }
    elseif ($relativePath -like 'tests\*') { $sortPriority = 2 }
    elseif ($relativePath -like 'examples\*') { $sortPriority = 3 }
    # Add more elseif here for other specific directories if needed

    [PSCustomObject]@{
        DisplayPath  = $displayPath
        RelativePath = $relativePath # Keep relative path for sorting
        LineCount    = $lineCount
        SortPriority = $sortPriority
    }
}

# Sort the array: 1st by SortPriority (ascending), 2nd by LineCount (descending)
$sortedFiles = $files | Sort-Object -Property SortPriority, @{Expression='LineCount'; Descending=$true}

# Display the sorted results
$sortedFiles | ForEach-Object {
    $output += "{0,-80}: {1,5}" -f $_.DisplayPath, $_.LineCount
}

# Calculate the total number of lines for the *counted* Python files
$totalPythonLines = 0
if ($sortedFiles) { # Avoid error if $sortedFiles is empty
    $totalPythonLines = $sortedFiles.LineCount | Measure-Object -Sum | Select-Object -ExpandProperty Sum
}


$output += $separator
$output += "{0,-87}: {1,5}" -f "Total Python lines (excluding web apps)", $totalPythonLines
$output += " "


# --- React Web App File Counting ---

$output += "Lines in web_app_react/ directory (excluding node_modules):" # Updated header
$output += $separator

# Find all files (not directories) within web_app_react recursively, excluding node_modules
$reactFiles = Get-ChildItem -Path 'web_app_react' -Recurse -File | Where-Object {
    $_.FullName -notlike "*\node_modules\*" # Exclude files within any node_modules directory
}

# Calculate the total line count for all files found
$totalReactLines = 0
if ($reactFiles) { # Check if any files were found after filtering
    $totalReactLines = ($reactFiles | Get-Content -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
}

$output += "{0,-87}: {1,5}" -f "Total lines (web_app_react excl. node_modules)", $totalReactLines
$output += " "

# --- Final Output ---

# Print the combined output to console
$output | ForEach-Object { Write-Output $_ }

# Save the combined output to count_lines.txt
# Ensure the output directory exists
$outputDir = "scripts/out"
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}
$output | Out-File -FilePath "$outputDir/count_lines.txt" -Encoding utf8

" "
Write-Host "Line count results saved to $outputDir/count_lines.txt"