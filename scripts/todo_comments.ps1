# Find TODO comments in Python files, excluding paths containing "__archive"
Get-ChildItem -Recurse -Filter *.py | Where-Object { 
    -not ($_.FullName -like "*__archive*") 
} | ForEach-Object {
    $lines = Get-Content $_.FullName
    $lines | ForEach-Object {
        if ($_ -match 'TODO') {
            [PSCustomObject]@{
                FileName = $_.FullName -replace ".*custom_database", "custom_database"
                Line     = $_
            }
        }
    }
} | Format-Table -AutoSize | Out-File -FilePath "scripts/todo_comments.txt" -Encoding utf8